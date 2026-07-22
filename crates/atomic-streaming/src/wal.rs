//! Write-Ahead Log for durable, replayable records.
//!
//! Receiver-based sources can lose in-flight data if the driver crashes between receiving and
//! processing. A WAL records each unit of work to reliable storage *before* it is acted on, so
//! recovery can replay everything not yet known to be committed.
//!
//! [`WriteAheadLog`] is the storage-agnostic contract; [`FileWriteAheadLog`] is an append-only
//! file implementation that rotates to a new segment once the active one exceeds a size limit
//! and replays every segment in write order. Records are opaque byte slices — callers serialize
//! their own payloads (e.g. dispatched partition descriptors).

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::errors::{StreamingError, StreamingResult};

/// A durable, append-only log of opaque byte records.
pub trait WriteAheadLog: Send + Sync {
    /// Append `record` durably, returning nothing until the bytes are flushed.
    fn write(&self, record: &[u8]) -> StreamingResult<()>;

    /// Replay every record still in the log, in write order.
    fn read_all(&self) -> StreamingResult<Vec<Vec<u8>>>;

    /// Discard all records — called once a batch is known committed and its records are no
    /// longer needed for recovery.
    fn clear(&self) -> StreamingResult<()>;
}

/// Default maximum size of one WAL segment before rotation (16 MiB).
pub const DEFAULT_SEGMENT_BYTES: u64 = 16 * 1024 * 1024;

/// File-backed [`WriteAheadLog`]. Records are written to `dir/wal-<n>.log` segments as
/// `u32` length prefix + payload; a new segment starts once the active one exceeds
/// `segment_bytes`.
pub struct FileWriteAheadLog {
    dir: PathBuf,
    segment_bytes: u64,
    inner: Mutex<ActiveSegment>,
}

struct ActiveSegment {
    index: u64,
    written: u64,
    writer: BufWriter<File>,
}

impl FileWriteAheadLog {
    /// Open (creating if needed) a WAL under `dir`, continuing after any existing segments.
    pub fn open(dir: impl Into<PathBuf>) -> StreamingResult<Self> {
        Self::open_with_segment_size(dir, DEFAULT_SEGMENT_BYTES)
    }

    /// As [`open`](Self::open) but with an explicit segment-rotation size (bytes).
    pub fn open_with_segment_size(
        dir: impl Into<PathBuf>,
        segment_bytes: u64,
    ) -> StreamingResult<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        // Continue after the highest existing segment so a reopened WAL keeps its history.
        let next = Self::segment_indices(&dir)?.last().map_or(0, |i| i + 1);
        let writer = Self::open_segment(&dir, next)?;
        Ok(FileWriteAheadLog {
            dir,
            segment_bytes: segment_bytes.max(1),
            inner: Mutex::new(ActiveSegment {
                index: next,
                written: 0,
                writer,
            }),
        })
    }

    fn segment_path(dir: &Path, index: u64) -> PathBuf {
        dir.join(format!("wal-{index}.log"))
    }

    fn open_segment(dir: &Path, index: u64) -> StreamingResult<BufWriter<File>> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(Self::segment_path(dir, index))?;
        Ok(BufWriter::new(file))
    }

    /// Existing segment indices in ascending order.
    fn segment_indices(dir: &Path) -> StreamingResult<Vec<u64>> {
        let mut indices = Vec::new();
        for entry in fs::read_dir(dir)? {
            let name = entry?.file_name();
            let name = name.to_string_lossy();
            if let Some(n) = name
                .strip_prefix("wal-")
                .and_then(|s| s.strip_suffix(".log"))
                .and_then(|s| s.parse::<u64>().ok())
            {
                indices.push(n);
            }
        }
        indices.sort_unstable();
        Ok(indices)
    }

    /// Read every length-prefixed record from one segment file.
    fn read_segment(path: &Path) -> StreamingResult<Vec<Vec<u8>>> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                // A clean EOF at a record boundary ends the segment.
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            // A short read here means a torn tail record (crash mid-write) — stop replay.
            if reader.read_exact(&mut payload).is_err() {
                break;
            }
            records.push(payload);
        }
        Ok(records)
    }
}

impl WriteAheadLog for FileWriteAheadLog {
    fn write(&self, record: &[u8]) -> StreamingResult<()> {
        let len: u32 = record
            .len()
            .try_into()
            .map_err(|_| StreamingError::Internal("WAL record exceeds u32 length".into()))?;
        let mut seg = self.inner.lock();
        seg.writer.write_all(&len.to_le_bytes())?;
        seg.writer.write_all(record)?;
        seg.writer.flush()?;
        seg.written += 4 + record.len() as u64;
        if seg.written >= self.segment_bytes {
            let next = seg.index + 1;
            let writer = Self::open_segment(&self.dir, next)?;
            *seg = ActiveSegment {
                index: next,
                written: 0,
                writer,
            };
        }
        Ok(())
    }

    fn read_all(&self) -> StreamingResult<Vec<Vec<u8>>> {
        // Hold the lock so a concurrent rotation can't race the read.
        let _seg = self.inner.lock();
        let mut all = Vec::new();
        for index in Self::segment_indices(&self.dir)? {
            all.extend(Self::read_segment(&Self::segment_path(&self.dir, index))?);
        }
        Ok(all)
    }

    fn clear(&self) -> StreamingResult<()> {
        let mut seg = self.inner.lock();
        for index in Self::segment_indices(&self.dir)? {
            fs::remove_file(Self::segment_path(&self.dir, index))?;
        }
        // Start a fresh active segment at 0.
        *seg = ActiveSegment {
            index: 0,
            written: 0,
            writer: Self::open_segment(&self.dir, 0)?,
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_replay() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWriteAheadLog::open(dir.path()).unwrap();
        wal.write(b"alpha").unwrap();
        wal.write(b"beta").unwrap();
        let got = wal.read_all().unwrap();
        assert_eq!(got, vec![b"alpha".to_vec(), b"beta".to_vec()]);
    }

    #[test]
    fn test_reopen_persists() {
        let dir = tempfile::tempdir().unwrap();
        {
            let wal = FileWriteAheadLog::open(dir.path()).unwrap();
            wal.write(b"one").unwrap();
        }
        // A fresh handle over the same dir replays prior records.
        let wal = FileWriteAheadLog::open(dir.path()).unwrap();
        wal.write(b"two").unwrap();
        assert_eq!(
            wal.read_all().unwrap(),
            vec![b"one".to_vec(), b"two".to_vec()]
        );
    }

    #[test]
    fn test_segment_rotation() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny segments force a rotation per record.
        let wal = FileWriteAheadLog::open_with_segment_size(dir.path(), 1).unwrap();
        for i in 0..5u8 {
            wal.write(&[i]).unwrap();
        }
        assert!(
            FileWriteAheadLog::segment_indices(dir.path())
                .unwrap()
                .len()
                > 1
        );
        let got = wal.read_all().unwrap();
        assert_eq!(got, (0..5u8).map(|i| vec![i]).collect::<Vec<_>>());
    }

    #[test]
    fn test_clear() {
        let dir = tempfile::tempdir().unwrap();
        let wal = FileWriteAheadLog::open(dir.path()).unwrap();
        wal.write(b"x").unwrap();
        wal.clear().unwrap();
        assert!(wal.read_all().unwrap().is_empty());
        // Still writable after clearing.
        wal.write(b"y").unwrap();
        assert_eq!(wal.read_all().unwrap(), vec![b"y".to_vec()]);
    }
}
