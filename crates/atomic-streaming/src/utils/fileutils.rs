use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use atomic_data::cache::StorageLevel;

/// A single segment written to a write-ahead log file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSegment {
    pub path: PathBuf,
    pub offset: u64,
    pub length: u64,
}

/// Trait for write-ahead log implementations.
pub trait WriteAheadLog: Send + Sync {
    /// Append bytes to the log. Returns a segment descriptor for later reads.
    fn write(&self, data: &[u8], time_ms: u64) -> io::Result<WalSegment>;
    /// Read a previously written segment.
    fn read(&self, segment: &WalSegment) -> io::Result<Vec<u8>>;
    /// Delete all segments older than `threshold_ms`.
    fn clean(&self, threshold_ms: u64) -> io::Result<()>;
}

struct WalInner {
    dir: PathBuf,
    current_file: Option<BufWriter<File>>,
    current_path: Option<PathBuf>,
    current_offset: u64,
    roll_interval_ms: u64,
    file_start_ms: u64,
}

/// A write-ahead log backed by rolling files in a local directory.
pub struct FileBasedWriteAheadLog {
    inner: Arc<Mutex<WalInner>>,
}

impl FileBasedWriteAheadLog {
    pub fn new(dir: impl Into<PathBuf>, roll_interval_ms: u64) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(FileBasedWriteAheadLog {
            inner: Arc::new(Mutex::new(WalInner {
                dir,
                current_file: None,
                current_path: None,
                current_offset: 0,
                roll_interval_ms,
                file_start_ms: 0,
            })),
        })
    }

    fn ensure_file(inner: &mut WalInner, time_ms: u64) -> io::Result<()> {
        let needs_roll = inner.current_file.is_none()
            || time_ms >= inner.file_start_ms + inner.roll_interval_ms;
        if needs_roll {
            if let Some(ref mut f) = inner.current_file {
                f.flush()?;
            }
            let filename = format!("wal-{}.log", time_ms);
            let path = inner.dir.join(&filename);
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            inner.current_file = Some(BufWriter::new(file));
            inner.current_path = Some(path);
            inner.current_offset = 0;
            inner.file_start_ms = time_ms;
        }
        Ok(())
    }
}

impl WriteAheadLog for FileBasedWriteAheadLog {
    fn write(&self, data: &[u8], time_ms: u64) -> io::Result<WalSegment> {
        let mut inner = self.inner.lock();
        Self::ensure_file(&mut inner, time_ms)?;
        let offset = inner.current_offset;
        let writer = inner.current_file.as_mut().unwrap();
        let len = data.len() as u64;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(data)?;
        writer.flush()?;
        inner.current_offset += 8 + len;
        Ok(WalSegment {
            path: inner.current_path.clone().unwrap(),
            offset,
            length: len,
        })
    }

    fn read(&self, segment: &WalSegment) -> io::Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = File::open(&segment.path)?;
        file.seek(SeekFrom::Start(segment.offset + 8))?;
        let mut buf = vec![0u8; segment.length as usize];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn clean(&self, threshold_ms: u64) -> io::Result<()> {
        let inner = self.inner.lock();
        for entry in fs::read_dir(&inner.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(stripped) = name.strip_prefix("wal-").and_then(|s| s.strip_suffix(".log")) {
                if let Ok(ts) = stripped.parse::<u64>() {
                    if ts < threshold_ms {
                        let _ = fs::remove_file(entry.path());
                    }
                }
            }
        }
        Ok(())
    }
}

/// List WAL files in a directory sorted by timestamp.
pub fn list_wal_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files: Vec<(u64, PathBuf)> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy().into_owned();
            let ts = name
                .strip_prefix("wal-")
                .and_then(|s| s.strip_suffix(".log"))
                .and_then(|s| s.parse::<u64>().ok())?;
            Some((ts, e.path()))
        })
        .collect();
    files.sort_by_key(|(ts, _)| *ts);
    Ok(files.into_iter().map(|(_, p)| p).collect())
}
