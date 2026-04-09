use bincode::{Decode, Encode};
use std::fs;
use std::io;
use std::path::Path;

/// Serializable checkpoint state for a streaming application.
#[derive(Debug, Clone, Encode, Decode)]
pub struct Checkpoint {
    /// The batch time at which this checkpoint was taken (ms since UNIX epoch).
    pub checkpoint_time_ms: u64,
    /// Batch duration in milliseconds.
    pub batch_duration_ms: u64,
    /// Path to the checkpoint directory.
    pub checkpoint_dir: String,
    /// Batch times that were in progress when the checkpoint was written.
    pub pending_batch_times: Vec<u64>,
}

impl Checkpoint {
    pub fn new(
        checkpoint_time_ms: u64,
        batch_duration_ms: u64,
        checkpoint_dir: impl Into<String>,
    ) -> Self {
        Checkpoint {
            checkpoint_time_ms,
            batch_duration_ms,
            checkpoint_dir: checkpoint_dir.into(),
            pending_batch_times: Vec::new(),
        }
    }

    /// Write this checkpoint atomically to `dir`.
    ///
    /// Writes to a `.tmp` file first, then renames to the final name to ensure
    /// atomicity on most operating systems.
    pub fn write(&self, dir: &Path) -> io::Result<()> {
        fs::create_dir_all(dir)?;
        let filename = format!("checkpoint-{}", self.checkpoint_time_ms);
        let final_path = dir.join(&filename);
        let tmp_path = dir.join(format!("{}.tmp", filename));

        let bytes = bincode::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        fs::write(&tmp_path, &bytes)?;
        fs::rename(&tmp_path, &final_path)?;
        log::debug!("Wrote checkpoint to {:?}", final_path);
        Ok(())
    }

    /// Read the most recent checkpoint from `dir`, or `None` if no checkpoint exists.
    pub fn read_latest(dir: &Path) -> io::Result<Option<Self>> {
        if !dir.exists() {
            return Ok(None);
        }
        let mut entries: Vec<_> = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                name.starts_with("checkpoint-") && !name.ends_with(".tmp")
            })
            .collect();
        entries.sort_by_key(|e| e.file_name());
        let latest = match entries.last() {
            Some(e) => e.path(),
            None => return Ok(None),
        };
        let bytes = fs::read(&latest)?;
        let (cp, _) = bincode::decode_from_slice::<Self, _>(&bytes, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        log::info!("Loaded checkpoint from {:?} (time={}ms)", latest, cp.checkpoint_time_ms);
        Ok(Some(cp))
    }

    /// Delete checkpoint files older than `threshold_ms`.
    pub fn clean(dir: &Path, threshold_ms: u64) -> io::Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(ts_str) = name.strip_prefix("checkpoint-") {
                if let Ok(ts) = ts_str.parse::<u64>() {
                    if ts < threshold_ms {
                        let _ = fs::remove_file(entry.path());
                    }
                }
            }
        }
        Ok(())
    }
}
