use std::net::Ipv4Addr;
use std::path::PathBuf;

/// Configuration for shuffle operations
#[derive(Clone, Debug)]
pub struct ShuffleConfig {
    /// Local IP address for shuffle server
    pub local_ip: Ipv4Addr,
    /// Local directory for shuffle data storage
    pub local_dir: PathBuf,
    /// Optional port for shuffle server (None = auto-assign)
    pub shuffle_port: Option<u16>,
    /// Whether to clean up shuffle data on completion
    pub log_cleanup: bool,
    /// When `Some(bytes)`, switch to `SpillableShuffleCache` that spills shuffle buckets to disk
    /// once in-memory bytes would exceed this threshold.  `None` (default) keeps all shuffle
    /// data in memory (`DashMapShuffleCache`).
    pub spill_threshold: Option<usize>,
    /// Directory for spill files when `spill_threshold` is set.
    /// Defaults to `local_dir/shuffle-spill` when `None`.
    pub spill_dir: Option<PathBuf>,
}

impl ShuffleConfig {
    pub fn new(
        local_ip: Ipv4Addr,
        local_dir: PathBuf,
        shuffle_port: Option<u16>,
        log_cleanup: bool,
    ) -> Self {
        ShuffleConfig {
            local_ip,
            local_dir,
            shuffle_port,
            log_cleanup,
            spill_threshold: None,
            spill_dir: None,
        }
    }

    /// Return the effective spill directory: `spill_dir` if set, else `local_dir/shuffle-spill`.
    pub fn effective_spill_dir(&self) -> PathBuf {
        self.spill_dir
            .clone()
            .unwrap_or_else(|| self.local_dir.join("shuffle-spill"))
    }
}
