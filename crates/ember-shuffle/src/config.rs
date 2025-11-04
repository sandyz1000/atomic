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
        }
    }
}
