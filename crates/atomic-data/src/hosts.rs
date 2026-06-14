use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(thiserror::Error, Debug)]
pub enum HostError {
    #[error("unable to determine home directory")]
    NoHome,
    #[error("failed to load hosts file {path}: {source}")]
    LoadHosts {
        source: std::io::Error,
        path: PathBuf,
    },
    #[error("failed to parse hosts file {path}: {source}")]
    ParseHosts {
        source: toml::de::Error,
        path: PathBuf,
    },
}

type Result<T> = std::result::Result<T, HostError>;
use once_cell::sync::OnceCell;
use serde::Deserialize;

static HOSTS: OnceCell<Hosts> = OnceCell::new();

/// Handles loading of the hosts configuration.
#[derive(Debug, Deserialize)]
pub struct Hosts {
    pub master: SocketAddr,
    /// The slaves have the format "user@address", e.g. "worker@192.168.0.2"
    pub slaves: Vec<String>,
}

impl Hosts {
    pub fn get() -> Result<&'static Hosts> {
        HOSTS.get_or_try_init(Self::load)
    }

    fn load() -> Result<Self> {
        let home = std::env::home_dir().ok_or(HostError::NoHome)?;
        Hosts::load_from(home.join("hosts.conf"))
    }

    fn load_from<P: AsRef<Path>>(path: P) -> Result<Self> {
        let s = std::fs::read_to_string(&path).map_err(|e| HostError::LoadHosts {
            source: e,
            path: path.as_ref().into(),
        })?;

        toml::from_str(&s).map_err(|e| HostError::ParseHosts {
            source: e,
            path: path.as_ref().into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_missing_hosts_file() {
        match Hosts::load_from("/does_not_exist").unwrap_err() {
            HostError::LoadHosts { .. } => {}
            _ => panic!("Expected HostError::LoadHosts"),
        }
    }

    #[test]
    fn test_invalid_hosts_file() {
        let (mut file, path) = tempfile::NamedTempFile::new().unwrap().keep().unwrap();
        file.write_all("invalid data".as_ref()).unwrap();

        match Hosts::load_from(&path).unwrap_err() {
            HostError::ParseHosts { .. } => {}
            _ => panic!("Expected HostError::ParseHosts"),
        }
    }
}
