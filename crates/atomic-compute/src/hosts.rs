use crate::error::{ComputeError, ComputeResult};
use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

pub static HOSTS: OnceCell<Hosts> = OnceCell::new();

/// Handles loading of the hosts configuration.
#[derive(Debug, Deserialize)]
pub struct Hosts {
    pub master: SocketAddr,
    /// The slaves have the format "user@address", e.g. "worker@192.168.0.2"
    pub slaves: Vec<String>,
}

impl Hosts {
    pub fn get() -> ComputeResult<&'static Hosts> {
        HOSTS.get_or_try_init(Self::load)
    }

    fn load() -> ComputeResult<Self> {
        let home = std::env::home_dir().ok_or(ComputeError::NoHome)?;
        Hosts::load_from(home.join("hosts.conf"))
    }

    fn load_from<P: AsRef<Path>>(path: P) -> ComputeResult<Self> {
        let s = std::fs::read_to_string(&path).map_err(|e| ComputeError::LoadHosts {
            source: e,
            path: path.as_ref().into(),
        })?;

        toml::from_str(&s).map_err(|e| ComputeError::ParseHosts {
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
            ComputeError::LoadHosts { .. } => {}
            _ => panic!("Expected ComputeError::LoadHosts"),
        }
    }

    #[test]
    fn test_invalid_hosts_file() {
        let (mut file, path) = tempfile::NamedTempFile::new().unwrap().keep().unwrap();
        file.write_all("invalid data".as_ref()).unwrap();

        match Hosts::load_from(&path).unwrap_err() {
            ComputeError::ParseHosts { .. } => {}
            _ => panic!("Expected ComputeError::ParseHosts"),
        }
    }
}
