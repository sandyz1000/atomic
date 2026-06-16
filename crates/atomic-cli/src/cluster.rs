use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub(crate) const CLUSTER_CONFIG_PATH: &str = ".atomic/cluster.toml";

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct ClusterConfig {
    pub(crate) workers: Vec<WorkerEntry>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct WorkerEntry {
    pub(crate) address: String,
}

pub(crate) fn save_cluster_config(workers: &[WorkerEntry]) -> crate::Result<()> {
    let config_path = PathBuf::from(CLUSTER_CONFIG_PATH);
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let toml = toml::to_string_pretty(&ClusterConfig {
        workers: workers.to_vec(),
    })?;
    fs::write(&config_path, toml)?;
    Ok(())
}

pub(crate) fn load_cluster_config() -> crate::Result<ClusterConfig> {
    let config_path = PathBuf::from(CLUSTER_CONFIG_PATH);
    if !config_path.exists() {
        return Ok(ClusterConfig::default());
    }
    let text = fs::read_to_string(&config_path)?;
    Ok(toml::from_str(&text)?)
}
