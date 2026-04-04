use std::fs;
use std::net::Ipv4Addr;
use std::path::PathBuf;

use atomic_data::distributed::ExecutionBackend;
use crate::error::Error;
use log::LevelFilter;
use once_cell::sync::{Lazy, OnceCell};
use serde::{Deserialize, Serialize};
use tokio::runtime::{Handle, Runtime};

const ENV_VAR_PREFIX: &str = "VEGA_";
pub(crate) const THREAD_PREFIX: &str = "_VEGA";
static CONF: OnceCell<Configuration> = OnceCell::new();
static ASYNC_RT: Lazy<Option<Runtime>> = Lazy::new(Env::build_async_executor);

/// Minimal env handle — only provides the async-runtime helper.
/// (Shuffle/cache trackers were removed as part of the Vega→Atomic rewrite.)
pub(crate) struct Env;

impl Env {
    /// Run a function inside the existing Tokio context (or the internal one).
    pub fn run_in_async_rt<F, R>(func: F) -> R
    where
        F: FnOnce() -> R,
    {
        if let Ok(handle) = Handle::try_current() {
            let _guard = handle.enter();
            func()
        } else if let Some(rt) = &*ASYNC_RT {
            let _guard = rt.enter();
            func()
        } else {
            unreachable!()
        }
    }

    fn build_async_executor() -> Option<Runtime> {
        if Handle::try_current().is_ok() {
            None
        } else {
            Some(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap(),
            )
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogLevel {
    Error,
    Warn,
    Debug,
    Trace,
    Info,
}

impl LogLevel {
    pub fn is_debug_or_lower(self) -> bool {
        use LogLevel::*;
        match self {
            Debug | Trace => true,
            _ => false,
        }
    }
}

impl From<LogLevel> for LevelFilter {
    fn from(val: LogLevel) -> LevelFilter {
        match val {
            LogLevel::Error => LevelFilter::Error,
            LogLevel::Warn => LevelFilter::Warn,
            LogLevel::Debug => LevelFilter::Debug,
            LogLevel::Trace => LevelFilter::Trace,
            _ => LevelFilter::Info,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentMode {
    Distributed,
    Local,
}

impl DeploymentMode {
    pub fn is_local(self) -> bool {
        matches!(self, DeploymentMode::Local)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Configuration {
    pub is_driver: bool,
    pub local_ip: Ipv4Addr,
    pub local_dir: std::path::PathBuf,
    pub deployment_mode: DeploymentMode,
    pub shuffle_svc_port: Option<u16>,
    pub slave: Option<SlaveConfig>,
    pub loggin: LogConfig,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SlaveConfig {
    pub deployment: bool,
    pub port: u16,
    pub backend: ExecutionBackend,
    pub max_concurrent_tasks: u16,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct LogConfig {
    pub log_level: LogLevel,
    pub log_cleanup: bool,
}

impl From<(bool, u16)> for SlaveConfig {
    fn from(config: (bool, u16)) -> Self {
        let (deployment, port) = config;
        SlaveConfig {
            deployment,
            port,
            backend: ExecutionBackend::Docker,
            max_concurrent_tasks: num_cpus::get().max(1) as u16,
        }
    }
}

impl Default for Configuration {
    fn default() -> Self {
        use DeploymentMode::*;

        // This may be a worker — try to get config from file first.
        if let Some(config) = Configuration::get_from_file() {
            return config;
        }

        // Load .env file if present, then read individual vars with VEGA_ prefix.
        let _ = dotenvy::dotenv();

        let deployment_mode = std::env::var(concat_prefix(ENV_VAR_PREFIX, "DEPLOYMENT_MODE"))
            .ok()
            .and_then(|s| serde_json::from_str::<DeploymentMode>(&format!("\"{}\"", s.to_lowercase())).ok())
            .unwrap_or(Local);

        let local_dir = std::env::var(concat_prefix(ENV_VAR_PREFIX, "LOCAL_DIR"))
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);

        let log_level = std::env::var(concat_prefix(ENV_VAR_PREFIX, "LOG_LEVEL"))
            .ok()
            .and_then(|s| serde_json::from_str::<LogLevel>(&format!("\"{}\"", s.to_lowercase())).ok())
            .unwrap_or(LogLevel::Info);

        let log_cleanup = std::env::var(concat_prefix(ENV_VAR_PREFIX, "LOG_CLEANUP"))
            .ok()
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(!cfg!(debug_assertions));

        log::debug!("Setting max log level to: {:?}", log_level);
        log::set_max_level(log_level.into());

        let local_ip: Ipv4Addr = std::env::var(concat_prefix(ENV_VAR_PREFIX, "LOCAL_IP"))
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                if deployment_mode == Distributed {
                    panic!("Local IP required while deploying in distributed mode.")
                } else {
                    Ipv4Addr::LOCALHOST
                }
            });

        let shuffle_service_port = std::env::var(concat_prefix(ENV_VAR_PREFIX, "SHUFFLE_SERVICE_PORT"))
            .ok()
            .and_then(|s| s.parse().ok());

        let slave_deployment = std::env::var(concat_prefix(ENV_VAR_PREFIX, "SLAVE_DEPLOYMENT"))
            .ok()
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(false);

        let (is_master, slave) = if slave_deployment {
            let port = std::env::var(concat_prefix(ENV_VAR_PREFIX, "SLAVE_PORT"))
                .ok()
                .and_then(|s| s.parse::<u16>().ok())
                .expect("Port required while deploying a worker.");
            let backend = std::env::var(concat_prefix(ENV_VAR_PREFIX, "WORKER_BACKEND"))
                .ok()
                .and_then(|s| serde_json::from_str::<ExecutionBackend>(&format!("\"{}\"", s.to_lowercase())).ok())
                .unwrap_or(ExecutionBackend::Docker);
            let max_concurrent_tasks = std::env::var(concat_prefix(ENV_VAR_PREFIX, "WORKER_MAX_CONCURRENT_TASKS"))
                .ok()
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or_else(|| num_cpus::get().max(1) as u16);
            (false, Some(SlaveConfig { deployment: true, port, backend, max_concurrent_tasks }))
        } else {
            (true, None)
        };

        Configuration {
            is_driver: is_master,
            local_ip,
            local_dir,
            deployment_mode,
            loggin: LogConfig { log_level, log_cleanup },
            shuffle_svc_port: shuffle_service_port,
            slave,
        }
    }
}

impl Configuration {
    pub fn get() -> &'static Configuration {
        CONF.get_or_init(Self::default)
    }

    fn get_from_file() -> Option<Configuration> {
        let binary_path = std::env::current_exe()
            .map_err(|_| Error::CurrentBinaryPath)
            .ok()?;
        let conf_file = binary_path.parent()?.join("config.toml");
        if conf_file.exists() {
            fs::read_to_string(conf_file)
                .map(|content| toml::from_str::<Configuration>(&content).ok())
                .ok()
                .flatten()
        } else {
            None
        }
    }
}

/// Concatenate prefix + suffix at compile time equivalent (runtime concat for env var names).
#[inline]
fn concat_prefix(prefix: &str, suffix: &str) -> String {
    format!("{}{}", prefix, suffix)
}
