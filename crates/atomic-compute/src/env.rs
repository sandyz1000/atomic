use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::Arc;

use log::LevelFilter;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tokio::runtime::{Handle, Runtime};

pub const THREAD_PREFIX: &str = "_VEGA";
static ASYNC_RT: Lazy<Option<Runtime>> = Lazy::new(Env::build_async_executor);

/// Minimal env handle — only provides the async-runtime helper.
pub struct Env;

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

// ── Types ──────────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    Debug,
    Trace,
    Info,
}

impl LogLevel {
    pub fn is_debug_or_lower(self) -> bool {
        matches!(self, LogLevel::Debug | LogLevel::Trace)
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LogConfig {
    pub log_level: LogLevel,
    pub log_cleanup: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            log_level: LogLevel::Info,
            log_cleanup: !cfg!(debug_assertions),
        }
    }
}

// ── WorkerConfig ───────────────────────────────────────────────────────────────

/// Configuration for a worker process.
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub port: u16,
    pub max_concurrent_tasks: u16,
}

impl WorkerConfig {
    pub fn new(port: u16) -> Self {
        WorkerConfig {
            port,
            max_concurrent_tasks: num_cpus::get().max(1) as u16,
        }
    }
}

// ── Config ─────────────────────────────────────────────────────────────────────

/// Runtime configuration for a driver or worker process.
///
/// Build this at the program entry point — using [`Config::local`],
/// [`Config::distributed_driver`], or [`Config::worker`] — and pass it to
/// [`Context::new_with_config`] or [`start_worker`].
///
/// This replaces the global `OnceCell<Configuration>` pattern: configuration is
/// loaded once, up front, and flows through the program rather than being read
/// from environment variables on demand.
#[derive(Clone, Debug)]
pub struct Config {
    /// IP address of this host (used by shuffle server and worker registration).
    pub local_ip: Ipv4Addr,
    /// Directory for temporary job / shuffle files.
    pub work_dir: PathBuf,
    /// Deployment mode.
    pub mode: DeploymentMode,
    /// Port for the shuffle HTTP server (None = OS-assigned).
    pub shuffle_port: Option<u16>,
    /// Remote worker addresses (distributed-driver only).
    pub workers: Vec<SocketAddrV4>,
    /// Worker-specific settings; `Some` only for worker processes.
    pub worker: Option<WorkerConfig>,
    /// Logging configuration.
    pub log: LogConfig,
}

impl Config {
    /// Local-mode driver: all tasks run in-process on the driver.
    pub fn local() -> Self {
        Config {
            local_ip: Ipv4Addr::LOCALHOST,
            work_dir: std::env::temp_dir(),
            mode: DeploymentMode::Local,
            shuffle_port: None,
            workers: vec![],
            worker: None,
            log: LogConfig::default(),
        }
    }

    /// Distributed-mode driver: dispatches tasks to the given workers.
    pub fn distributed_driver(local_ip: Ipv4Addr, workers: Vec<SocketAddrV4>) -> Self {
        Config {
            local_ip,
            work_dir: std::env::temp_dir(),
            mode: DeploymentMode::Distributed,
            shuffle_port: None,
            workers,
            worker: None,
            log: LogConfig::default(),
        }
    }

    /// Distributed-mode worker: listens for tasks on the given port.
    pub fn worker(local_ip: Ipv4Addr, port: u16) -> Self {
        Config {
            local_ip,
            work_dir: std::env::temp_dir(),
            mode: DeploymentMode::Distributed,
            shuffle_port: None,
            workers: vec![],
            worker: Some(WorkerConfig::new(port)),
            log: LogConfig::default(),
        }
    }

    /// Load configuration from environment variables.
    ///
    /// Use this when you cannot explicitly construct `Config` at the entry point
    /// (e.g. Python/JS bindings, legacy code). Prefers explicit constructors for
    /// new Rust programs.
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();
        const PREFIX: &str = "VEGA_";

        let mode = std::env::var(format!("{PREFIX}DEPLOYMENT_MODE"))
            .ok()
            .and_then(|s| {
                serde_json::from_str::<DeploymentMode>(&format!("\"{}\"", s.to_lowercase())).ok()
            })
            .unwrap_or(DeploymentMode::Local);

        let work_dir = std::env::var(format!("{PREFIX}LOCAL_DIR"))
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);

        let log_level = std::env::var(format!("{PREFIX}LOG_LEVEL"))
            .ok()
            .and_then(|s| {
                serde_json::from_str::<LogLevel>(&format!("\"{}\"", s.to_lowercase())).ok()
            })
            .unwrap_or(LogLevel::Info);

        let log_cleanup = std::env::var(format!("{PREFIX}LOG_CLEANUP"))
            .ok()
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(!cfg!(debug_assertions));

        let local_ip: Ipv4Addr = std::env::var(format!("{PREFIX}LOCAL_IP"))
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                if mode == DeploymentMode::Distributed {
                    panic!("VEGA_LOCAL_IP is required in distributed mode");
                }
                Ipv4Addr::LOCALHOST
            });

        let shuffle_port = std::env::var(format!("{PREFIX}SHUFFLE_SERVICE_PORT"))
            .ok()
            .and_then(|s| s.parse().ok());

        let slave_deployment = std::env::var(format!("{PREFIX}SLAVE_DEPLOYMENT"))
            .ok()
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(false);

        let worker = if slave_deployment {
            let port = std::env::var(format!("{PREFIX}SLAVE_PORT"))
                .ok()
                .and_then(|s| s.parse::<u16>().ok())
                .expect("VEGA_SLAVE_PORT is required for worker processes");
            let max_concurrent_tasks =
                std::env::var(format!("{PREFIX}WORKER_MAX_CONCURRENT_TASKS"))
                    .ok()
                    .and_then(|s| s.parse::<u16>().ok())
                    .unwrap_or_else(|| num_cpus::get().max(1) as u16);
            Some(WorkerConfig { port, max_concurrent_tasks })
        } else {
            None
        };

        // In env-var mode, workers come from hosts.conf — resolved by the caller.
        Config {
            local_ip,
            work_dir,
            mode,
            shuffle_port,
            workers: vec![],
            worker,
            log: LogConfig { log_level, log_cleanup },
        }
    }

    pub fn is_local(&self) -> bool {
        self.mode == DeploymentMode::Local
    }

    pub fn is_distributed(&self) -> bool {
        self.mode == DeploymentMode::Distributed
    }
}

// ── Shuffle initialisation ─────────────────────────────────────────────────────

/// Initialise the shuffle infrastructure: starts the `ShuffleManager` HTTP server and
/// populates the `SHUFFLE_CACHE` and `SHUFFLE_SERVER_URI` statics in `atomic_data::env`.
///
/// This is idempotent — subsequent calls are no-ops.
/// Must be called before submitting any jobs that involve wide transformations.
pub fn init_shuffle(config: &Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use atomic_data::shuffle::cache::DashMapShuffleCache;
    use atomic_data::shuffle::config::ShuffleConfig;
    use atomic_data::shuffle::manager::ShuffleManager;

    if atomic_data::env::SHUFFLE_CACHE.get().is_some() {
        return Ok(());
    }

    let cache: Arc<dyn atomic_data::shuffle::cache::ShuffleCache> =
        Arc::new(DashMapShuffleCache::default());
    let _ = atomic_data::env::SHUFFLE_CACHE.set(cache.clone());

    let tracker = Arc::new(atomic_data::shuffle::MapOutputTracker::default());
    let _ = atomic_data::env::MAP_OUTPUT_TRACKER.set(tracker);

    let shuffle_config = ShuffleConfig::new(
        config.local_ip,
        config.work_dir.clone(),
        config.shuffle_port,
        config.log.log_cleanup,
    );

    let mgr = ShuffleManager::new(shuffle_config, cache)
        .map_err(|e| format!("failed to start ShuffleManager: {e}"))?;

    let _ = atomic_data::env::SHUFFLE_SERVER_URI.set(mgr.get_server_uri());

    log::info!("shuffle service started at {}", mgr.get_server_uri());
    Ok(())
}
