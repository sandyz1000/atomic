use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::Arc;

use log::LevelFilter;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tokio::runtime::{Handle, Runtime};

pub const THREAD_PREFIX: &str = "_ATOMIC";
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
            panic!(
                "no Tokio runtime available: \
                 call Env::run_in_async_rt from within a Tokio context \
                 or before the ASYNC_RT lazy was initialised"
            )
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
                    .expect("failed to build Tokio multi-thread runtime for Atomic"),
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
    /// When `Some(bytes)`, shuffle buckets that would push in-memory usage above this threshold
    /// are spilled to disk.  `None` keeps all shuffle data in memory (default).
    pub shuffle_spill_threshold: Option<usize>,
    /// TCP port for the Prometheus `/metrics` HTTP endpoint on the driver.
    /// `None` (default) disables the metrics server.
    pub metrics_port: Option<u16>,
    /// Speculative execution multiplier.  When `Some(m)`, any task that has been running
    /// longer than `m × median_task_duration` (after ≥50% of the stage has completed)
    /// gets a speculative re-run on a different worker.  The first result wins; the
    /// duplicate is discarded.  `None` (default) disables speculation.
    pub speculation_multiplier: Option<f64>,
    /// Adaptive shuffle coalescing threshold (bytes).  After the shuffle-map stage
    /// completes, reduce partitions whose total byte content is smaller than
    /// `coalesce_shuffle_threshold_bytes / original_num_partitions` are merged with
    /// adjacent partitions.  `0` (default) disables coalescing.
    pub coalesce_shuffle_threshold_bytes: u64,
    /// How often (in seconds) the driver sends a heartbeat probe to each worker.
    /// `0` (default) disables proactive heartbeating; failures are still detected
    /// reactively via TCP errors during task dispatch.
    pub heartbeat_interval_secs: u64,
    /// Per-probe timeout in milliseconds for the heartbeat HTTP GET `/health` call.
    /// Only used when `heartbeat_interval_secs > 0`.  Default: 2000 ms.
    pub heartbeat_timeout_ms: u64,
    /// Path to the cluster CA certificate (PEM) used for mutual TLS.
    /// `None` (default) disables TLS — plain TCP is used instead.
    /// Requires the `tls` feature flag.
    pub tls_ca_cert: Option<std::path::PathBuf>,
    /// Path to this process's TLS certificate (PEM). Required when `tls_ca_cert` is set.
    pub tls_cert: Option<std::path::PathBuf>,
    /// Path to this process's TLS private key (PEM). Required when `tls_ca_cert` is set.
    pub tls_key: Option<std::path::PathBuf>,
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
            shuffle_spill_threshold: None,
            metrics_port: None,
            speculation_multiplier: None,
            coalesce_shuffle_threshold_bytes: 0,
            heartbeat_interval_secs: 0,
            heartbeat_timeout_ms: 2000,
            tls_ca_cert: None,
            tls_cert: None,
            tls_key: None,
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
            shuffle_spill_threshold: None,
            metrics_port: None,
            speculation_multiplier: None,
            coalesce_shuffle_threshold_bytes: 0,
            heartbeat_interval_secs: 0,
            heartbeat_timeout_ms: 2000,
            tls_ca_cert: None,
            tls_cert: None,
            tls_key: None,
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
            shuffle_spill_threshold: None,
            metrics_port: None,
            speculation_multiplier: None,
            coalesce_shuffle_threshold_bytes: 0,
            heartbeat_interval_secs: 0,
            heartbeat_timeout_ms: 2000,
            tls_ca_cert: None,
            tls_cert: None,
            tls_key: None,
        }
    }

    /// Load configuration from environment variables.
    ///
    /// Use this when you cannot explicitly construct `Config` at the entry point
    /// (e.g. Python/JS bindings, legacy code). Prefers explicit constructors for
    /// new Rust programs.
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();
        const PREFIX: &str = "ATOMIC_";

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
                    panic!("ATOMIC_LOCAL_IP is required in distributed mode");
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
                .expect("ATOMIC_SLAVE_PORT is required for worker processes");
            let max_concurrent_tasks =
                std::env::var(format!("{PREFIX}WORKER_MAX_CONCURRENT_TASKS"))
                    .ok()
                    .and_then(|s| s.parse::<u16>().ok())
                    .unwrap_or_else(|| num_cpus::get().max(1) as u16);
            Some(WorkerConfig { port, max_concurrent_tasks })
        } else {
            None
        };

        let shuffle_spill_threshold = std::env::var(format!("{PREFIX}SHUFFLE_SPILL_THRESHOLD"))
            .ok()
            .and_then(|s| s.parse::<usize>().ok());

        let metrics_port = std::env::var(format!("{PREFIX}METRICS_PORT"))
            .ok()
            .and_then(|s| s.parse::<u16>().ok());

        let speculation_multiplier = std::env::var(format!("{PREFIX}SPECULATION_MULTIPLIER"))
            .ok()
            .and_then(|s| s.parse::<f64>().ok());

        let coalesce_shuffle_threshold_bytes = std::env::var(
            format!("{PREFIX}COALESCE_SHUFFLE_THRESHOLD_BYTES"),
        )
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

        let heartbeat_interval_secs = std::env::var(format!("{PREFIX}HEARTBEAT_INTERVAL_SECS"))
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let heartbeat_timeout_ms = std::env::var(format!("{PREFIX}HEARTBEAT_TIMEOUT_MS"))
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(2000);

        let tls_ca_cert = std::env::var(format!("{PREFIX}TLS_CA_CERT"))
            .ok()
            .map(std::path::PathBuf::from);
        let tls_cert = std::env::var(format!("{PREFIX}TLS_CERT"))
            .ok()
            .map(std::path::PathBuf::from);
        let tls_key = std::env::var(format!("{PREFIX}TLS_KEY"))
            .ok()
            .map(std::path::PathBuf::from);

        // In env-var mode, workers come from hosts.conf — resolved by the caller.
        Config {
            local_ip,
            work_dir,
            mode,
            shuffle_port,
            workers: vec![],
            worker,
            log: LogConfig { log_level, log_cleanup },
            shuffle_spill_threshold,
            metrics_port,
            speculation_multiplier,
            coalesce_shuffle_threshold_bytes,
            heartbeat_interval_secs,
            heartbeat_timeout_ms,
            tls_ca_cert,
            tls_cert,
            tls_key,
        }
    }

    pub fn is_local(&self) -> bool {
        self.mode == DeploymentMode::Local
    }

    pub fn is_distributed(&self) -> bool {
        self.mode == DeploymentMode::Distributed
    }

    /// Return a `ConfigBuilder` seeded from `Config::local()`.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder { inner: Config::local() }
    }
}

/// Fluent builder for `Config`.
///
/// Equivalent to Spark's `SparkConf`-style builder.  Unset fields inherit their
/// defaults from `Config::local()`.
///
/// # Example
/// ```ignore
/// let config = Config::builder()
///     .app_name("my-app")
///     .workers(vec!["10.0.0.1:10001".parse().unwrap()])
///     .metrics_port(9090)
///     .build();
/// ```
pub struct ConfigBuilder {
    inner: Config,
}

impl ConfigBuilder {
    /// Set any config key by name, mirroring the `ATOMIC_*` env var convention.
    ///
    /// Recognized keys (case-insensitive, with or without `ATOMIC_` prefix):
    /// `work_dir`, `local_ip`, `shuffle_port`, `metrics_port`,
    /// `speculation_multiplier`, `heartbeat_interval_secs`, `heartbeat_timeout_ms`.
    pub fn set(mut self, key: &str, value: &str) -> Self {
        let k = key.trim_start_matches("ATOMIC_").to_lowercase();
        match k.as_str() {
            "work_dir" => self.inner.work_dir = std::path::PathBuf::from(value),
            "local_ip" => {
                if let Ok(ip) = value.parse() {
                    self.inner.local_ip = ip;
                }
            }
            "shuffle_port" => self.inner.shuffle_port = value.parse().ok(),
            "metrics_port" => self.inner.metrics_port = value.parse().ok(),
            "speculation_multiplier" => self.inner.speculation_multiplier = value.parse().ok(),
            "heartbeat_interval_secs" => {
                self.inner.heartbeat_interval_secs = value.parse().unwrap_or(0)
            }
            "heartbeat_timeout_ms" => {
                self.inner.heartbeat_timeout_ms = value.parse().unwrap_or(2000)
            }
            _ => {}
        }
        self
    }

    /// Set the driver's local IP address.
    pub fn local_ip(mut self, ip: std::net::Ipv4Addr) -> Self {
        self.inner.local_ip = ip;
        self
    }

    /// Set the remote worker addresses for distributed mode.
    pub fn workers(mut self, workers: Vec<std::net::SocketAddrV4>) -> Self {
        self.inner.workers = workers;
        self.inner.mode = DeploymentMode::Distributed;
        self
    }

    /// Set the Prometheus metrics server port.
    pub fn metrics_port(mut self, port: u16) -> Self {
        self.inner.metrics_port = Some(port);
        self
    }

    /// Enable speculative execution with the given multiplier.
    pub fn speculation_multiplier(mut self, m: f64) -> Self {
        self.inner.speculation_multiplier = Some(m);
        self
    }

    /// Set the shuffle spill threshold in bytes.
    pub fn shuffle_spill_threshold(mut self, bytes: usize) -> Self {
        self.inner.shuffle_spill_threshold = Some(bytes);
        self
    }

    /// Set the work directory for temporary files.
    pub fn work_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        self.inner.work_dir = dir.into();
        self
    }

    /// Consume the builder and return the final `Config`.
    pub fn build(self) -> Config {
        self.inner
    }
}

// ── Shuffle initialisation ─────────────────────────────────────────────────────

/// Initialise the shuffle infrastructure: starts the `ShuffleManager` HTTP server and
/// populates the `SHUFFLE_CACHE` and `SHUFFLE_SERVER_URI` statics in `atomic_data::env`.
///
/// This is idempotent — subsequent calls are no-ops.
/// Must be called before submitting any jobs that involve wide transformations.
pub fn init_shuffle(config: &Config) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use atomic_data::shuffle::cache::{DashMapShuffleCache, SpillableShuffleCache};
    use atomic_data::shuffle::config::ShuffleConfig;
    use atomic_data::shuffle::manager::ShuffleManager;

    if atomic_data::env::get_shuffle_server_uri().is_some() {
        return Ok(());
    }

    let shuffle_config = ShuffleConfig::new(
        config.local_ip,
        config.work_dir.clone(),
        config.shuffle_port,
        config.log.log_cleanup,
    );

    let cache: Arc<dyn atomic_data::shuffle::cache::ShuffleCache> =
        if let Some(threshold) = config.shuffle_spill_threshold {
            Arc::new(SpillableShuffleCache::new(
                shuffle_config.effective_spill_dir(),
                threshold,
            ))
        } else {
            Arc::new(DashMapShuffleCache::default())
        };
    atomic_data::env::set_shuffle_cache(cache.clone());

    let tracker = Arc::new(atomic_data::shuffle::MapOutputTracker::default());
    atomic_data::env::set_map_output_tracker(tracker);

    let mgr = ShuffleManager::new(shuffle_config, cache)
        .map_err(|e| format!("failed to start ShuffleManager: {e}"))?;

    let uri = mgr.get_server_uri();
    atomic_data::env::set_shuffle_server_uri(uri.clone());
    log::info!("shuffle service started at {uri}");
    Ok(())
}
