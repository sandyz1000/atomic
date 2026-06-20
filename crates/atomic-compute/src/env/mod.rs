use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;

mod builder;
mod log_config;
mod runtime;

pub use builder::*;
pub use log_config::*;
pub use runtime::*;

/// Default bound (ms) for `Context::stop()` to wait for in-flight distributed
/// tasks to drain before proceeding with shutdown.
pub const DEFAULT_DRAIN_TIMEOUT_MS: u64 = 30_000;

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
    /// `(host, port)` of a DNS name to periodically re-resolve for live worker
    /// discovery (e.g. a Kubernetes headless Service). When `Some`, the driver
    /// re-resolves it every `heartbeat_interval_secs` and registers any newly
    /// appeared endpoints via `dynamically_add_worker`. `None` (default) means
    /// the worker set is fixed at startup. Set by `AtomicApp` from
    /// `--workers dns:<host>:<port>`.
    pub worker_dns: Option<(String, u16)>,
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
    /// Per-task timeout (seconds) for pipelines containing an `AgentStep` op. Multi-round
    /// LLM calls run far longer than the cheap-CPU-task default `task_timeout` (5 min).
    /// `None` (default) falls back to `AGENT_STEP_DEFAULT_TIMEOUT` (30 min) in the scheduler.
    pub agent_step_timeout_secs: Option<u64>,
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
    /// TCP port for the worker self-registration HTTP endpoint (`POST /register`).
    /// When set, the driver starts an HTTP listener; workers can POST a
    /// `RegisterRequest` JSON body to dynamically join the cluster at runtime.
    /// `None` (default) disables the registration endpoint.
    /// Env var: `ATOMIC_REGISTER_PORT`.
    pub register_port: Option<u16>,
    /// Reduce-partition count at or above which shuffle writes use the consolidated
    /// (sort-shuffle) layout — one DATA blob + offset INDEX per map task instead of one
    /// bucket per reduce partition. `None` (default) falls back to the
    /// `ATOMIC_SORT_SHUFFLE_THRESHOLD` env var, otherwise 200.
    pub sort_shuffle_threshold: Option<usize>,
    /// Bounded wait (ms) for in-flight distributed tasks to finish when
    /// `Context::stop()` is called. Default: 30000 (30s). The shutdown proceeds
    /// after the timeout regardless — workers are long-running daemons and an
    /// abandoned task is simply recomputed on the next job.
    pub drain_timeout_ms: u64,
}

/// Errors raised by [`Config::validate`].
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("worker config requires a non-zero listening port")]
    ZeroWorkerPort,

    #[error(
        "distributed driver requires at least one worker (`workers`) or \
         a `worker_dns` discovery target"
    )]
    NoWorkers,

    #[error("TLS requires tls_ca_cert, tls_cert, and tls_key together; only {0} set")]
    PartialTls(String),

    #[error("{name} does not exist or is not a file: {path}")]
    TlsFileMissing { name: String, path: String },

    #[error("{0} must not be 0")]
    ZeroPort(String),
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
            worker_dns: None,
            worker: None,
            log: LogConfig::default(),
            shuffle_spill_threshold: None,
            metrics_port: None,
            speculation_multiplier: None,
            agent_step_timeout_secs: None,
            coalesce_shuffle_threshold_bytes: 0,
            heartbeat_interval_secs: 0,
            heartbeat_timeout_ms: 2000,
            tls_ca_cert: None,
            tls_cert: None,
            tls_key: None,
            register_port: None,
            sort_shuffle_threshold: None,
            drain_timeout_ms: DEFAULT_DRAIN_TIMEOUT_MS,
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
            worker_dns: None,
            worker: None,
            log: LogConfig::default(),
            shuffle_spill_threshold: None,
            metrics_port: None,
            speculation_multiplier: None,
            agent_step_timeout_secs: None,
            coalesce_shuffle_threshold_bytes: 0,
            heartbeat_interval_secs: 0,
            heartbeat_timeout_ms: 2000,
            tls_ca_cert: None,
            tls_cert: None,
            tls_key: None,
            register_port: None,
            sort_shuffle_threshold: None,
            drain_timeout_ms: DEFAULT_DRAIN_TIMEOUT_MS,
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
            worker_dns: None,
            worker: Some(WorkerConfig::new(port)),
            log: LogConfig::default(),
            shuffle_spill_threshold: None,
            metrics_port: None,
            speculation_multiplier: None,
            agent_step_timeout_secs: None,
            coalesce_shuffle_threshold_bytes: 0,
            heartbeat_interval_secs: 0,
            heartbeat_timeout_ms: 2000,
            tls_ca_cert: None,
            tls_cert: None,
            tls_key: None,
            register_port: None,
            sort_shuffle_threshold: None,
            drain_timeout_ms: DEFAULT_DRAIN_TIMEOUT_MS,
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
            Some(WorkerConfig {
                port,
                max_concurrent_tasks,
            })
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

        let agent_step_timeout_secs = std::env::var(format!("{PREFIX}AGENT_STEP_TIMEOUT_SECS"))
            .ok()
            .and_then(|s| s.parse::<u64>().ok());

        let coalesce_shuffle_threshold_bytes =
            std::env::var(format!("{PREFIX}COALESCE_SHUFFLE_THRESHOLD_BYTES"))
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

        let register_port = std::env::var(format!("{PREFIX}REGISTER_PORT"))
            .ok()
            .and_then(|s| s.parse::<u16>().ok());

        let sort_shuffle_threshold = std::env::var(format!("{PREFIX}SORT_SHUFFLE_THRESHOLD"))
            .ok()
            .and_then(|s| s.parse::<usize>().ok());

        let drain_timeout_ms = std::env::var(format!("{PREFIX}DRAIN_TIMEOUT_MS"))
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_DRAIN_TIMEOUT_MS);

        // In env-var mode, workers come from hosts.conf — resolved by the caller.
        Config {
            local_ip,
            work_dir,
            mode,
            shuffle_port,
            workers: vec![],
            worker_dns: None,
            worker,
            log: LogConfig {
                log_level,
                log_cleanup,
            },
            shuffle_spill_threshold,
            metrics_port,
            speculation_multiplier,
            agent_step_timeout_secs,
            coalesce_shuffle_threshold_bytes,
            heartbeat_interval_secs,
            heartbeat_timeout_ms,
            tls_ca_cert,
            tls_cert,
            tls_key,
            register_port,
            sort_shuffle_threshold,
            drain_timeout_ms,
        }
    }

    pub fn is_local(&self) -> bool {
        self.mode == DeploymentMode::Local
    }

    pub fn is_distributed(&self) -> bool {
        self.mode == DeploymentMode::Distributed
    }

    /// Validate configuration invariants before `Context::new_with_config` acts on them.
    ///
    /// Catches misconfiguration up front (no reachable workers configured, a worker
    /// with no listening port, a partial TLS cert/key/CA triple, a TLS file that
    /// doesn't exist) instead of failing deep inside connection or handshake code
    /// with a less direct error.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.mode == DeploymentMode::Distributed {
            if let Some(worker) = &self.worker {
                if worker.port == 0 {
                    return Err(ConfigError::ZeroWorkerPort);
                }
            } else if self.workers.is_empty() && self.worker_dns.is_none() {
                return Err(ConfigError::NoWorkers);
            }
        }

        let tls_fields = [
            ("tls_ca_cert", &self.tls_ca_cert),
            ("tls_cert", &self.tls_cert),
            ("tls_key", &self.tls_key),
        ];
        let tls_set: Vec<&str> = tls_fields
            .iter()
            .filter(|(_, v)| v.is_some())
            .map(|(name, _)| *name)
            .collect();
        if !tls_set.is_empty() && tls_set.len() < tls_fields.len() {
            return Err(ConfigError::PartialTls(tls_set.join(", ")));
        }
        for (name, path) in tls_fields
            .iter()
            .filter_map(|(n, v)| v.as_ref().map(|p| (n, p)))
        {
            if !path.is_file() {
                return Err(ConfigError::TlsFileMissing {
                    name: name.to_string(),
                    path: path.display().to_string(),
                });
            }
        }

        for (name, port) in [
            ("shuffle_port", self.shuffle_port),
            ("metrics_port", self.metrics_port),
            ("register_port", self.register_port),
        ] {
            if port == Some(0) {
                return Err(ConfigError::ZeroPort(name.to_string()));
            }
        }

        Ok(())
    }

    /// Return a `ConfigBuilder` seeded from `Config::local()`.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder {
            inner: Config::local(),
        }
    }
}

#[cfg(test)]
mod config_validate_tests {
    use super::*;

    #[test]
    fn local_config_is_valid() {
        assert!(Config::local().validate().is_ok());
    }

    #[test]
    fn distributed_driver_needs_workers() {
        let config = Config::distributed_driver(Ipv4Addr::LOCALHOST, vec![]);
        assert!(config.validate().is_err());
    }

    #[test]
    fn distributed_driver_with_dns_is_valid() {
        let mut config = Config::distributed_driver(Ipv4Addr::LOCALHOST, vec![]);
        config.worker_dns = Some(("workers.svc".to_string(), 10001));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn worker_rejects_zero_port() {
        let mut config = Config::worker(Ipv4Addr::LOCALHOST, 10001);
        config.worker.as_mut().unwrap().port = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_zero_metrics_port() {
        let mut config = Config::local();
        config.metrics_port = Some(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_partial_tls_triple() {
        let mut config = Config::local();
        config.tls_cert = Some(PathBuf::from("/tmp/does-not-matter.pem"));
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_missing_tls_file() {
        let mut config = Config::local();
        config.tls_ca_cert = Some(PathBuf::from("/nonexistent/ca.pem"));
        config.tls_cert = Some(PathBuf::from("/nonexistent/cert.pem"));
        config.tls_key = Some(PathBuf::from("/nonexistent/key.pem"));
        assert!(config.validate().is_err());
    }
}
