use super::{Config, DeploymentMode};

/// Typed key for [`ConfigBuilder::set`].
///
/// Derives `serde::Deserialize` so config files can specify keys by their
/// snake_case names (e.g. `"work_dir"`, `"local_ip"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigKey {
    WorkDir,
    LocalIp,
    ShufflePort,
    MetricsPort,
    SpeculationMultiplier,
    AgentStepTimeoutSecs,
    HeartbeatIntervalSecs,
    HeartbeatTimeoutMs,
    RegisterPort,
}

/// Fluent builder for `Config`.
///
/// Unset fields inherit their defaults from `Config::local()`.
///
/// # Example
/// ```ignore
/// let config = Config::builder()
///     .workers(vec!["10.0.0.1:10001".parse().unwrap()])
///     .metrics_port(9090)
///     .build();
/// ```
pub struct ConfigBuilder {
    pub(super) inner: Config,
}

impl ConfigBuilder {
    /// Set a config field by typed key. The `value` is always a string and is
    /// parsed into the target type (e.g. `"9090"` → `u16` for `MetricsPort`).
    /// Parse failures are silently ignored; use the dedicated builder methods
    /// (e.g. [`metrics_port`](Self::metrics_port)) for compile-time safety.
    pub fn set(mut self, key: ConfigKey, value: &str) -> Self {
        match key {
            ConfigKey::WorkDir => self.inner.work_dir = std::path::PathBuf::from(value),
            ConfigKey::LocalIp => {
                if let Ok(ip) = value.parse() {
                    self.inner.local_ip = ip;
                }
            }
            ConfigKey::ShufflePort => self.inner.shuffle_port = value.parse().ok(),
            ConfigKey::MetricsPort => self.inner.metrics_port = value.parse().ok(),
            ConfigKey::SpeculationMultiplier => {
                self.inner.speculation_multiplier = value.parse().ok()
            }
            ConfigKey::AgentStepTimeoutSecs => {
                self.inner.agent_step_timeout_secs = value.parse().ok()
            }
            ConfigKey::HeartbeatIntervalSecs => {
                self.inner.heartbeat_interval_secs = value.parse().unwrap_or(0)
            }
            ConfigKey::HeartbeatTimeoutMs => {
                self.inner.heartbeat_timeout_ms = value.parse().unwrap_or(2000)
            }
            ConfigKey::RegisterPort => self.inner.register_port = value.parse().ok(),
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

    /// Per-task timeout (seconds) for pipelines containing an `AgentStep` op.
    /// Defaults to 30 minutes when unset — a multi-round LLM call needs far more
    /// headroom than the 5-minute default used for cheap CPU tasks.
    pub fn agent_step_timeout_secs(mut self, secs: u64) -> Self {
        self.inner.agent_step_timeout_secs = Some(secs);
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

    /// Enable the worker self-registration endpoint on the given port.
    pub fn register_port(mut self, port: u16) -> Self {
        self.inner.register_port = Some(port);
        self
    }

    /// Reduce-partition count at/above which shuffle writes use the consolidated
    /// sort-shuffle layout. `0` forces it on; `usize::MAX` keeps the legacy per-bucket layout.
    pub fn sort_shuffle_threshold(mut self, n: usize) -> Self {
        self.inner.sort_shuffle_threshold = Some(n);
        self
    }

    /// Bounded wait (ms) for in-flight distributed tasks to drain on `Context::stop()`.
    pub fn drain_timeout_ms(mut self, ms: u64) -> Self {
        self.inner.drain_timeout_ms = ms;
        self
    }

    /// Consume the builder and return the final `Config`.
    pub fn build(self) -> Config {
        self.inner
    }
}
