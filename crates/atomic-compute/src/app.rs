use std::net::{Ipv4Addr, SocketAddrV4, ToSocketAddrs};
use std::sync::Arc;

use crate::context::{Context, start_worker};
use crate::env::Config;
use crate::error::{ComputeError, ComputeResult};

/// A `dns:<host>:<port>` worker spec, retained so the driver can re-resolve the
/// headless Service periodically and pick up pods that scaled in/out.
#[derive(Debug, Clone)]
pub struct WorkerDns {
    pub host: String,
    pub port: u16,
}

/// Resolve a DNS hostname to the IPv4 worker endpoints behind it (e.g. the A
/// records of a Kubernetes headless Service). Non-fatal: an empty result just
/// means no workers are reachable yet.
pub fn resolve_worker_dns(host: &str, port: u16) -> Vec<SocketAddrV4> {
    match (host, port).to_socket_addrs() {
        Ok(addrs) => addrs
            .filter_map(|a| match a {
                std::net::SocketAddr::V4(v4) => Some(v4),
                std::net::SocketAddr::V6(_) => None,
            })
            .collect(),
        Err(e) => {
            log::warn!("worker DNS resolution failed for {host}:{port}: {e}");
            vec![]
        }
    }
}

/// The role this binary plays at runtime.
#[derive(Debug, Clone)]
pub enum AppRole {
    /// This process is the driver — it submits jobs and collects results.
    Driver,
    /// This process is a worker — it listens on `port` and executes tasks.
    Worker { port: u16 },
}

/// Entry point produced by [`AtomicApp::build`].
///
/// After `build()` completes, match on `role` to branch explicitly:
///
/// ```rust,ignore
/// let app = AtomicApp::build().await?;
/// match app.role {
///     AppRole::Worker { .. } => { app.run_worker(); }   // never returns
///     AppRole::Driver       => {
///         let ctx = app.driver_context()?;
///         // ... job logic
///     }
/// }
/// ```
pub struct AtomicApp {
    pub role: AppRole,
    ctx: Option<Arc<Context>>,
    /// Stored config for the worker path — populated by `build()` when `--worker`
    /// is passed. Consumed by `run_worker()`.
    worker_config: Option<Config>,
}

impl AtomicApp {
    /// Parse CLI arguments, build a [`Config`], and return an [`AtomicApp`] with
    /// the role stored. This function always returns — worker startup is deferred
    /// to [`run_worker`](AtomicApp::run_worker).
    ///
    /// Recognised flags:
    /// - `--worker [--port N] [--local-ip ADDR]`  — prepare worker role
    /// - `--driver [--workers addr:port,...] [--local-ip ADDR]`  — explicit driver mode
    /// - `--workers addr:port,...`                  — distributed driver
    /// - `--local-ip ADDR`                          — override the local IP
    /// - no flags                                   — local driver (single-process)
    ///
    /// `--driver` is the explicit counterpart to `--worker`. Both roles are in the
    /// same binary; match on [`AppRole`] after `build()` to branch:
    ///
    /// ```rust,ignore
    /// match app.role {
    ///     AppRole::Worker { .. } => { app.run_worker(); }
    ///     AppRole::Driver       => { /* job logic */ }
    /// }
    /// ```
    pub async fn build() -> ComputeResult<Self> {
        let args: Vec<String> = std::env::args().collect();

        let _ = env_logger::try_init();

        if args.iter().any(|a| a == "--worker") {
            let port = args
                .windows(2)
                .find(|w| w[0] == "--port")
                .and_then(|w| w[1].parse::<u16>().ok())
                .unwrap_or(10000);

            let local_ip = Self::parse_local_ip(&args);
            let config = Config::worker(local_ip, port);

            log::info!("[worker-{}] process ready pid={}", port, std::process::id());

            return Ok(AtomicApp {
                role: AppRole::Worker { port },
                ctx: None,
                worker_config: Some(config),
            });
        }

        if args.iter().any(|a| a == "--driver") {
            log::info!("role=driver (explicit --driver flag)");
        }

        let worker_addrs = Self::worker_addresses(&args);
        let local_ip = Self::parse_local_ip(&args);

        let mut config = if worker_addrs.is_empty() {
            Config::local()
        } else {
            Config::distributed_driver(local_ip, worker_addrs)
        };
        // If discovery was via `dns:host:port`, keep the spec so the driver can
        // re-resolve it and pick up workers that scale in/out (K8s autoscaling).
        config.worker_dns = Self::worker_dns(&args).map(|d| (d.host, d.port));

        let ctx = Context::new_with_config(config)?;
        Ok(AtomicApp {
            role: AppRole::Driver,
            ctx: Some(ctx),
            worker_config: None,
        })
    }

    /// Start the worker executor loop. This is the consumer side of the
    /// driver/worker split — analogous to `app.consume_from(queues)` in celery.
    ///
    /// Binds the TCP port declared in `--port`, initialises shuffle and UDF
    /// runtimes, then enters the task-dispatch loop. **Never returns.**
    ///
    /// # Panics
    ///
    /// Panics if called on an `AtomicApp` that was built without `--worker`.
    pub fn run_worker(&self) -> ! {
        let config = self
            .worker_config
            .clone()
            .expect("run_worker() called on a Driver AtomicApp — pass --worker to the binary");
        start_worker(config)
    }

    /// Returns the driver [`Context`].
    ///
    /// Returns an error if called on a Worker-role app — use [`run_worker`](AtomicApp::run_worker)
    /// in the `AppRole::Worker` match arm instead.
    pub fn driver_context(&self) -> ComputeResult<Arc<Context>> {
        self.ctx.clone().ok_or(ComputeError::UnsupportedOperation(
            "driver_context() called on a Worker-role AtomicApp — use run_worker() instead",
        ))
    }

    /// Parse `--workers host:port,...` into a `Vec<SocketAddrV4>`.
    ///
    /// Each comma-separated token is either a literal `ip:port` or a
    /// `dns:<host>:<port>` spec. A `dns:` token is resolved to every IPv4 A
    /// record behind `<host>` (e.g. the pods of a Kubernetes headless Service),
    /// each paired with `<port>`.
    pub fn worker_addresses(args: &[String]) -> Vec<SocketAddrV4> {
        args.windows(2)
            .find(|w| w[0] == "--workers")
            .map(|w| {
                w[1].split(',')
                    .flat_map(Self::resolve_worker_token)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Resolve a single `--workers` token to zero or more endpoints.
    fn resolve_worker_token(token: &str) -> Vec<SocketAddrV4> {
        if let Some(rest) = token.strip_prefix("dns:") {
            let mut parts = rest.rsplitn(2, ':');
            match (
                parts.next().and_then(|p| p.parse::<u16>().ok()),
                parts.next(),
            ) {
                (Some(port), Some(host)) => resolve_worker_dns(host, port),
                _ => vec![],
            }
        } else {
            let mut parts = token.rsplitn(2, ':');
            match (
                parts.next().and_then(|p| p.parse::<u16>().ok()),
                parts.next().and_then(|ip| ip.parse::<Ipv4Addr>().ok()),
            ) {
                (Some(port), Some(ip)) => vec![SocketAddrV4::new(ip, port)],
                _ => vec![],
            }
        }
    }

    /// Extract a `dns:<host>:<port>` worker spec from `--workers`, if present, so
    /// the driver can re-resolve it for live discovery. The first `dns:` token wins.
    pub fn worker_dns(args: &[String]) -> Option<WorkerDns> {
        let value = args
            .windows(2)
            .find(|w| w[0] == "--workers")
            .map(|w| w[1].as_str())?;
        for token in value.split(',') {
            if let Some(rest) = token.strip_prefix("dns:") {
                let mut parts = rest.rsplitn(2, ':');
                if let (Some(port), Some(host)) = (
                    parts.next().and_then(|p| p.parse::<u16>().ok()),
                    parts.next(),
                ) {
                    return Some(WorkerDns {
                        host: host.to_string(),
                        port,
                    });
                }
            }
        }
        None
    }

    /// Parse `--local-ip ADDR`, falling back to `127.0.0.1`.
    fn parse_local_ip(args: &[String]) -> Ipv4Addr {
        args.windows(2)
            .find(|w| w[0] == "--local-ip")
            .and_then(|w| w[1].parse().ok())
            .unwrap_or(Ipv4Addr::LOCALHOST)
    }
}

/// Convenience macro that builds the [`AtomicApp`] entry point.
#[macro_export]
macro_rules! app {
    () => {
        $crate::app::AtomicApp::build()
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(workers: &str) -> Vec<String> {
        vec!["bin".into(), "--workers".into(), workers.into()]
    }

    #[test]
    fn literal_addresses_parse() {
        let got = AtomicApp::worker_addresses(&args("127.0.0.1:10001,10.0.0.5:10002"));
        assert_eq!(
            got,
            vec![
                SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 10001),
                SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 5), 10002),
            ]
        );
    }

    #[test]
    fn dns_spec_extracted() {
        let got = AtomicApp::worker_dns(&args("dns:atomic-workers:10001")).unwrap();
        assert_eq!(got.host, "atomic-workers");
        assert_eq!(got.port, 10001);
    }

    #[test]
    fn literals_have_no_dns() {
        assert!(AtomicApp::worker_dns(&args("127.0.0.1:10001")).is_none());
    }

    #[test]
    fn dns_applies_port() {
        // `localhost` resolves to a loopback A record on supported hosts; we only
        // assert the port is applied, which holds regardless of address count.
        for addr in resolve_worker_dns("localhost", 9999) {
            assert_eq!(addr.port(), 9999);
        }
    }
}
