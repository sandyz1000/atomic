use std::net::{Ipv4Addr, SocketAddrV4, ToSocketAddrs};
use std::sync::Arc;

use clap::Parser;

use crate::context::{Context, start_worker};
use crate::env::Config;
use crate::error::{ComputeError, ComputeResult};

/// Recognised CLI flags for a binary built on [`AtomicApp`]. `clap` owns the
/// entire CLI surface: unknown flags are rejected, so a user program embeds its
/// own options as `clap` args on the same parser rather than defining a parallel
/// argv scanner.
#[derive(Parser, Debug)]
#[command(about = "Atomic driver/worker entry point")]
struct AppArgs {
    /// Run as a worker: bind `--port` and enter the task-dispatch loop.
    #[arg(long)]
    worker: bool,
    /// Run as an explicit driver (the default when neither role flag is set).
    #[arg(long)]
    driver: bool,
    /// Worker listen port (worker role only).
    #[arg(long, default_value_t = 10000)]
    port: u16,
    /// Comma-separated `ip:port` or `dns:host:port` worker endpoints.
    #[arg(long)]
    workers: Option<String>,
    /// Override the local IP advertised to workers.
    #[arg(long, default_value_t = Ipv4Addr::LOCALHOST)]
    local_ip: Ipv4Addr,
}

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
        let args = AppArgs::parse();

        let _ = env_logger::try_init();

        if args.worker {
            let config = Config::worker(args.local_ip, args.port);

            log::info!(
                "[worker-{}] process ready pid={}",
                args.port,
                std::process::id()
            );

            return Ok(AtomicApp {
                role: AppRole::Worker { port: args.port },
                ctx: None,
                worker_config: Some(config),
            });
        }

        if args.driver {
            log::info!("role=driver (explicit --driver flag)");
        }

        let worker_addrs = Self::worker_addresses(args.workers.as_deref());

        let mut config = if worker_addrs.is_empty() {
            Config::local()
        } else {
            Config::distributed_driver(args.local_ip, worker_addrs)
        };
        // If discovery was via `dns:host:port`, keep the spec so the driver can
        // re-resolve it and pick up workers that scale in/out (K8s autoscaling).
        config.worker_dns = Self::worker_dns(args.workers.as_deref()).map(|d| (d.host, d.port));
        // `Config::local`/`distributed_driver` default to AllocatorKind::Static; overlay
        // ATOMIC_ALLOCATOR/ATOMIC_K8S_* so a driver launched by `atomic submit-k8s
        // --dynamic-workers` can still reach AllocatorKind::Kube through this entry point.
        (config.allocator, config.kube) = crate::env::allocator_from_env();

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
    /// Binds the TCP port declared in `--port`, initialises shuffle and task
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
    pub fn worker_addresses(workers: Option<&str>) -> Vec<SocketAddrV4> {
        workers
            .map(|w| w.split(',').flat_map(Self::resolve_worker_token).collect())
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
    pub fn worker_dns(workers: Option<&str>) -> Option<WorkerDns> {
        let value = workers?;
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

    #[test]
    fn literal_addresses_parse() {
        let got = AtomicApp::worker_addresses(Some("127.0.0.1:10001,10.0.0.5:10002"));
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
        let got = AtomicApp::worker_dns(Some("dns:atomic-workers:10001")).unwrap();
        assert_eq!(got.host, "atomic-workers");
        assert_eq!(got.port, 10001);
    }

    #[test]
    fn literals_have_no_dns() {
        assert!(AtomicApp::worker_dns(Some("127.0.0.1:10001")).is_none());
    }

    #[test]
    fn cli_parses() {
        let a = AppArgs::parse_from(["atomic", "--worker", "--port", "10005"]);
        assert!(a.worker);
        assert_eq!(a.port, 10005);
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
