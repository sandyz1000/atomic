use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;

use crate::context::{Context, start_worker};
use crate::env::Config;
use crate::error::Error;

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
/// After `build()` completes:
/// - In **worker** mode the executor loop has already started and the process
///   will exit when the driver sends a shutdown signal.
/// - In **driver** mode `driver_context()` returns the live [`Context`].
pub struct AtomicApp {
    pub role: AppRole,
    ctx: Option<Arc<Context>>,
}

impl AtomicApp {
    /// Parse CLI arguments, build a [`Config`], and launch the appropriate role.
    ///
    /// Recognised flags:
    /// - `--worker [--port N] [--local-ip ADDR]`  — start the worker loop (never returns)
    /// - `--workers addr:port,...`                  — driver connecting to these workers
    /// - `--local-ip ADDR`                          — override the local IP for the driver
    /// - no flags                                   — local driver (single-process)
    pub async fn build() -> Result<Self, Error> {
        let args: Vec<String> = std::env::args().collect();

        // ── Worker path ──────────────────────────────────────────────────────
        if args.iter().any(|a| a == "--worker") {
            let port = args
                .windows(2)
                .find(|w| w[0] == "--port")
                .and_then(|w| w[1].parse::<u16>().ok())
                .unwrap_or(10000);

            let local_ip = Self::parse_local_ip(&args);
            let config = Config::worker(local_ip, port);

            let _ = env_logger::try_init();
            log::info!("[worker-{}] process started pid={}", port, std::process::id());

            // start_worker never returns.
            start_worker(config);
        }

        // ── Driver path ──────────────────────────────────────────────────────
        let worker_addrs = Self::worker_addresses(&args);
        let local_ip = Self::parse_local_ip(&args);

        let config = if worker_addrs.is_empty() {
            // No --workers given: run fully local.
            Config::local()
        } else {
            Config::distributed_driver(local_ip, worker_addrs)
        };

        let ctx = Context::new_with_config(config)?;
        Ok(AtomicApp {
            role: AppRole::Driver,
            ctx: Some(ctx),
        })
    }

    /// Returns the driver context.
    ///
    /// Panics if called from a worker process (which never reaches this point).
    pub fn driver_context(&self) -> Result<Arc<Context>, Error> {
        self.ctx
            .clone()
            .ok_or(Error::UnsupportedOperation("driver_context called on a worker"))
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// Parse `--workers host:port,...` into a `Vec<SocketAddrV4>`.
    pub fn worker_addresses(args: &[String]) -> Vec<SocketAddrV4> {
        args.windows(2)
            .find(|w| w[0] == "--workers")
            .map(|w| {
                w[1].split(',')
                    .filter_map(|addr| {
                        let mut parts = addr.rsplitn(2, ':');
                        let port = parts.next()?.parse::<u16>().ok()?;
                        let ip = parts.next()?.parse::<Ipv4Addr>().ok()?;
                        Some(SocketAddrV4::new(ip, port))
                    })
                    .collect()
            })
            .unwrap_or_default()
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
