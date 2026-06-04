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
    pub async fn build() -> Result<Self, Error> {
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

        let config = if worker_addrs.is_empty() {
            Config::local()
        } else {
            Config::distributed_driver(local_ip, worker_addrs)
        };

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
    pub fn driver_context(&self) -> Result<Arc<Context>, Error> {
        self.ctx.clone().ok_or(Error::UnsupportedOperation(
            "driver_context() called on a Worker-role AtomicApp — use run_worker() instead",
        ))
    }

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
