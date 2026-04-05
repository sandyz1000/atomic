use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;

use crate::context::Context;
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
    /// Parse CLI arguments and build the application.
    ///
    /// Flags recognised:
    /// - `--worker [--port N]`  — start the worker loop on port N (default 10000)
    /// - `--driver`             — return a driver context (default if no flag given)
    pub async fn build() -> Result<Self, Error> {
        let args: Vec<String> = std::env::args().collect();
        if args.iter().any(|a| a == "--worker") {
            let port = args
                .windows(2)
                .find(|w| w[0] == "--port")
                .and_then(|w| w[1].parse::<u16>().ok())
                .unwrap_or(10000);

            crate::env::Env::run_in_async_rt(move || -> crate::error::LibResult<()> {
                let executor = Arc::new(crate::executor::Executor::new(port));
                executor.worker().map(|_| ())
            })?;

            std::process::exit(0);
        }

        let ctx = Context::new()?;
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

    /// Worker addresses parsed from `--workers host1:port,host2:port,...`
    pub fn worker_addresses() -> Vec<SocketAddrV4> {
        let args: Vec<String> = std::env::args().collect();
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
}

/// Convenience macro that builds the [`AtomicApp`] entry point.
///
/// ```ignore
/// #[tokio::main]
/// async fn main() -> atomic_compute::error::Result<()> {
///     let app = atomic::app!().await?;
///     let ctx = app.driver_context()?;
///     // ...
///     Ok(())
/// }
/// ```
#[macro_export]
macro_rules! app {
    () => {
        $crate::app::AtomicApp::build()
    };
}
