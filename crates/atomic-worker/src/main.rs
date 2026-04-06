/// atomic-worker — polyglot distributed task worker.
///
/// Starts an Atomic worker process that handles native Rust tasks, Python UDFs,
/// and JavaScript UDFs. Run one instance per worker node:
///
///   ATOMIC_MODE=worker ATOMIC_PORT=10001 atomic-worker
///
/// Configure workers in `hosts.conf`, then run the driver with:
///
///   ATOMIC_MODE=distributed ATOMIC_MASTER=127.0.0.1:10001 python job.py
use atomic_compute::context::Context;

fn main() {
    env_logger::init();
    log::info!("atomic-worker starting");
    // Context::new() in distributed non-driver mode enters the worker loop and
    // never returns (calls std::process::exit on shutdown).
    if let Err(e) = Context::new() {
        log::error!("worker failed to start: {}", e);
        std::process::exit(1);
    }
}
