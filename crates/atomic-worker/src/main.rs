/// atomic-worker — polyglot distributed task worker.
///
/// Starts a long-running Atomic worker process that accepts task requests over
/// TCP. Run one instance per worker node:
///
///   atomic-worker --port 10001
///   atomic-worker --port 10001 --local-ip 192.168.1.5
///
/// Configure workers in `hosts.conf`, then run the driver:
///
///   VEGA_DEPLOYMENT_MODE=distributed VEGA_LOCAL_IP=127.0.0.1 \
///     ./my_driver_binary
use atomic_compute::context::start_worker;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let port: u16 = args
        .windows(2)
        .find(|w| w[0] == "--port")
        .and_then(|w| w[1].parse().ok())
        .unwrap_or_else(|| {
            eprintln!("Usage: atomic-worker --port N [--local-ip ADDR]");
            std::process::exit(1);
        });

    let local_ip = args
        .windows(2)
        .find(|w| w[0] == "--local-ip")
        .map(|w| w[1].as_str())
        .unwrap_or("127.0.0.1");

    // Configure via environment variables before the OnceCell initialises.
    // SAFETY: single-threaded here — no other threads have started yet.
    unsafe {
        std::env::set_var("VEGA_DEPLOYMENT_MODE", "distributed");
        std::env::set_var("VEGA_SLAVE_DEPLOYMENT", "true");
        std::env::set_var("VEGA_SLAVE_PORT", port.to_string());
        std::env::set_var("VEGA_LOCAL_IP", local_ip);
    }

    // start_worker() reads the configuration above and never returns.
    start_worker();
}
