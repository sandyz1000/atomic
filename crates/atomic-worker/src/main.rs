/// atomic-worker — polyglot distributed task worker.
///
/// Starts a long-running Atomic worker process that accepts task requests over TCP.
/// Run one instance per worker node:
///
///   atomic-worker --port 10001
///   atomic-worker --port 10001 --local-ip 192.168.1.5
use atomic_compute::context::start_worker;
use atomic_compute::env::Config;
use std::net::Ipv4Addr;

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

    let local_ip: Ipv4Addr = args
        .windows(2)
        .find(|w| w[0] == "--local-ip")
        .and_then(|w| w[1].parse().ok())
        .unwrap_or(Ipv4Addr::LOCALHOST);

    let config = Config::worker(local_ip, port);

    // start_worker initialises shuffle, enters the TCP executor loop, and never returns.
    start_worker(config);
}
