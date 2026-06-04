/// atomic-worker — standalone deployable worker binary.
///
/// This exists as a separate binary so worker nodes can be provisioned without
/// shipping driver application code. A driver binary built with `AtomicApp::build()`
/// also supports `--worker`, but couples the worker to the driver's task registry.
/// `atomic-worker` is the generic option: any cluster node can run it without knowing
/// which driver application it will serve.
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

    start_worker(Config::worker(local_ip, port));
}
