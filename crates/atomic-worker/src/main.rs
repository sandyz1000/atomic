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
use clap::Parser;
use std::net::Ipv4Addr;

#[derive(Parser)]
#[command(about = "Atomic standalone worker — listens for task envelopes over TCP")]
struct Cli {
    /// Port to listen on.
    #[arg(long)]
    port: u16,

    /// Local IP to bind/advertise.
    #[arg(long, default_value = "127.0.0.1")]
    local_ip: Ipv4Addr,
}

fn main() {
    let cli = Cli::parse();
    start_worker(Config::worker(cli.local_ip, cli.port));
}
