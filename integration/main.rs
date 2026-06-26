//! Distributed integration test binary — one process, dispatched by scenario.
//!
//! Driver and worker are the same binary; the scenario only selects which
//! `run_driver` runs on the driver side. Worker mode is scenario-agnostic: it
//! just dispatches whatever task envelope the driver sends.
//!
//! Run as worker:
//!   ./integration --worker --port 19201
//!
//! Run as driver:
//!   ./integration shuffle_wordcount --driver --workers 127.0.0.1:19201

use atomic_compute::context::{Context, start_worker};
use atomic_compute::env::Config;
use clap::Parser;
use std::net::{Ipv4Addr, SocketAddrV4};

mod scenarios;
use scenarios::Scenario;

#[derive(Parser, Debug)]
struct Cli {
    /// Which scenario to run (driver mode only).
    #[arg(value_enum)]
    scenario: Option<Scenario>,

    /// Run as a worker, listening on `--port`.
    #[arg(long)]
    worker: bool,

    /// Run as a driver, dispatching to `--workers`.
    #[arg(long)]
    driver: bool,

    /// Listening port (worker mode).
    #[arg(long)]
    port: Option<u16>,

    /// Comma-separated `host:port` worker list (driver mode).
    #[arg(long, value_delimiter = ',')]
    workers: Vec<SocketAddrV4>,
}

fn main() {
    let cli = Cli::parse();
    let _ = env_logger::try_init();

    if cli.worker {
        let port = cli.port.expect("--worker requires --port N");
        let config = Config::worker(Ipv4Addr::LOCALHOST, port);
        // start_worker enters the TCP loop and never returns.
        start_worker(config);
    } else if cli.driver {
        let scenario = cli.scenario.expect("--driver requires a scenario argument");
        let config = Config::distributed_driver(Ipv4Addr::LOCALHOST, cli.workers);

        if let Err(e) = run_driver(scenario, config) {
            eprintln!("driver error: {e}");
            std::process::exit(1);
        }
    } else {
        eprintln!(
            "usage:\n  integration --worker --port N\n  \
             integration <scenario> --driver --workers host:port[,...]\n\n\
             scenarios: map_fold, shuffle_wordcount, multi_stage, fault_tolerance, \
             cache_locality, named_partitioner, sort_by_task"
        );
        std::process::exit(1);
    }
}

fn run_driver(scenario: Scenario, config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::new_with_config(config)?;
    scenarios::run(scenario, &ctx)
}
