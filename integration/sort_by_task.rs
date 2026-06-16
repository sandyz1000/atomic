//! Integration binary for distributed `sort_by_task` test.
//!
//! Validates that a non-pair RDD sorted via `sort_by_task` is globally ordered
//! across worker-produced partitions with no client-side re-sort: range
//! partitioning + per-partition sort-shuffle happens entirely on the workers.
//!
//! Run as worker:
//!   ./integration_sort_by_task --worker --port 19601
//!
//! Run as driver:
//!   ./integration_sort_by_task --driver --workers 127.0.0.1:19601
//!   Output: {"partitions":[[0,1,2],[3,4,5,6],[7,8,9]]}  (illustrative)

use atomic_compute::context::{Context, start_worker};
use atomic_compute::env::Config;
use atomic_compute::task;
use clap::Parser;
use std::net::Ipv4Addr;

#[path = "cli.rs"]
mod cli;
use cli::IntegrationCli;

atomic_compute::register_sort_shuffle_map!(i32, i32);

#[task]
fn self_key(x: i32) -> (i32, i32) {
    (x, x)
}

fn main() {
    let cli = IntegrationCli::parse();
    let _ = env_logger::try_init();

    if cli.worker {
        let port = cli.port.expect("--worker requires --port N");
        let config = Config::worker(Ipv4Addr::LOCALHOST, port);
        start_worker(config);
    } else if cli.driver {
        let config = Config::distributed_driver(Ipv4Addr::LOCALHOST, cli.workers);
        if let Err(e) = run_driver(config) {
            eprintln!("driver error: {e}");
            std::process::exit(1);
        }
    } else {
        eprintln!(
            "usage:\n  integration_sort_by_task --worker --port N\n  \
             integration_sort_by_task --driver --workers host:port[,...]\n"
        );
        std::process::exit(1);
    }
}

fn run_driver(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::new_with_config(config)?;
    let data: Vec<i32> = vec![5, 3, 8, 1, 9, 2, 7, 4, 6, 0];

    let partitions = ctx
        .parallelize_typed(data, 3)
        .sort_by_task(SelfKey, true)
        .collect_partitions()?;

    println!("{}", serde_json::json!({ "partitions": partitions }));
    Ok(())
}
