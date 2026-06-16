//! Integration binary for fault-tolerance test.
//!
//! Validates:
//! - Dispatching a job with 2 workers and killing one mid-execution
//! - Driver retries tasks on the surviving worker
//! - Result is still correct despite worker failure
//! - Worker removal after 3 consecutive TCP failures
//!
//! Run as worker:
//!   ./integration_fault_tolerance --worker --port 19303
//!
//! Run as driver with fault injection:
//!   ./integration_fault_tolerance --driver \
//!     --workers 127.0.0.1:19303,127.0.0.1:19304 \
//!     --kill-worker 127.0.0.1:19304
//!   Expected output: {"sum":20,"doubled":[2,4,6,8,10,12,14,16,18,20]}

use atomic_compute::context::{Context, start_worker};
use atomic_compute::env::Config;
use atomic_compute::task;
use clap::Parser;
use std::net::Ipv4Addr;

#[path = "cli.rs"]
mod cli;
use cli::IntegrationCli;

#[task]
fn double_i32(x: i32) -> i32 {
    x * 2
}

#[task]
fn add_i32(a: i32, b: i32) -> i32 {
    a + b
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
            "usage:\n  integration_fault_tolerance --worker --port N\n  \
             integration_fault_tolerance --driver --workers host:port[,...]\n"
        );
        std::process::exit(1);
    }
}

fn run_driver(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::new_with_config(config)?;

    let data: Vec<i32> = (1..=10).collect();

    // Double each element — spread across workers.
    let doubled = ctx
        .parallelize_typed(data.clone(), 4)
        .map_task(DoubleI32)
        .collect()?;

    // Sum all — uses fold_task which also dispatches to workers.
    let sum = ctx.parallelize_typed(data, 4).fold_task(0i32, AddI32)?;

    println!("{}", serde_json::json!({ "sum": sum, "doubled": doubled }));
    Ok(())
}
