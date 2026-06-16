//! Integration binary for distributed cache + locality test.
//!
//! Validates:
//! - A cached RDD's second action does not recompute partitions that are still
//!   held by a live worker (locality-pinned cache read, not recompute).
//! - After a worker dies between actions, the partitions it held are
//!   recomputed on a surviving worker while the survivor's own partitions are
//!   still served from cache.
//!
//! Each element is tagged with a process-local call counter at compute time, so
//! the driver can tell a cache hit (call id unchanged across actions) from a
//! recompute (call id advances) without any shared state between processes.
//!
//! Run as worker:
//!   ./integration_cache_locality --worker --port 19401
//!
//! Run as driver:
//!   ./integration_cache_locality --driver --workers 127.0.0.1:19401,127.0.0.1:19402
//!   The driver sleeps between the first and second collect so the test harness
//!   has a window to kill a worker in between.
//!   Output: {"first":[[1,0],[2,1],[3,0],[4,1]],"second":[[1,0],[2,1],[3,0],[4,1]]}

use atomic_compute::context::{Context, start_worker};
use atomic_compute::env::Config;
use atomic_compute::task;
use clap::Parser;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[path = "cli.rs"]
mod cli;
use cli::IntegrationCli;

static CALLS: AtomicUsize = AtomicUsize::new(0);

#[task]
fn tag_call(x: i32) -> (i32, usize) {
    let n = CALLS.fetch_add(1, Ordering::SeqCst);
    (x, n)
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
            "usage:\n  integration_cache_locality --worker --port N\n  \
             integration_cache_locality --driver --workers host:port[,...]\n"
        );
        std::process::exit(1);
    }
}

fn run_driver(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::new_with_config(config)?;
    let data: Vec<i32> = (1..=4).collect();

    let rdd = ctx.parallelize_typed(data, 2).map_task(TagCall).cache();

    let first = rdd.collect()?;

    // Give the test harness a window to kill a worker before the second action.
    std::thread::sleep(Duration::from_millis(1500));

    let second = rdd.collect()?;

    println!(
        "{}",
        serde_json::json!({ "first": first, "second": second })
    );
    Ok(())
}
