//! Self-contained distributed integration test helper.
//!
//! Run as worker:
//!   ./integration --worker --port 19201
//!
//! Run as driver:
//!   ./integration --driver --workers 127.0.0.1:19201
//!   Output: {"doubled":[2,4,6,8],"sum":10}

use std::net::Ipv4Addr;
use atomic_compute::context::{Context, start_worker};
use atomic_compute::env::Config;
use atomic_compute::task;

#[task]
fn double(x: i32) -> i32 {
    x * 2
}

#[task]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let _ = env_logger::try_init();

    if args.iter().any(|a| a == "--worker") {
        let port: u16 = args
            .windows(2)
            .find(|w| w[0] == "--port")
            .and_then(|w| w[1].parse().ok())
            .expect("--worker requires --port N");

        let config = Config::worker(Ipv4Addr::LOCALHOST, port);
        // start_worker enters the TCP loop and never returns.
        start_worker(config);
    } else if args.iter().any(|a| a == "--driver") {
        let workers: Vec<std::net::SocketAddrV4> = args
            .windows(2)
            .find(|w| w[0] == "--workers")
            .map(|w| {
                w[1].split(',')
                    .filter_map(|s| s.parse().ok())
                    .collect()
            })
            .unwrap_or_default();

        let config = Config::distributed_driver(Ipv4Addr::LOCALHOST, workers);

        if let Err(e) = run_driver(config) {
            eprintln!("driver error: {e}");
            std::process::exit(1);
        }
    } else {
        eprintln!(
            "usage:\n  integration --worker --port N\n  integration --driver --workers host:port[,...]"
        );
        std::process::exit(1);
    }
}

fn run_driver(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::new_with_config(config)?;

    let input = vec![1i32, 2, 3, 4];

    let doubled = ctx
        .parallelize_typed(input.clone(), 2)
        .map_task(Double)
        .collect()?;

    let sum = ctx
        .parallelize_typed(input, 2)
        .fold_task(0i32, Add)?;

    // Machine-readable JSON output — parsed by the integration test.
    println!(
        "{}",
        serde_json::json!({ "doubled": doubled, "sum": sum })
    );

    Ok(())
}
