//! Integration binary for distributed shuffle word-count test.
//!
//! Validates:
//! - Distributed multi-op pipeline ending in shuffle (reduce_by_key)
//! - `register_shuffle_map!(String, i32)` wires correctly
//! - Shuffle-map stage fires before the reduce stage
//! - `ShuffleFetcher` HTTP pull from worker completes correctly
//!
//! Run as worker:
//!   ./integration_shuffle_wordcount --worker --port 19301
//!
//! Run as driver:
//!   ./integration_shuffle_wordcount --driver --workers 127.0.0.1:19301
//!   Expected output: {"hello":2,"of":1,"rust":2,"world":2}

use atomic_compute::context::{Context, start_worker};
use atomic_compute::env::Config;
use atomic_compute::task;
use std::net::Ipv4Addr;

// Register shuffle handler for (String, i32) key-value pairs.
atomic_compute::register_shuffle_map!(String, i32);

#[task]
fn tokenize(line: String) -> Vec<(String, i32)> {
    line.split_whitespace()
        .map(|w| (w.to_lowercase(), 1i32))
        .collect()
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
        start_worker(config);
    } else if args.iter().any(|a| a == "--driver") {
        let workers: Vec<std::net::SocketAddrV4> = args
            .windows(2)
            .find(|w| w[0] == "--workers")
            .map(|w| w[1].split(',').filter_map(|s| s.parse().ok()).collect())
            .unwrap_or_default();

        let config = Config::distributed_driver(Ipv4Addr::LOCALHOST, workers);
        if let Err(e) = run_driver(config) {
            eprintln!("driver error: {e}");
            std::process::exit(1);
        }
    } else {
        eprintln!(
            "usage:\n  integration_shuffle_wordcount --worker --port N\n  \
             integration_shuffle_wordcount --driver --workers host:port[,...]"
        );
        std::process::exit(1);
    }
}

fn run_driver(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::new_with_config(config)?;

    let lines = vec![
        "hello world".to_string(),
        "hello rust".to_string(),
        "world of rust".to_string(),
    ];

    let mut word_counts = ctx
        .parallelize_typed(lines, 2)
        .flat_map_task(Tokenize)
        .reduce_by_key(|a, b| a + b)
        .collect()?;

    word_counts.sort_by_key(|(k, _)| k.clone());

    // Emit machine-readable JSON — parsed by the integration test.
    let map: serde_json::Map<String, serde_json::Value> = word_counts
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::Number(v.into())))
        .collect();
    println!("{}", serde_json::Value::Object(map));
    Ok(())
}
