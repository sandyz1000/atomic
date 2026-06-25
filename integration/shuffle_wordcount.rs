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
use clap::Parser;
use std::net::Ipv4Addr;

#[path = "cli.rs"]
mod cli;
use cli::IntegrationCli;

// Register shuffle handler for (String, i32) key-value pairs.
atomic_compute::register_shuffle_map!(String, i32);

#[task]
fn tokenize(line: String) -> Vec<(String, i32)> {
    line.split_whitespace()
        .map(|w| (w.to_lowercase(), 1i32))
        .collect()
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
        .reduce_by_key_task(AddI32)
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
