//! Integration binary for multi-stage distributed pipeline test.
//!
//! Validates:
//! - Two-stage DAG: flat_map → reduce_by_key (shuffle 1) → map → sort_by_key (shuffle 2)
//! - Stage sequencing: map stage fully completes before reduce stage
//! - Deterministic result ordering after sort
//!
//! Run as worker:
//!   ./integration_multi_stage --worker --port 19302
//!
//! Run as driver:
//!   ./integration_multi_stage --driver --workers 127.0.0.1:19302
//!   Expected output: {"sorted_words":[["rust",2],["world",2],["hello",2],["of",1]]}
//!   (sorted descending by count, then alphabetically within same count)

use atomic_compute::context::{Context, start_worker};
use atomic_compute::env::Config;
use atomic_compute::task;
use clap::Parser;
use std::net::Ipv4Addr;

#[path = "cli.rs"]
mod cli;
use cli::IntegrationCli;

// Register handlers for both KV type pairs used in this pipeline.
atomic_compute::register_shuffle_map!(String, i32);
atomic_compute::register_shuffle_map!(i32, String);

#[task]
fn tokenize_to_pairs(line: String) -> Vec<(String, i32)> {
    line.split_whitespace()
        .map(|w| (w.to_lowercase(), 1i32))
        .collect()
}

#[task]
fn sum_counts(a: i32, b: i32) -> i32 {
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
            "usage:\n  integration_multi_stage --worker --port N\n  \
             integration_multi_stage --driver --workers host:port[,...]"
        );
        std::process::exit(1);
    }
}

fn run_driver(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::new_with_config(config)?;

    let corpus = vec![
        "hello world".to_string(),
        "hello rust".to_string(),
        "world of rust".to_string(),
    ];

    // Stage 1: tokenize → reduce_by_key (first shuffle boundary)
    let mut word_counts = ctx
        .parallelize_typed(corpus, 2)
        .flat_map_task(TokenizeToPairs)
        .reduce_by_key_task(SumCounts)
        .collect()?;

    // Sort by count descending, then alphabetically — deterministic output.
    word_counts.sort_by(|(k1, v1), (k2, v2)| v2.cmp(v1).then(k1.cmp(k2)));

    // Emit machine-readable JSON.
    let sorted: Vec<serde_json::Value> = word_counts
        .into_iter()
        .map(|(word, count)| serde_json::json!([word, count]))
        .collect();

    println!("{}", serde_json::json!({ "sorted_words": sorted }));
    Ok(())
}
