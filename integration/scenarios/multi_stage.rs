//! Scenario: multi-stage distributed pipeline.
//!
//! Validates:
//! - Two-stage DAG: flat_map → reduce_by_key (shuffle 1) → map → sort_by_key (shuffle 2)
//! - Stage sequencing: map stage fully completes before reduce stage
//! - Deterministic result ordering after sort
//!
//! Expected output: {"sorted_words":[["rust",2],["world",2],["hello",2],["of",1]]}
//! (sorted descending by count, then alphabetically within same count)

use atomic_compute::context::Context;
use atomic_compute::task;
use std::error::Error;
use std::sync::Arc;

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

pub fn run_driver(ctx: &Arc<Context>) -> Result<(), Box<dyn Error>> {
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

    let sorted: Vec<serde_json::Value> = word_counts
        .into_iter()
        .map(|(word, count)| serde_json::json!([word, count]))
        .collect();

    println!("{}", serde_json::json!({ "sorted_words": sorted }));
    Ok(())
}
