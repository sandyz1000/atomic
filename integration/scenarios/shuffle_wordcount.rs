//! Scenario: distributed shuffle word-count.
//!
//! Validates:
//! - Distributed multi-op pipeline ending in shuffle (reduce_by_key)
//! - `register_shuffle_map!(String, i32)` wires correctly
//! - Shuffle-map stage fires before the reduce stage
//! - `ShuffleFetcher` HTTP pull from worker completes correctly
//!
//! Expected output: {"hello":2,"of":1,"rust":2,"world":2}

use atomic_compute::context::Context;
use atomic_compute::task;
use std::error::Error;
use std::sync::Arc;

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

pub fn run_driver(ctx: &Arc<Context>) -> Result<(), Box<dyn Error>> {
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

    let map: serde_json::Map<String, serde_json::Value> = word_counts
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::Number(v.into())))
        .collect();
    println!("{}", serde_json::Value::Object(map));
    Ok(())
}
