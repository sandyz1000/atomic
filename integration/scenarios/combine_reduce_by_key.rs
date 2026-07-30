//! Scenario: opt-in map-side pre-combine for `reduce_by_key_task` (the `C == V` case).
//!
//! A staged `_task` pipeline (`flat_map_task`) precedes the shuffle and
//! `register_combine!(String, i64)` is present, so in distributed mode a
//! `CombineByKey` step runs on each worker immediately before the shuffle write —
//! folding same-key counts within the map partition so fewer pairs cross the network.
//!
//! Uses `(String, i64)` (not the `(String, i32)` the plain wordcount scenarios use) so
//! registering a combine handler here cannot affect those fallback-path scenarios in the
//! same binary. The result must still be the correct word counts — map-side pre-combine is
//! transparent for `C == V`.
//!
//! Expected output: {"hello":2,"of":1,"rust":2,"world":2}

use atomic_compute::context::Context;
use atomic_compute::task;
use std::error::Error;
use std::sync::Arc;

atomic_compute::register_shuffle_map!(String, i64);
atomic_compute::register_combine!(String, i64);

#[task]
fn tokenize_i64(line: String) -> Vec<(String, i64)> {
    line.split_whitespace()
        .map(|w| (w.to_lowercase(), 1i64))
        .collect()
}

#[task]
fn add_i64(a: i64, b: i64) -> i64 {
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
        .flat_map_task(TokenizeI64) // staged pipeline precedes the shuffle
        .reduce_by_key_task(AddI64) // map-side CombineByKey runs before ShuffleMap
        .collect()?;

    word_counts.sort_by_key(|(k, _)| k.clone());

    let map: serde_json::Map<String, serde_json::Value> = word_counts
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::Number(v.into())))
        .collect();
    println!("{}", serde_json::Value::Object(map));
    Ok(())
}
