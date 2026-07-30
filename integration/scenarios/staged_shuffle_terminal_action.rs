//! Scenario: staged map → `reduce_by_key_task` → a terminal `_task` action that is
//! NOT `.collect()`.
//!
//! Regression coverage for a bug where a terminal `_task` action after a shuffle that was
//! preceded by a staged `_task` pipeline created a second, wrongly-sized shuffle-map `Stage`
//! (sized from the shuffle dependency's 1-partition placeholder RDD instead of the real N
//! staged partitions) and resubmitted it, corrupting the already-correct `MapOutputTracker`
//! registration the real shuffle-map run had already produced.
//!
//! `.collect()` right after `reduce_by_key_task` does not reach this: an empty step pipeline
//! short-circuits `dispatch_pipeline` before any `Stage` is touched. A further action is
//! required — here `.values().fold_task(...)`.
//!
//! Expected output: {"total": 7} (sum of all word counts across
//! "hello world" / "hello rust" / "world of rust")

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

    let total = ctx
        .parallelize_typed(lines, 2)
        .flat_map_task(Tokenize) // staged pipeline precedes the shuffle
        .reduce_by_key_task(AddI32) // shuffle: the resulting ShuffledRdd's staged is None
        .values() // OneToOne hop, transparent to the Stage walk
        .fold_task(0, AddI32)?; // terminal action: dispatch_pipeline(ShuffledRdd, ...)

    println!("{}", serde_json::json!({ "total": total }));
    Ok(())
}
