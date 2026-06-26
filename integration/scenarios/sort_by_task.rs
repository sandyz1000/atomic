//! Scenario: distributed `sort_by_task`.
//!
//! Validates that a non-pair RDD sorted via `sort_by_task` is globally ordered
//! across worker-produced partitions with no client-side re-sort: range
//! partitioning + per-partition sort-shuffle happens entirely on the workers.
//!
//! Expected output: {"partitions":[[0,1,2],[3,4,5,6],[7,8,9]]} (illustrative)

use atomic_compute::context::Context;
use atomic_compute::task;
use std::error::Error;
use std::sync::Arc;

atomic_compute::register_sort_shuffle_map!(i32, i32);

#[task]
fn self_key(x: i32) -> (i32, i32) {
    (x, x)
}

pub fn run_driver(ctx: &Arc<Context>) -> Result<(), Box<dyn Error>> {
    let data: Vec<i32> = vec![5, 3, 8, 1, 9, 2, 7, 4, 6, 0];

    let partitions = ctx
        .parallelize_typed(data, 3)
        .sort_by_task(SelfKey, true)
        .collect_partitions()?;

    println!("{}", serde_json::json!({ "partitions": partitions }));
    Ok(())
}
