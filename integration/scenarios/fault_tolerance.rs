//! Scenario: fault tolerance with a dead worker.
//!
//! Validates:
//! - Dispatching a job with 2 workers listed where one is unreachable
//! - Driver falls back to the surviving worker
//! - Result is still correct despite the missing worker
//!
//! Expected output: {"sum":55,"doubled":[2,4,...,20]}

use atomic_compute::context::Context;
use atomic_compute::task;
use std::error::Error;
use std::sync::Arc;

#[task]
fn double_i32(x: i32) -> i32 {
    x * 2
}

#[task]
fn add_i32(a: i32, b: i32) -> i32 {
    a + b
}

pub fn run_driver(ctx: &Arc<Context>) -> Result<(), Box<dyn Error>> {
    let data: Vec<i32> = (1..=10).collect();

    // Double each element — spread across workers.
    let doubled = ctx
        .parallelize_typed(data.clone(), 4)
        .map_task(DoubleI32)
        .collect()?;

    // Sum all — uses fold_task which also dispatches to workers.
    let sum = ctx.parallelize_typed(data, 4).fold_task(0i32, AddI32)?;

    println!("{}", serde_json::json!({ "sum": sum, "doubled": doubled }));
    Ok(())
}
