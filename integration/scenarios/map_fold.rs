//! Scenario: baseline distributed map + fold.
//!
//! Expected output: {"doubled":[2,4,6,8],"sum":10}

use atomic_compute::context::Context;
use atomic_compute::task;
use std::error::Error;
use std::sync::Arc;

#[task]
fn double(x: i32) -> i32 {
    x * 2
}

#[task]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

pub fn run_driver(ctx: &Arc<Context>) -> Result<(), Box<dyn Error>> {
    let input = vec![1i32, 2, 3, 4];

    let doubled = ctx
        .parallelize_typed(input.clone(), 2)
        .map_task(Double)
        .collect()?;

    let sum = ctx.parallelize_typed(input, 2).fold_task(0i32, Add)?;

    println!("{}", serde_json::json!({ "doubled": doubled, "sum": sum }));
    Ok(())
}
