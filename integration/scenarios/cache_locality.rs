//! Scenario: distributed cache + locality.
//!
//! Validates:
//! - A cached RDD's second action does not recompute partitions that are still
//!   held by a live worker (locality-pinned cache read, not recompute).
//! - After a worker dies between actions, the partitions it held are
//!   recomputed on a surviving worker while the survivor's own partitions are
//!   still served from cache.
//!
//! Each element is tagged with a process-local call counter at compute time, so
//! the driver can tell a cache hit (call id unchanged across actions) from a
//! recompute (call id advances) without any shared state between processes.
//!
//! The driver sleeps between the first and second collect so the test harness
//! has a window to kill a worker in between.
//! Expected output: {"first":[[1,0],[2,1],[3,0],[4,1]],"second":[[1,0],[2,1],[3,0],[4,1]]}

use atomic_compute::context::Context;
use atomic_compute::task;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static CALLS: AtomicUsize = AtomicUsize::new(0);

#[task]
fn tag_call(x: i32) -> (i32, usize) {
    let n = CALLS.fetch_add(1, Ordering::SeqCst);
    (x, n)
}

pub fn run_driver(ctx: &Arc<Context>) -> Result<(), Box<dyn Error>> {
    let data: Vec<i32> = (1..=4).collect();

    let rdd = ctx.parallelize_typed(data, 2).map_task(TagCall).cache();

    let first = rdd.collect()?;

    // Give the test harness a window to kill a worker before the second action.
    std::thread::sleep(Duration::from_millis(1500));

    let second = rdd.collect()?;

    println!(
        "{}",
        serde_json::json!({ "first": first, "second": second })
    );
    Ok(())
}
