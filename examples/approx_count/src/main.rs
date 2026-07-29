/// Approximate counting via `Context::run_approximate_job`.
///
/// Each partition's count merges into a `CountEvaluator` as results arrive. The call
/// returns a `PartialResult` after either every partition has reported or the timeout
/// elapses, whichever comes first — `PartialResult::is_final` tells you which happened.
/// A short timeout demonstrates the "not all partitions finished yet" case; a generous
/// one lets every partition report and returns the exact count.
///
/// **Local-only example**: `run_approximate_job` needs the local scheduler's polling
/// event loop — the distributed dispatch path returns `UnsupportedOperation` for it.
/// Use `Context::local()`.
///
/// Run:
///   cargo run -p approx_count
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_data::partial::CountEvaluator;
use atomic_data::task_context::TaskContext;

const NUM_PARTITIONS: usize = 8;

/// Per-partition work with an artificial delay, so a short timeout reliably
/// catches the job mid-flight regardless of machine speed.
fn count_partition(args: (TaskContext, Box<dyn Iterator<Item = i32>>)) -> usize {
    std::thread::sleep(Duration::from_millis(50));
    args.1.count()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::local()?;

    let data: Vec<i32> = (0..2_000).collect();
    let rdd = ctx.parallelize(data.clone(), NUM_PARTITIONS);

    // Timeout far shorter than a single partition's 50ms delay: the job can't have
    // finished, so this is a partial estimate.
    let quick = ctx.run_approximate_job(
        count_partition,
        rdd.clone(),
        CountEvaluator::new(NUM_PARTITIONS, 0.95),
        Duration::from_millis(10),
    )?;
    println!(
        "quick (10ms timeout): final={} interval=[{:.1}, {:.1}]",
        quick.is_final, quick.initial_value.low, quick.initial_value.high
    );
    assert!(
        !quick.is_final,
        "expected the 10ms timeout to cut the job off before any 50ms partition finished"
    );

    // Generous timeout: every partition finishes and the evaluator reports the exact count.
    let full = ctx.run_approximate_job(
        count_partition,
        rdd,
        CountEvaluator::new(NUM_PARTITIONS, 0.95),
        Duration::from_secs(5),
    )?;
    println!(
        "full (5s timeout): final={} count={}",
        full.is_final, full.initial_value.mean
    );
    assert!(
        full.is_final,
        "expected the 5s timeout to let every partition report"
    );
    assert_eq!(
        full.initial_value.mean,
        data.len() as f64,
        "final count should be exact, not just estimated"
    );

    println!("approx_count verified.");
    Ok(())
}
