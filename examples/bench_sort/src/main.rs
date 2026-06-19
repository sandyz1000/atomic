/// Distributed sort via `sort_by_key` (sample-based range partitioning) —
/// benchmark counterpart to `benchmarks/spark/sort_benchmark.py`'s `sortByKey`.
///
/// Generates pseudo-random `(key, value)` pairs with a deterministic xorshift
/// PRNG, sorts by key, and verifies the result is globally ordered.
///
/// ```bash
/// cargo run --release -p bench_sort
/// ```
use std::time::Instant;

use atomic_compute::app::AtomicApp;

atomic_compute::register_shuffle_map!(i64, i64);

const NUM_RECORDS: usize = 2_000_000;
const NUM_PARTITIONS: usize = 12;

/// Deterministic xorshift PRNG — same family used by `bench_pi`, so corpus
/// generation cost is comparable across benchmarks and not seeded by the
/// system RNG (keeps runs reproducible).
fn generate_pairs() -> Vec<(i64, i64)> {
    let mut state: u64 = 0x2545F4914F6CDD1D;
    (0..NUM_RECORDS)
        .map(|i| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let key = (state % 10_000_000) as i64;
            (key, i as i64)
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let setup_start = Instant::now();
    let app = AtomicApp::build().await?;
    let ctx = app.driver_context()?;
    let pairs = generate_pairs();
    let setup_elapsed = setup_start.elapsed();

    let job_start = Instant::now();
    let sorted: Vec<(i64, i64)> = ctx
        .parallelize_typed(pairs, NUM_PARTITIONS)
        .sort_by_key(true)
        .collect()?;
    let job_elapsed = job_start.elapsed();

    let is_sorted = sorted.windows(2).all(|w| w[0].0 <= w[1].0);
    assert!(is_sorted, "result is not sorted");

    println!(
        "RESULT engine=atomic workload=sort records={} partitions={} sorted_ok={} \
         setup_ms={} job_ms={} total_ms={}",
        sorted.len(),
        NUM_PARTITIONS,
        is_sorted,
        setup_elapsed.as_millis(),
        job_elapsed.as_millis(),
        (setup_elapsed + job_elapsed).as_millis(),
    );

    Ok(())
}
