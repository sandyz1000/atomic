/// Shuffle-based join — benchmark counterpart to `benchmarks/spark/join_benchmark.py`.
///
/// Builds two pair RDDs over a shared, narrower key space (so each key has
/// several matches on both sides) and joins them via `TypedRdd::join`, which
/// routes through two shuffle stages (`cogroup_shuffle`) rather than a driver
/// collect.
///
/// ```bash
/// cargo run --release -p bench_join
/// ```
use std::time::Instant;

use atomic_compute::app::AtomicApp;

atomic_compute::register_shuffle_map!(i64, i64);
atomic_compute::register_shuffle_map!(i64, Vec<i64>);

const NUM_LEFT: usize = 1_000_000;
const NUM_RIGHT: usize = 1_000_000;
const KEY_SPACE: i64 = 200_000;
const NUM_PARTITIONS: usize = 12;

fn generate_pairs(n: usize, seed: u64) -> Vec<(i64, i64)> {
    let mut state = seed;
    (0..n)
        .map(|i| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let key = (state % KEY_SPACE as u64) as i64;
            (key, i as i64)
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let setup_start = Instant::now();
    let app = AtomicApp::build().await?;
    let ctx = app.driver_context()?;
    let left = generate_pairs(NUM_LEFT, 0x2545F4914F6CDD1D);
    let right = generate_pairs(NUM_RIGHT, 0x9E3779B97F4A7C15);
    let setup_elapsed = setup_start.elapsed();

    let job_start = Instant::now();
    let left_rdd = ctx.parallelize_typed(left, NUM_PARTITIONS);
    let right_rdd = ctx.parallelize_typed(right, NUM_PARTITIONS);
    let joined: Vec<(i64, (i64, i64))> = left_rdd.join(right_rdd).collect()?;
    let job_elapsed = job_start.elapsed();

    println!(
        "RESULT engine=atomic workload=join left={} right={} key_space={} partitions={} \
         joined_rows={} setup_ms={} job_ms={} total_ms={}",
        NUM_LEFT,
        NUM_RIGHT,
        KEY_SPACE,
        NUM_PARTITIONS,
        joined.len(),
        setup_elapsed.as_millis(),
        job_elapsed.as_millis(),
        (setup_elapsed + job_elapsed).as_millis(),
    );

    Ok(())
}
