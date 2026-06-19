/// Monte Carlo pi estimation — benchmark counterpart to `examples/pi`.
///
/// Same algorithm as the canonical `pi` example (xorshift PRNG, count points
/// inside the unit circle), scaled up and instrumented with wall-clock timing
/// so it can be compared against an equivalent PySpark job. See
/// `benchmarks/spark/pi_benchmark.py` for the Spark side and
/// `benchmarks/README.md` for methodology.
///
/// ```bash
/// cargo run --release -p bench_pi
/// ```
use std::time::Instant;

use atomic_compute::app::AtomicApp;
use atomic_compute::task;

const SAMPLES_PER_PARTITION: u64 = 20_000_000;
const NUM_PARTITIONS: usize = 12;

#[task]
fn count_hits(seed: u64) -> u64 {
    let mut state = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let mut hits: u64 = 0;
    for _ in 0..SAMPLES_PER_PARTITION {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let x = (state & 0xFFFF) as f64 / 65535.0;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let y = (state & 0xFFFF) as f64 / 65535.0;
        if x * x + y * y <= 1.0 {
            hits += 1;
        }
    }
    hits
}

#[task]
fn sum_hits(a: u64, b: u64) -> u64 {
    a + b
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let setup_start = Instant::now();
    let app = AtomicApp::build().await?;
    let ctx = app.driver_context()?;
    let setup_elapsed = setup_start.elapsed();

    let seeds: Vec<u64> = (0..NUM_PARTITIONS as u64).collect();

    let job_start = Instant::now();
    let total_hits = ctx
        .parallelize_typed(seeds, NUM_PARTITIONS)
        .map_task(CountHits)
        .fold_task(0u64, SumHits)?;
    let job_elapsed = job_start.elapsed();

    let total_samples = SAMPLES_PER_PARTITION * NUM_PARTITIONS as u64;
    let pi_estimate = 4.0 * total_hits as f64 / total_samples as f64;

    println!(
        "RESULT engine=atomic workload=pi samples={} partitions={} pi_estimate={:.6} \
         setup_ms={} job_ms={} total_ms={}",
        total_samples,
        NUM_PARTITIONS,
        pi_estimate,
        setup_elapsed.as_millis(),
        job_elapsed.as_millis(),
        (setup_elapsed + job_elapsed).as_millis(),
    );

    Ok(())
}
