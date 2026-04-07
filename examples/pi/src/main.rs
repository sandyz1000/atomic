/// Monte Carlo π estimation — numeric Atomic example.
///
/// Each partition runs `SAMPLES_PER_PARTITION` point samples using a
/// deterministic xorshift PRNG seeded by the partition index. Workers count
/// how many points fall inside the unit circle; the driver sums the counts
/// and estimates π ≈ 4 * (hits / total_samples).
///
/// # Running locally
///
/// ```bash
/// cargo run -p pi
/// ```
///
/// # Running distributed (two terminals)
///
/// Terminal 1 — start a worker:
/// ```bash
/// cargo build -p pi --release
/// ./target/release/pi --worker --port 10001
/// ```
///
/// Terminal 2 — run the driver:
/// ```bash
/// ./target/release/pi --workers 127.0.0.1:10001
/// ```
use atomic_compute::app::AtomicApp;
use atomic_compute::task;

const SAMPLES_PER_PARTITION: u64 = 500_000;
const NUM_PARTITIONS: usize = 4;

/// Count how many random points (seeded by `seed`) fall inside the unit circle.
///
/// Uses a simple xorshift PRNG — no external randomness needed.
/// Registered in `TASK_REGISTRY`; executes on workers in distributed mode.
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

/// Sum two u64 hit counts (used by fold_task).
#[task]
fn sum_hits(a: u64, b: u64) -> u64 {
    a + b
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // AtomicApp::build() reads CLI flags:
    //   --worker [--port N]    → starts worker loop, never returns
    //   --workers addr:port,...→ distributed driver
    //   (no flags)             → local driver
    let app = AtomicApp::build().await?;
    let ctx = app.driver_context()?;

    // Each element is a partition seed; workers run count_hits on their partition.
    let seeds: Vec<u64> = (0..NUM_PARTITIONS as u64).collect();

    // Pipeline:
    //   map_task(CountHits)   — each seed → hit count for that partition  (remote)
    //   fold_task(0, SumHits) — reduce per-partition counts to a total    (remote + driver combine)
    let total_hits = ctx
        .parallelize_typed(seeds, NUM_PARTITIONS)
        .map_task(CountHits)
        .fold_task(0u64, SumHits)?;

    let total_samples = SAMPLES_PER_PARTITION * NUM_PARTITIONS as u64;
    let pi_estimate = 4.0 * total_hits as f64 / total_samples as f64;

    println!("Samples : {}", total_samples);
    println!("Hits    : {}", total_hits);
    println!("π ≈     : {:.6}", pi_estimate);
    println!("Error   : {:.6}", (pi_estimate - std::f64::consts::PI).abs());

    Ok(())
}
