/// Monte Carlo π estimation — numeric atomic example.
///
/// Demonstrates: parallelize, map, fold (aggregation across partitions).
///
/// Each partition independently samples random points in the unit square and
/// counts how many fall inside the unit circle. The driver aggregates the
/// per-partition counts to estimate π ≈ 4 * (hits / total_samples).
///
/// Run locally:
///   cargo run --manifest-path examples/pi/Cargo.toml
///
/// Run distributed (after `atomic deploy`):
///   ATOMIC_DEPLOYMENT_MODE=distributed ATOMIC_IS_DRIVER=true \
///   ATOMIC_SLAVES=host1@10.0.0.1 \
///   cargo run --manifest-path examples/pi/Cargo.toml
use atomic_compute::context::Context;

const SAMPLES_PER_PARTITION: usize = 500_000;
const NUM_PARTITIONS: usize = 4;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::local()?;

    // Each element is a partition index; the map function does the sampling.
    let partition_ids: Vec<u64> = (0..NUM_PARTITIONS as u64).collect();

    let total_hits: u64 = ctx
        .parallelize_typed(partition_ids, NUM_PARTITIONS)
        .map(move |seed| {
            // Simple xorshift PRNG — no external deps needed.
            let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
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
        })
        .fold(0u64, |acc, hits| acc + hits)?;

    let total_samples = (SAMPLES_PER_PARTITION * NUM_PARTITIONS) as f64;
    let pi_estimate = 4.0 * total_hits as f64 / total_samples;

    println!("Samples : {}", total_samples as u64);
    println!("Hits    : {}", total_hits);
    println!("π ≈     : {:.6}", pi_estimate);
    println!("Error   : {:.6}", (pi_estimate - std::f64::consts::PI).abs());

    Ok(())
}
