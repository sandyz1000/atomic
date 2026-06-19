"""Monte Carlo pi estimation — atomic-py counterpart to benchmarks/spark/pi_benchmark.py.

Same algorithm, same Python closure (no Rust #[task] involved), so this
isolates engine overhead (scheduling, serialization) from raw language speed.
Requires atomic-py built into this venv:

    cd crates/atomic-py && ../../benchmarks/.venv/bin/maturin develop --release

Run with:

    benchmarks/.venv/bin/python benchmarks/atomic_py/pi_benchmark.py
"""

import time

import atomic_compute as ac

SAMPLES_PER_PARTITION = 20_000_000
NUM_PARTITIONS = 12


def count_hits(seed: int) -> int:
    mask = (1 << 64) - 1
    state = (seed * 6364136223846793005 + 1442695040888963407) & mask
    hits = 0
    for _ in range(SAMPLES_PER_PARTITION):
        state ^= (state << 13) & mask
        state ^= state >> 7
        state ^= (state << 17) & mask
        x = (state & 0xFFFF) / 65535.0
        state ^= (state << 13) & mask
        state ^= state >> 7
        state ^= (state << 17) & mask
        y = (state & 0xFFFF) / 65535.0
        if x * x + y * y <= 1.0:
            hits += 1
    return hits


def main() -> None:
    setup_start = time.perf_counter()
    ctx = ac.Context(default_parallelism=NUM_PARTITIONS)
    setup_elapsed = time.perf_counter() - setup_start

    job_start = time.perf_counter()
    seeds = list(range(NUM_PARTITIONS))
    total_hits = (
        ctx.parallelize(seeds, num_partitions=NUM_PARTITIONS)
        .map(count_hits)
        .reduce(lambda a, b: a + b)
    )
    job_elapsed = time.perf_counter() - job_start

    total_samples = SAMPLES_PER_PARTITION * NUM_PARTITIONS
    pi_estimate = 4.0 * total_hits / total_samples

    print(
        "RESULT engine=atomic-py workload=pi samples={} partitions={} pi_estimate={:.6f} "
        "setup_ms={:.0f} job_ms={:.0f} total_ms={:.0f}".format(
            total_samples,
            NUM_PARTITIONS,
            pi_estimate,
            setup_elapsed * 1000,
            job_elapsed * 1000,
            (setup_elapsed + job_elapsed) * 1000,
        )
    )


if __name__ == "__main__":
    main()
