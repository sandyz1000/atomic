"""Monte Carlo pi estimation — PySpark counterpart to examples/bench_pi.

Same algorithm as the Rust version: a per-partition xorshift PRNG counts how
many sampled points land inside the unit circle. Run with:

    benchmarks/.venv/bin/python benchmarks/spark/pi_benchmark.py
"""

import time

from pyspark.sql import SparkSession

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
    spark = (
        SparkSession.builder.master(f"local[{NUM_PARTITIONS}]")
        .appName("bench_pi")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    setup_elapsed = time.perf_counter() - setup_start

    job_start = time.perf_counter()
    seeds = list(range(NUM_PARTITIONS))
    total_hits = (
        sc.parallelize(seeds, NUM_PARTITIONS).map(count_hits).reduce(lambda a, b: a + b)
    )
    job_elapsed = time.perf_counter() - job_start

    total_samples = SAMPLES_PER_PARTITION * NUM_PARTITIONS
    pi_estimate = 4.0 * total_hits / total_samples

    print(
        "RESULT engine=spark workload=pi samples={} partitions={} pi_estimate={:.6f} "
        "setup_ms={:.0f} job_ms={:.0f} total_ms={:.0f}".format(
            total_samples,
            NUM_PARTITIONS,
            pi_estimate,
            setup_elapsed * 1000,
            job_elapsed * 1000,
            (setup_elapsed + job_elapsed) * 1000,
        )
    )

    spark.stop()


if __name__ == "__main__":
    main()
