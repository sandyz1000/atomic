"""Shuffle join — PySpark counterpart to examples/bench_join.

Same key space and generator as the Rust version. Run with:

    benchmarks/.venv/bin/python benchmarks/spark/join_benchmark.py
"""

import time

from pyspark.sql import SparkSession

NUM_LEFT = 1_000_000
NUM_RIGHT = 1_000_000
KEY_SPACE = 200_000
NUM_PARTITIONS = 12
MASK = (1 << 64) - 1


def generate_pairs(n: int, seed: int):
    state = seed
    pairs = []
    for i in range(n):
        state ^= (state << 13) & MASK
        state ^= state >> 7
        state ^= (state << 17) & MASK
        key = state % KEY_SPACE
        pairs.append((key, i))
    return pairs


def main() -> None:
    setup_start = time.perf_counter()
    spark = (
        SparkSession.builder.master(f"local[{NUM_PARTITIONS}]")
        .appName("bench_join")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    left = generate_pairs(NUM_LEFT, 0x2545F4914F6CDD1D)
    right = generate_pairs(NUM_RIGHT, 0x9E3779B97F4A7C15)
    setup_elapsed = time.perf_counter() - setup_start

    job_start = time.perf_counter()
    left_rdd = sc.parallelize(left, NUM_PARTITIONS)
    right_rdd = sc.parallelize(right, NUM_PARTITIONS)
    joined = left_rdd.join(right_rdd).collect()
    joined_count = len(joined)
    job_elapsed = time.perf_counter() - job_start

    print(
        "RESULT engine=spark workload=join left={} right={} key_space={} partitions={} "
        "joined_rows={} setup_ms={:.0f} job_ms={:.0f} total_ms={:.0f}".format(
            NUM_LEFT,
            NUM_RIGHT,
            KEY_SPACE,
            NUM_PARTITIONS,
            joined_count,
            setup_elapsed * 1000,
            job_elapsed * 1000,
            (setup_elapsed + job_elapsed) * 1000,
        )
    )

    spark.stop()


if __name__ == "__main__":
    main()
