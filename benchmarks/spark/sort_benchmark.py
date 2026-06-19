"""Distributed sort via sortByKey — PySpark counterpart to examples/bench_sort.

Same xorshift PRNG and key space as the Rust version. Run with:

    benchmarks/.venv/bin/python benchmarks/spark/sort_benchmark.py
"""

import time

from pyspark.sql import SparkSession

NUM_RECORDS = 2_000_000
NUM_PARTITIONS = 12
MASK = (1 << 64) - 1


def generate_pairs():
    state = 0x2545F4914F6CDD1D
    pairs = []
    for i in range(NUM_RECORDS):
        state ^= (state << 13) & MASK
        state ^= state >> 7
        state ^= (state << 17) & MASK
        key = state % 10_000_000
        pairs.append((key, i))
    return pairs


def main() -> None:
    setup_start = time.perf_counter()
    spark = (
        SparkSession.builder.master(f"local[{NUM_PARTITIONS}]")
        .appName("bench_sort")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    pairs = generate_pairs()
    setup_elapsed = time.perf_counter() - setup_start

    job_start = time.perf_counter()
    sorted_rdd = sc.parallelize(pairs, NUM_PARTITIONS).sortByKey(ascending=True)
    result = sorted_rdd.collect()
    job_elapsed = time.perf_counter() - job_start

    is_sorted = all(result[i][0] <= result[i + 1][0] for i in range(len(result) - 1))
    assert is_sorted, "result is not sorted"

    print(
        "RESULT engine=spark workload=sort records={} partitions={} sorted_ok={} "
        "setup_ms={:.0f} job_ms={:.0f} total_ms={:.0f}".format(
            len(result),
            NUM_PARTITIONS,
            is_sorted,
            setup_elapsed * 1000,
            job_elapsed * 1000,
            (setup_elapsed + job_elapsed) * 1000,
        )
    )

    spark.stop()


if __name__ == "__main__":
    main()
