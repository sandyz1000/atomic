"""Word count via reduceByKey — PySpark counterpart to examples/bench_wordcount.

Generates the same synthetic corpus layout as the Rust version (cyclic
vocabulary, fixed words-per-line) and runs flatMap -> map -> reduceByKey.
Run with:

    benchmarks/.venv/bin/python benchmarks/spark/wordcount_benchmark.py
"""

import time

from pyspark.sql import SparkSession

VOCAB_SIZE = 5_000
WORDS_PER_LINE = 20
TOTAL_WORDS = 3_000_000
NUM_PARTITIONS = 12


def generate_corpus() -> list[str]:
    num_lines = TOTAL_WORDS // WORDS_PER_LINE
    lines = []
    for line_idx in range(num_lines):
        base = line_idx * WORDS_PER_LINE
        words = [f"word{(base + w) % VOCAB_SIZE}" for w in range(WORDS_PER_LINE)]
        lines.append(" ".join(words))
    return lines


def main() -> None:
    setup_start = time.perf_counter()
    spark = (
        SparkSession.builder.master(f"local[{NUM_PARTITIONS}]")
        .appName("bench_wordcount")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    lines = generate_corpus()
    num_lines = len(lines)
    setup_elapsed = time.perf_counter() - setup_start

    job_start = time.perf_counter()
    counts = (
        sc.parallelize(lines, NUM_PARTITIONS)
        .flatMap(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )
    job_elapsed = time.perf_counter() - job_start

    distinct_keys = len(counts)
    total_count = sum(c for _, c in counts)

    print(
        "RESULT engine=spark workload=wordcount total_words={} lines={} vocab={} "
        "partitions={} distinct_keys={} total_count={} setup_ms={:.0f} job_ms={:.0f} "
        "total_ms={:.0f}".format(
            TOTAL_WORDS,
            num_lines,
            VOCAB_SIZE,
            NUM_PARTITIONS,
            distinct_keys,
            total_count,
            setup_elapsed * 1000,
            job_elapsed * 1000,
            (setup_elapsed + job_elapsed) * 1000,
        )
    )

    spark.stop()


if __name__ == "__main__":
    main()
