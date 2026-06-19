"""Distributed WordCount benchmark — atomic-py with real separate worker processes.

The distributed execution model in atomic-py (current implementation):

  flat_map(tokenize) + map(pair_one) + reduce_by_key(sum) dispatch as ONE pipeline.
    Each worker tokenizes its partition of lines, emits (word, 1) pairs, and does
    a local partial reduce — returning only compact (word, partial_count) pairs.
    The driver merges the per-worker partial dicts with the same reduce function.

There is no intermediate collect()/re-parallelize() between stages: reduce_by_key
appends to the still-staged pipeline from flat_map/map, so the whole chain
dispatches to workers in a single round trip. The network cost is bounded by
(distinct_words × num_workers), not total_words — for 3M words / 5,000 distinct
words across 4 workers that's ~20K pairs back to the driver, not 3M.

Requirements
------------
Same as dist_pi_benchmark.py — workers must be running before the driver starts.

Usage
-----
    benchmarks/.venv/bin/python benchmarks/atomic_py/dist_benchmark_runner.py wordcount
"""

import time

import atomic_compute as ac

VOCAB_SIZE = 5_000
WORDS_PER_LINE = 20
TOTAL_WORDS = 3_000_000
NUM_PARTITIONS = 4


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
    ctx = ac.Context(default_parallelism=NUM_PARTITIONS)
    lines = generate_corpus()
    setup_elapsed = time.perf_counter() - setup_start

    job_start = time.perf_counter()

    # One staged pipeline: tokenize + pair + partial-reduce all dispatch to
    # workers together; the driver merges the compact per-worker partial counts.
    counts = (
        ctx.parallelize(lines, num_partitions=NUM_PARTITIONS)
        .flat_map(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduce_by_key(lambda a, b: a + b)
        .collect()
    )

    job_elapsed = time.perf_counter() - job_start

    distinct_keys = len(counts)
    total_count = sum(c for _, c in counts)

    print(
        "RESULT engine=atomic-py-dist workload=wordcount total_words={} vocab={} "
        "partitions={} distinct_keys={} total_count={} setup_ms={:.0f} "
        "job_ms={:.0f} total_ms={:.0f}".format(
            TOTAL_WORDS,
            VOCAB_SIZE,
            NUM_PARTITIONS,
            distinct_keys,
            total_count,
            setup_elapsed * 1000,
            job_elapsed * 1000,
            (setup_elapsed + job_elapsed) * 1000,
        )
    )


if __name__ == "__main__":
    main()
