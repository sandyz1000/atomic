"""Shuffle join — atomic-py counterpart to benchmarks/spark/join_benchmark.py.

Same key space and xorshift generator as the Spark script. Requires atomic-py
built into this venv:

    cd crates/atomic-py && ../../benchmarks/.venv/bin/maturin develop --release

Run with:

    benchmarks/.venv/bin/python benchmarks/atomic_py/join_benchmark.py
"""

import time

import atomic_compute as ac

NUM_LEFT = 1_000_000
NUM_RIGHT = 1_000_000
KEY_SPACE = 200_000
NUM_PARTITIONS = 12
MASK = (1 << 64) - 1


def generate_pairs(n: int, seed: int) -> list[tuple[int, int]]:
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
    ctx = ac.Context(default_parallelism=NUM_PARTITIONS)
    left = generate_pairs(NUM_LEFT, 0x2545F4914F6CDD1D)
    right = generate_pairs(NUM_RIGHT, 0x9E3779B97F4A7C15)
    setup_elapsed = time.perf_counter() - setup_start

    job_start = time.perf_counter()
    left_rdd = ctx.parallelize(left, num_partitions=NUM_PARTITIONS)
    right_rdd = ctx.parallelize(right, num_partitions=NUM_PARTITIONS)
    joined = left_rdd.join(right_rdd).collect()
    joined_count = len(joined)
    job_elapsed = time.perf_counter() - job_start

    print(
        "RESULT engine=atomic-py workload=join left={} right={} key_space={} partitions={} "
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


if __name__ == "__main__":
    main()
