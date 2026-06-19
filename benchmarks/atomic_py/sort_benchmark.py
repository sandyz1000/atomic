"""Distributed sort via sort_by_key — atomic-py counterpart to
benchmarks/spark/sort_benchmark.py.

Same xorshift PRNG and key space as the Spark script, so this isolates engine
overhead from raw language speed. Requires atomic-py built into this venv:

    cd crates/atomic-py && ../../benchmarks/.venv/bin/maturin develop --release

Run with:

    benchmarks/.venv/bin/python benchmarks/atomic_py/sort_benchmark.py
"""

import time

import atomic_compute as ac

NUM_RECORDS = 2_000_000
NUM_PARTITIONS = 12
MASK = (1 << 64) - 1


def generate_pairs() -> list[tuple[int, int]]:
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
    ctx = ac.Context(default_parallelism=NUM_PARTITIONS)
    pairs = generate_pairs()
    setup_elapsed = time.perf_counter() - setup_start

    job_start = time.perf_counter()
    result = (
        ctx.parallelize(pairs, num_partitions=NUM_PARTITIONS)
        .sort_by_key(ascending=True)
        .collect()
    )
    job_elapsed = time.perf_counter() - job_start

    is_sorted = all(result[i][0] <= result[i + 1][0] for i in range(len(result) - 1))
    assert is_sorted, "result is not sorted"

    print(
        "RESULT engine=atomic-py workload=sort records={} partitions={} sorted_ok={} "
        "setup_ms={:.0f} job_ms={:.0f} total_ms={:.0f}".format(
            len(result),
            NUM_PARTITIONS,
            is_sorted,
            setup_elapsed * 1000,
            job_elapsed * 1000,
            (setup_elapsed + job_elapsed) * 1000,
        )
    )


if __name__ == "__main__":
    main()
