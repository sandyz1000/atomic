"""Distributed Pi benchmark — atomic-py with real separate worker processes.

Each partition runs entirely on a worker: the worker generates SAMPLES_PER_PARTITION
random samples, counts hits, and returns a single integer. Only N integers cross the
wire (one per partition), so transfer overhead is negligible.

Compares the same workload run by PySpark local[N], which uses N OS-process Python
workers communicating over local sockets — same parallelism model, different engine.

Requirements
------------
- atomic-worker binary built: cargo build --release -p atomic-worker
- atomic-py installed in this venv: maturin develop --release (in crates/atomic-py)
- cloudpickle: pip install cloudpickle

Usage
-----
Run via the wrapper that starts/stops workers:

    benchmarks/.venv/bin/python benchmarks/atomic_py/dist_benchmark_runner.py pi

Or manually:

    # Terminal 1-4 (one per worker)
    ATOMIC_LOCAL_IP=127.0.0.1 ./target/release/atomic-worker --port 10001
    ...

    # Terminal 5 (driver)
    ATOMIC_DEPLOYMENT_MODE=distributed ATOMIC_LOCAL_IP=127.0.0.1 \\
        benchmarks/.venv/bin/python benchmarks/atomic_py/dist_pi_benchmark.py
"""

import random
import time

import atomic_compute as ac

NUM_PARTITIONS = 4        # one per worker; each handles SAMPLES_PER_PARTITION samples
SAMPLES_PER_PARTITION = 60_000_000  # same total (240M) as local benchmark


def count_hits_in_partition(_partition_idx: int) -> int:
    """Runs entirely on the worker — zero large data returned to driver."""
    hits = 0
    for _ in range(SAMPLES_PER_PARTITION):
        x = random.random()
        y = random.random()
        if x * x + y * y <= 1.0:
            hits += 1
    return hits


def main() -> None:
    setup_start = time.perf_counter()
    ctx = ac.Context(default_parallelism=NUM_PARTITIONS)
    setup_elapsed = time.perf_counter() - setup_start

    job_start = time.perf_counter()
    total_hits = (
        ctx.parallelize(list(range(NUM_PARTITIONS)), num_partitions=NUM_PARTITIONS)
        .map(count_hits_in_partition)          # dispatched to workers in dist mode
        .reduce(lambda a, b: a + b)            # per-partition partial results summed
    )
    total_samples = NUM_PARTITIONS * SAMPLES_PER_PARTITION
    pi_estimate = 4.0 * total_hits / total_samples
    job_elapsed = time.perf_counter() - job_start

    print(
        "RESULT engine=atomic-py-dist workload=pi samples={} partitions={} "
        "pi_estimate={:.6f} setup_ms={:.0f} job_ms={:.0f} total_ms={:.0f}".format(
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
