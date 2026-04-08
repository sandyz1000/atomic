"""
Local range-sum demo using the atomic Python API.

Partitions the range [1, 101) across 4 partitions and folds the
elements into a single sum on the driver.

Prerequisites:
  cd crates/atomic-py && maturin develop --release && cd ../..

Run:
  python demo/python/local_sum_job.py
"""

import atomic

ctx = atomic.Context(default_parallelism=4)

total = (
    ctx.range(1, 101, num_partitions=4)
       .fold(0, lambda acc, x: acc + x)
)

print(f"Sum of 1..100 = {total}")   # 5050
