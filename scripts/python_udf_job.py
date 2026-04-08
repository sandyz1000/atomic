"""
Atomic Python UDF demo — local mode.

Run with:
    maturin develop -m crates/atomic-py/Cargo.toml
    python examples/python_udf/job.py

Expected output:
    Input:              [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    Doubled + filtered: [12, 14, 16, 18, 20, 22, 24]
    Sum of doubled:     156
"""

import atomic

ctx = atomic.Context()

data = list(range(1, 13))
print("Input:             ", data)

rdd = ctx.parallelize(data, num_partitions=4)

result = rdd.map(lambda x: x * 2).filter(lambda x: x > 10).collect()
print("Doubled + filtered:", result)

total = ctx.parallelize(data, num_partitions=4).map(lambda x: x * 2).fold(0, lambda a, b: a + b)
print("Sum of doubled:    ", total)
