"""
Distributed word count demo using the atomic Python API.

Python lambdas are serialized via ``pickle`` and executed on workers via their
embedded PyO3 runtime. The ``atomic-worker`` pre-built binary handles all Python
UDF execution — no binary deployment of your code is required.

Setup:
  1. Build and install the Python module (driver side):
       cd crates/atomic-py && maturin develop --release && cd ../..

  2. Build the worker binary:
       cargo build --release -p atomic-worker

  3. Start a worker in a separate terminal:
       RUST_LOG=info ./target/release/atomic-worker --worker --port 10001

  4. Run this driver:
       VEGA_DEPLOYMENT_MODE=distributed VEGA_LOCAL_IP=127.0.0.1 \\
         python demo/python/distributed_word_count.py
"""

import atomic

ctx = atomic.Context()

lines = [
    "the quick brown fox jumps over the lazy dog",
    "the fox and the dog are friends",
    "quick brown foxes jump over lazy dogs",
    "atomic distributed compute engine written in rust",
]

rdd = ctx.parallelize(lines, num_partitions=2)

word_counts = (
    rdd.flat_map(lambda line: line.split())
       .map(lambda word: (word, 1))
       .reduce_by_key(lambda a, b: a + b)
       .collect()
)

# Sort by count descending and print top 10
for word, count in sorted(word_counts, key=lambda x: -x[1])[:10]:
    print(f"{word:<20} {count}")
