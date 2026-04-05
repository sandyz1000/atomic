"""
Local word count job using the atomic Python API.

Prerequisites:
  cd crates/atomic-python && maturin develop --release && cd ../..

Run:
  python demo/python/local_word_count.py
"""

import atomic

ctx = atomic.Context(default_parallelism=4)

# Input: lines of text spread across the dataset
lines = ctx.parallelize([
    "hello world hello",
    "world foo bar hello",
    "atomic is fast and atomic is small",
    "fast systems need fast tools",
])

# Classic Spark word count:
#   split lines → emit (word, 1) pairs → aggregate by key
word_counts = (
    lines
    .flat_map(lambda line: line.split())        # ["hello", "world", "hello", ...]
    .map(lambda w: (w, 1))                      # [("hello", 1), ("world", 1), ...]
    .reduce_by_key(lambda a, b: a + b)          # [("hello", 3), ("world", 2), ...]
    .collect()
)

print(f"{'word':<20} count")
print("-" * 28)
for word, count in sorted(word_counts):
    print(f"{word:<20} {count}")
