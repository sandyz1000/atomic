# Python Examples

Spark-like jobs written in Python using the `atomic` extension module
(built from `crates/atomic-py` via PyO3).

## Prerequisites

- Python ≥ 3.11
- Rust toolchain (stable)
- `maturin` (`pip install maturin`)

## Setup

```bash
cd examples/py-demo

# Build and install the atomic Python module into the active venv/interpreter
pip install maturin
cd ../../crates/atomic-py && maturin develop --release && cd ../../examples/py-demo
```

## Running examples

```bash
# All local examples (word count + sum)
python3 main.py

# Word count only
python3 src/local_word_count.py

# Range sum only
python3 src/local_sum_job.py

# Distributed word count (see distributed section below)
python3 src/distributed_word_count.py
```

Or via Make:

```bash
make run            # word count + sum
make run-wordcount  # word count only
make run-sum        # range sum only
make run-distributed
```

## Examples

### `src/local_word_count.py` — Word count (local)

Classic word count using `Rdd` transformations. Everything runs in-process.

```text
Expected output:

word                 count
----------------------------
and                  1
atomic               2
bar                  1
fast                 3
foo                  1
hello                3
is                   2
need                 1
small                1
systems              1
tools                1
world                2
```

### `src/local_sum_job.py` — Range sum demo

Partitions the range [1, 101) across 4 partitions and folds into a single sum.

```text
Expected output:

Sum of 1..100 = 5050
```

### `src/distributed_word_count.py` — Distributed word count

Python lambdas are serialized via `pickle` and executed on workers by their embedded
PyO3 runtime. The `atomic-worker` binary handles all Python UDF execution — no binary
deployment of your code is required.

```bash
# Build the worker binary
cargo build --release -p atomic-worker --manifest-path ../../Cargo.toml

# Start a worker in a separate terminal
RUST_LOG=info ../../target/release/atomic-worker --worker --port 10001

# Run the driver
python3 src/distributed_word_count.py
```

## API overview

```python
import atomic

ctx = atomic.Context(default_parallelism=4)

rdd = ctx.parallelize([1, 2, 3, 4])        # from list
rdd = ctx.range(0, 100, step=1)            # from range
rdd = ctx.text_file("/path/to/file.txt")   # line-by-line file

# Transformations (lazy, return new Rdd)
rdd.map(f)                     # apply f to each element
rdd.filter(pred)               # keep elements where pred(x) is True
rdd.flat_map(f)                # f returns iterable; results are flattened
rdd.key_by(f)                  # produce (f(x), x)
rdd.map_values(f)              # for (k, v): produce (k, f(v))
rdd.group_by(f)                # group by f(x)
rdd.group_by_key()             # group (k, v) pairs by k
rdd.reduce_by_key(f)           # aggregate values per key using f
rdd.union(other)               # concatenate two Rdds
rdd.zip(other)                 # element-wise (x, y) pairs
rdd.cartesian(other)           # all (x, y) combinations
rdd.coalesce(n)                # reduce to n partitions
rdd.repartition(n)             # change partition count

# Actions (eager, return a value)
rdd.collect()                  # all elements as a Python list
rdd.count()                    # number of elements
rdd.first()                    # first element
rdd.take(n)                    # first n elements
rdd.reduce(f)                  # fold with f(acc, elem)
rdd.fold(zero, f)              # fold with initial value
```

## Notes

- All element types must be picklable (numbers, strings, lists, dicts, tuples).
- Lambda functions are pickled via `pickle.dumps()` for distributed dispatch.
- Workers execute pickled lambdas via their embedded PyO3 runtime.
- For distributed jobs: only the `atomic-worker` binary needs to be on the workers;
  your Python script stays on the driver.
