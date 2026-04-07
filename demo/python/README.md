# Python Examples

Spark-like jobs written in Python using the `atomic` extension module
(built from `crates/atomic-py` via PyO3).

## Setup

```bash
# Install maturin if needed
pip install maturin

# Build and install the atomic Python module in dev mode
cd crates/atomic-py
maturin develop --release
cd ../..
```

## Examples

### `local_word_count.py` — Pure local job

Classic word count using `PyRdd` transformations. Everything runs in-process.

```bash
cd demo/python
python local_word_count.py
```

Expected output:
```
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

### `local_sum_job.py` — Range sum demo

Partitions a numeric range and sums all values using `fold`.

```bash
cd demo/python
python local_sum_job.py
```

Expected output:
```
Sum of 1..100 = 5050
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
