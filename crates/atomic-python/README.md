# atomic-python

Python bindings for the Atomic distributed compute engine — a Spark-like API for local and distributed data processing.

## Prerequisites

- Python ≥ 3.11
- Rust stable (`rustup toolchain install stable`)
- `maturin` (`pip install maturin`)

## Install

### Development (editable)

```bash
cd crates/atomic-python
maturin develop --release
```

### Build a wheel

```bash
cd crates/atomic-python
maturin build --release
# wheel lands in ../../target/wheels/atomic-*.whl
pip install ../../target/wheels/atomic-*.whl
```

## Quick start

```python
import atomic

ctx = atomic.Context(default_parallelism=4)

result = ctx.parallelize([1, 2, 3, 4]) \
            .map(lambda x: x * 2) \
            .filter(lambda x: x > 4) \
            .collect()
# [6, 8]
```

## Running demos

```bash
# Install first (dev mode is fine)
cd crates/atomic-python && maturin develop --release && cd ../..

# Word count
python demo/python/local_word_count.py
```

## API

### `atomic.Context`

| Method | Description |
|---|---|
| `Context(default_parallelism=N)` | Create a context; N defaults to CPU count |
| `parallelize(list, num_partitions=N)` | Create an RDD from a Python list |
| `text_file(path)` | Create an RDD of lines from a text file |
| `range(start, end, step=1, num_partitions=N)` | Create an integer range RDD |

### Transformations (return a new RDD)

| Method | Description |
|---|---|
| `map(f)` | Apply `f` to each element |
| `filter(f)` | Keep elements where `f` returns `True` |
| `flat_map(f)` | Apply `f` and flatten; `f` must return an iterable |
| `map_values(f)` | Apply `f` to value in `(key, value)` pairs |
| `flat_map_values(f)` | Flatten values in `(key, value)` pairs |
| `key_by(f)` | Produce `(f(x), x)` pairs |
| `group_by(f)` | Group by `f(element)` → `(key, [elements])` pairs |
| `group_by_key()` | Group `(key, value)` pairs → `(key, [values])` |
| `reduce_by_key(f)` | Aggregate values per key with `f(acc, v)` |
| `union(other)` | Concatenate two RDDs |
| `zip(other)` | Zip two equal-length RDDs into `(a, b)` pairs |
| `cartesian(other)` | Cartesian product as `(a, b)` pairs |
| `coalesce(n)` / `repartition(n)` | Change logical partition count |

### Actions (trigger execution)

| Method | Description |
|---|---|
| `collect()` | Return all elements as a Python list |
| `count()` | Return number of elements |
| `first()` | Return the first element |
| `take(n)` | Return the first `n` elements |
| `reduce(f)` | Aggregate all elements with `f(acc, x)` |
| `fold(zero, f)` | Aggregate with an initial value |
| `count_by_value()` | Return a `dict` of `{value: count}` |
| `count_by_key()` | Return a `dict` of `{key: count}` for `(k, v)` pairs |
