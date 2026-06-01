# atomic-py

Python bindings for the Atomic distributed compute engine — a Spark-like API for local and distributed data processing, SQL, graph analytics, and micro-batch streaming.

## Prerequisites

- Python ≥ 3.11
- Rust stable (`rustup toolchain install stable`)
- `maturin` (`pip install maturin`)

## Install

### Development (editable)

```bash
cd crates/atomic-py
maturin develop --release
```

### Build a wheel

```bash
cd crates/atomic-py
maturin build --release
pip install ../../target/wheels/atomic_compute-*.whl
```

## Quick start

```python
import atomic_compute

ctx = atomic_compute.Context(default_parallelism=4)

result = (ctx.parallelize([1, 2, 3, 4])
            .map(lambda x: x * 2)
            .filter(lambda x: x > 4)
            .collect())
# [6, 8]
```

---

## API

### `atomic_compute.Context`

| Method | Description |
| --- | --- |
| `Context(default_parallelism=N)` | Create a context; N defaults to CPU count |
| `parallelize(list, num_partitions=N)` | Distribute a Python list as an RDD |
| `text_file(path)` | Create an RDD of lines from a local text file |
| `range(start, end, step=1, num_partitions=N)` | Integer range RDD |
| `broadcast(value)` | Broadcast a read-only value to all tasks → `BroadcastVar` |
| `accumulator(zero)` | Create a distributed counter/accumulator → `Accumulator` |
| `stop()` | Graceful shutdown; signals workers in distributed mode |

### RDD Transformations

| Method | Description |
| --- | --- |
| `map(f)` | Apply `f` to each element |
| `filter(f)` | Keep elements where `f` returns `True` |
| `flat_map(f)` | Apply `f` and flatten; `f` must return an iterable |
| `map_values(f)` | Apply `f` to value in `(key, value)` pairs |
| `flat_map_values(f)` | Flatten values in `(key, value)` pairs |
| `key_by(f)` | Produce `(f(x), x)` pairs |
| `group_by(f)` | Group by `f(element)` → `(key, [elements])` |
| `group_by_key()` | Group `(key, value)` pairs → `(key, [values])` |
| `reduce_by_key(f)` | Aggregate values per key with `f(acc, v)` |
| `join(other)` | Inner join on `(key, value)` pair RDDs → `(k, (v1, v2))` |
| `left_outer_join(other)` | Left outer join; missing right values are `None` |
| `sort_by(key_fn=None, ascending=True)` | Sort by key function; `None` = natural order |
| `sort_by_key(ascending=True)` | Sort `(key, value)` pairs by key |
| `distinct()` | Remove duplicate elements |
| `subtract(other)` | Elements in self but not in other |
| `intersection(other)` | Elements in both RDDs |
| `union(other)` | Concatenate two RDDs |
| `zip(other)` | Zip two equal-length RDDs into `(a, b)` pairs |
| `cartesian(other)` | Cartesian product |
| `map_partitions(f)` | Apply `f` to each partition as a list |
| `keys()` / `values()` | Extract keys or values from `(k, v)` pairs |
| `glom()` | Collect each partition as a sublist → `list[list[T]]` |
| `coalesce(n)` / `repartition(n)` | Change logical partition count |
| `cache()` | Mark for in-memory caching (local mode: no-op) |
| `persist()` / `unpersist()` | Persist / release cached partitions |
| `checkpoint(path)` | Write to `{path}/checkpoint.pkl` and truncate lineage |

### RDD Actions

| Method | Description |
| --- | --- |
| `collect()` | Return all elements as a Python list |
| `count()` | Return number of elements |
| `first()` | Return the first element |
| `take(n)` | Return the first `n` elements |
| `reduce(f)` | Aggregate all elements with `f(acc, x)` |
| `fold(zero, f)` | Aggregate with an initial value |
| `aggregate(zero, seq_fn, comb_fn)` | Two-phase aggregation |
| `for_each(f)` | Call `f(x)` for each element |
| `for_each_partition(f)` | Call `f(partition)` for each partition |
| `count_by_value()` | `{value: count}` dict |
| `count_by_key()` | `{key: count}` dict for `(k, v)` pairs |
| `lookup(key)` | Return all values for a key |
| `is_empty()` | `True` if RDD has no elements |
| `max(key=None)` / `min(key=None)` | Maximum/minimum element |
| `top(n, key=None)` | Top `n` elements (largest first) |
| `take_ordered(n, key=None)` | Bottom `n` elements (smallest first) |
| `save_as_text_file(path)` | Write each element as a line to a file |

---

### `atomic_compute.BroadcastVar`

```python
bv = ctx.broadcast({"threshold": 10})

rdd.filter(lambda x, _bv=bv: x > _bv.value()["threshold"])
```

| Method | Description |
| --- | --- |
| `value()` | Deserialize and return the broadcast value |

### `atomic_compute.Accumulator`

```python
acc = ctx.accumulator(0)
ctx.parallelize([1, 2, 3]).for_each(lambda x: acc.add(x))
print(acc.value())  # 6
```

| Method | Description |
| --- | --- |
| `add(delta)` | Add `delta` (int, float, list append, or string concat) |
| `value()` | Return the current accumulated value |
| `reset()` | Reset to the initial zero value |

---

### SQL — `atomic_compute.SqlContext`

```python
from atomic_compute import SqlContext

ctx = SqlContext()
ctx.register_csv("orders", "data/orders.csv")
df = ctx.sql("SELECT customer_id, SUM(amount) FROM orders GROUP BY 1")
df.show()
```

| Method | Description |
| --- | --- |
| `sql(query)` | Execute SQL and return a lazy `DataFrame` |
| `register_csv(name, path)` | Register a CSV file as a table |
| `register_parquet(name, path)` | Register a Parquet file/directory |
| `register_json(name, path)` | Register a JSONL file/directory |
| `register_rdd(name, rdd, schema)` | Register a Python RDD as a table; schema is `{col: type_str}` |
| `register_udf(name, func, input_types, return_type)` | Register a Python callable as a SQL scalar UDF |
| `deregister_table(name)` | Remove a registered table |

**`DataFrame` methods:** `collect()`, `show()`, `show_limit(n)`, `count()`, `filter(expr)`, `select(cols)`, `limit(n)`, `sort(col, ascending=True)`, `schema()`, `write_parquet(path)`, `write_csv(path)`, `to_arrow()` (→ PyArrow Table)

---

### Graph — `atomic_compute.Graph`

```python
import atomic_compute

# vertices: list of (id, weight), edges: list of (src, dst, weight)
g = atomic_compute.Graph(
    vertices=[(1, 1.0), (2, 1.0), (3, 1.0)],
    edges=[(1, 2, 1.0), (2, 3, 1.0), (3, 1, 1.0)],
)

ranks = g.page_rank(num_iter=20, reset_prob=0.15)
# {1: 0.33, 2: 0.33, 3: 0.33}
```

| Method | Returns | Description |
| --- | --- | --- |
| `Graph(vertices, edges)` | `Graph` | Construct from `[(id, weight)]` and `[(src, dst, weight)]` |
| `num_vertices()` | `int` | Vertex count |
| `num_edges()` | `int` | Edge count |
| `vertices()` | `list[(id, weight)]` | All vertices |
| `edges()` | `list[(src, dst, weight)]` | All edges |
| `page_rank(num_iter=20, reset_prob=0.15)` | `dict[int, float]` | PageRank score per vertex |
| `connected_components()` | `dict[int, int]` | Component label per vertex |
| `strongly_connected_components()` | `dict[int, int]` | SCC label per vertex |
| `label_propagation(max_iter=10)` | `dict[int, int]` | Community label per vertex |
| `triangle_count()` | `dict[int, int]` | Triangle count per vertex |
| `shortest_path(landmarks)` | `dict[int, dict[int, float]]` | Distance from each landmark |

---

### Streaming — `atomic_compute.StreamingContext`

```python
import atomic_compute

ssc = atomic_compute.StreamingContext(batch_secs=1.0)
stream, queue = ssc.test_queue_stream()       # deterministic queue input

results = []
ssc.foreach_rdd(
    stream.flat_map(lambda line: line.split()),
    lambda batch: results.extend(batch),
)

queue.push(["hello world", "foo bar"])
ssc.run_one_batch()    # synchronous tick (for tests / single-step use)
# results == ["hello", "world", "foo", "bar"]
```

**`StreamingContext` methods:**

| Method | Description |
| --- | --- |
| `StreamingContext(batch_secs)` | Create a context with the given batch interval |
| `socket_text_stream(host, port)` | Read text lines from a TCP socket |
| `text_file_stream(directory)` | Watch a directory for new text files |
| `test_queue_stream()` | Returns `(DStream, BatchQueue)` for testing |
| `test_pair_queue_stream()` | Same, but stream is marked as a pair stream |
| `foreach_rdd(stream, f)` | Register output op: `f(batch_list)` called each tick |
| `run_one_batch()` | Execute one batch tick synchronously (for tests) |
| `start()` | Start the background batch loop |
| `stop()` | Signal the batch loop to stop |
| `await_termination_or_timeout(secs)` | Block until stopped or timeout; returns `bool` |

**`DStream` transforms:**

| Method | Description |
| --- | --- |
| `map(f)` | Apply `f` to each element |
| `filter(f)` | Keep elements where `f` is truthy |
| `flat_map(f)` | Apply `f` and flatten |
| `reduce_by_key(f, num_partitions=4)` | Aggregate `(k, v)` pairs per key per batch |
| `group_by_key(num_partitions=4)` | Group `(k, v)` pairs → `(k, [values])` per batch |
| `join(other, num_partitions=4)` | Inner join two pair DStreams per batch |
| `left_outer_join(other, num_partitions=4)` | Left outer join per batch |
| `update_state_by_key(f)` | Stateful aggregation: `f(new_values, old_state) -> new_state` |
| `map_values(f)` | Apply `f` to value in `(k, v)` elements |

**`BatchQueue` methods:**

| Method | Description |
| --- | --- |
| `push(list)` | Enqueue a batch as a Python list |
