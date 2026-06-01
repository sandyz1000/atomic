# Getting Started with Atomic

Atomic is a stable-Rust distributed compute engine with a Spark-like RDD API. It has three entry points: Rust (native), Python (`atomic-compute` on PyPI), and TypeScript/JavaScript (`@atomic-compute/js` on npm).

---

## 1. Local mode — Rust

```bash
cargo add atomic-compute
```

```rust
use atomic_compute::context::Context;
use atomic_compute::env::Config;

fn main() -> anyhow::Result<()> {
    let ctx = Context::new_with_config(Config::local())?;
    let data = vec![1i32, 2, 3, 4, 5, 6, 7, 8];
    let result = ctx
        .parallelize_typed(data, 4)
        .filter(|x| x % 2 == 0)
        .map(|x| x * x)
        .collect()?;
    println!("{result:?}"); // [4, 16, 36, 64]
    Ok(())
}
```

For production code that runs tasks on workers, use the `#[task]` macro to register functions at compile time:

```rust
use atomic_compute::task;

#[task]
fn square(x: i32) -> i32 { x * x }

let result = rdd.map_task(Square).collect()?;
```

---

## 2. Local mode — Python

```bash
pip install atomic-compute
```

```python
import atomic_compute

ctx = atomic_compute.Context()
result = (
    ctx.parallelize([1, 2, 3, 4, 5, 6, 7, 8], num_partitions=4)
    .filter(lambda x: x % 2 == 0)
    .map(lambda x: x * x)
    .collect()
)
print(result)  # [4, 16, 36, 64]
```

---

## 3. Local mode — TypeScript / JavaScript

```bash
npm install @atomic-compute/js
```

```typescript
import { Context } from "@atomic-compute/js";

const ctx = new Context();
const result = ctx
  .parallelize([1, 2, 3, 4, 5, 6, 7, 8], 4)
  .filter((x: number) => x % 2 === 0)
  .map((x: number) => x * x)
  .collect();
console.log(result); // [4, 16, 36, 64]
```

---

## 4. Word count example

```python
import atomic_compute

ctx = atomic_compute.Context(default_parallelism=4)

words = (
    ctx.text_file("data/shakespeare.txt")
    .flat_map(str.split)
    .map(lambda w: (w.lower(), 1))
    .reduce_by_key(lambda a, b: a + b)
    .collect()
)

top_10 = sorted(words, key=lambda kv: -kv[1])[:10]
for word, count in top_10:
    print(f"{word:20s} {count}")
```

---

## 5. SQL queries

```python
import atomic_compute

ctx = atomic_compute.SqlContext()
ctx.register_csv("orders", "data/orders.csv")

df = ctx.sql("SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id")
df.show()

# Export to Parquet
df.write_parquet("/tmp/output/")

# Convert to Pandas via PyArrow
table = df.to_arrow()
pandas_df = table.to_pandas()
```

---

## 6. Distributed mode

Start a worker on each remote machine (same binary as your driver):

```bash
# On each worker host
./my_app --worker --port 10001
```

Configure the driver to connect to workers:

```python
import os
os.environ["ATOMIC_DEPLOYMENT_MODE"] = "distributed"
os.environ["ATOMIC_LOCAL_IP"] = "10.0.0.100"  # driver's IP
os.environ["ATOMIC_WORKERS"] = "10.0.0.101:10001,10.0.0.102:10001"

import atomic_compute
ctx = atomic_compute.Context()
```

Or in Rust:

```rust
use std::net::{Ipv4Addr, SocketAddrV4};
use atomic_compute::env::Config;

let config = Config::builder()
    .local_ip("10.0.0.100".parse()?)
    .workers(vec![
        "10.0.0.101:10001".parse()?,
        "10.0.0.102:10001".parse()?,
    ])
    .build();
let ctx = Context::new_with_config(config)?;
```

---

---

## 7. Graph analytics (Python + JavaScript)

```python
import atomic_compute

# vertices: [(id, weight)], edges: [(src, dst, weight)]
g = atomic_compute.Graph(
    vertices=[(1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)],
    edges=[(1, 2, 1.0), (2, 3, 1.0), (3, 4, 1.0), (4, 1, 1.0)],
)

print(g.page_rank())              # {1: 0.25, 2: 0.25, 3: 0.25, 4: 0.25}
print(g.connected_components())   # {1: 1, 2: 1, 3: 1, 4: 1}
print(g.shortest_path([1]))       # {1: {1: 0, 2: 1.0, 3: 2.0, 4: 3.0}}
```

The same API is available in TypeScript as `new Graph(vertices, edges)` with camelCase methods (`pageRank`, `connectedComponents`, `shortestPath`).

---

## 8. Micro-batch streaming (Python + JavaScript)

```python
import atomic_compute

ssc = atomic_compute.StreamingContext(batch_secs=1.0)

# For tests, use a queue-backed stream
stream, queue = ssc.test_pair_queue_stream()

word_counts = {}
def update_counts(new_vals, old_state):
    return (old_state or 0) + sum(new_vals)

stateful = stream.update_state_by_key(update_counts)
ssc.foreach_rdd(stateful, lambda batch: word_counts.update(dict(batch)))

# Push a batch and process it
queue.push([("hello", 1), ("world", 1), ("hello", 1)])
ssc.run_one_batch()
print(word_counts)  # {'hello': 2, 'world': 1}

# For real-time use, start the background loop instead:
# ssc.start()
# ssc.await_termination_or_timeout(60.0)
```

---

## 9. Broadcast variables and accumulators (Python + JavaScript)

```python
import atomic_compute

ctx = atomic_compute.Context()

# Broadcast: share a large read-only value with all tasks
threshold = ctx.broadcast({"min": 3, "max": 10})
result = (
    ctx.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    .filter(lambda x, _t=threshold: _t.value()["min"] <= x <= _t.value()["max"])
    .collect()
)
print(result)  # [3, 4, 5, 6, 7, 8, 9, 10]

# Accumulator: distributed counter
acc = ctx.accumulator(0)
ctx.parallelize([1, 2, 3, 4, 5]).for_each(lambda x: acc.add(x))
print(acc.value())  # 15
```

---

## 10. Next steps

- [Configuration Reference](configuration.md) — all `ATOMIC_*` env vars and `Config` fields
- [Deployment Guide](deployment.md) — building, shipping binaries, mTLS, S3
- [atomic-py README](../crates/atomic-py/README.md) — full Python API reference
- [atomic-js README](../crates/atomic-js/README.md) — full TypeScript/JS API reference
- [API Reference](https://docs.rs/atomic-compute) — full Rust API docs
