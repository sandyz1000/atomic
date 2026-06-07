# Atomic

**A distributed compute engine in stable Rust — where your task functions are compiled in,
not pickled across.**

Atomic is a Spark-inspired RDD engine built on three ideas no other distributed framework has combined:

1. **Zero closure serialization.** Task functions are registered at compile time via `#[task]` and dispatched by ID. Workers can never receive code they weren't compiled with. No pickle failures, no "class not found", no nightly Rust required.
2. **One binary, anywhere.** Driver and worker are the same executable. No JVM, no daemon, no cluster manager required to start. Cross-compile with `atomic build`, ship with `atomic ship` over SSH — workers are running in under a minute.
3. **Prototype in Python, optimize in Rust — same API.** Write a job in Python or TypeScript, confirm it's correct, then rewrite the hot partition as a `#[task]` Rust function. The driver script does not change. No rewrite from scratch, no framework switch.

---

## Why This Matters

Every other distributed framework ships code to workers at runtime:

- **Spark/PySpark** pickles Python closures and ships JVM bytecode. "Pickle errors" and "task not serializable" are rites of passage.
- **Flink** serializes Java lambdas. Kryo failures are a known production hazard.
- **Ray** ships Python functions by serializing their closure state. Complex Python object graphs fail to serialize in ways that are hard to debug.

Atomic's `#[task]` approach inverts this. The worker's dispatch table is linked at compile time via `inventory`. The driver sends a string ID and a data payload — not code. If a task ID doesn't exist on the worker, you get a clear error with a list of what *is* registered, at dispatch time, not buried in a worker log three hours later.

```text
Task 'my_crate::transform::normalize_v2' not registered in TASK_REGISTRY.
Registered ops (12 total): [my_crate::transform::normalize, my_crate::transform::filter_nulls, ...]
```

This is a structural guarantee, not a coding convention.

A second safety layer catches the subtler case — where the task name stays the same but the **body changes**. Every `#[task]` function embeds a short FNV-1a hash of its body tokens in its op_id (`"my_crate::transform::normalize::a1b2c3d4"`). The entire registry is hashed into a single `REGISTRY_FINGERPRINT`. Workers advertise their fingerprint on the TCP handshake; the driver compares it before accepting the worker. A mismatched fingerprint means the worker was compiled from different code and is logged as an error before any task is dispatched:

```text
worker 10.0.0.5:10001 registry fingerprint mismatch:
  driver=0xdeadbeef12345678, worker=0xbadcafe087654321.
Task implementations diverged — redeploy workers with the same binary.
```

---

## Quick Start

### Rust

```rust
use atomic_compute::{context::Context, env::Config, task};

#[task]
fn square(x: i32) -> i32 { x * x }

fn main() -> anyhow::Result<()> {
    let ctx = Context::new_with_config(Config::local())?;

    let result = ctx
        .parallelize_typed(vec![1, 2, 3, 4, 5], 2)
        .filter(|x| x % 2 != 0)
        .map_task(Square)          // dispatched to workers by ID in distributed mode
        .collect()?;

    println!("{result:?}");        // [1, 9, 25]
    Ok(())
}
```

Switch to distributed mode by changing one `Config` line — the job code is unchanged:

```rust
let config = Config::builder()
    .local_ip("10.0.0.100".parse()?)
    .workers(vec!["10.0.0.101:10001".parse()?, "10.0.0.102:10001".parse()?])
    .build();
```

### Python (prototype)

```python
import atomic_compute

ctx = atomic_compute.Context()
result = (
    ctx.parallelize([1, 2, 3, 4, 5], num_partitions=2)
       .filter(lambda x: x % 2 != 0)
       .map(lambda x: x * x)
       .collect()
)
print(result)  # [1, 9, 25]
```

### TypeScript

```typescript
import { Context } from "@atomic-compute/js";

const ctx = new Context();
const result = ctx
  .parallelize([1, 2, 3, 4, 5], 2)
  .filter((x: number) => x % 2 !== 0)
  .map((x: number) => x * x)
  .collect();
console.log(result);  // [1, 9, 25]
```

---

## The PoC → Production Workflow

Atomic is designed around a progressive adoption model. Start with Python or TypeScript to prove correctness quickly, then rewrite the job in Rust for production throughput.

### Step 1 — Prototype in Python

```python
import atomic_compute

ctx = atomic_compute.Context()
result = (
    ctx.text_file("data/events.txt")       # Python reads local or remote paths
       .flat_map(lambda line: line.split())
       .map(lambda w: (w.lower(), 1))
       .reduce_by_key(lambda a, b: a + b)
       .collect()
)
```

Python lambdas are pickled with `cloudpickle` and executed by the embedded PyO3 runtime on workers. This works immediately — no compilation step.

### Step 2 — Rewrite in Rust when correctness is confirmed

Once the Python job produces correct results, rewrite it in Rust using the `#[task]` API. This is a new program, not a patch to the Python driver:

```rust
use atomic_compute::{context::Context, env::Config, task};

#[task]
fn tokenize(line: String) -> Vec<(String, u64)> {
    line.split_whitespace()
        .map(|w| (w.to_lowercase(), 1u64))
        .collect()
}

fn main() -> anyhow::Result<()> {
    let ctx = Context::new_with_config(Config::distributed(workers)?)?;
    ctx.text_file("s3://my-bucket/events/")?
       .flat_map_task(Tokenize)
       .reduce_by_key(Add)
       .collect()
}
```

### Step 3 — Redeploy the same binary

```bash
atomic build && atomic ship --workers user@host1,user@host2
```

Workers now execute the compiled Rust `#[task]` function instead of deserializing a Python lambda. The Python prototype still runs unchanged — it uses the PyO3 runtime path and produces the same results. Both versions are independently deployable from the same cluster.

**Language capability summary:**

| Feature | Rust | Python | TypeScript |
| --- | --- | --- | --- |
| `#[task]` compile-time dispatch | yes | no | no |
| Closure / lambda UDFs | yes (local) | yes (pickled) | yes (V8 source string) |
| SQL (`SqlContext`) | yes | yes | yes |
| Streaming (`StreamingContext`) | yes | yes | yes |
| Graph (`Graph`, 6 algorithms) | yes | yes | yes |
| Broadcast variables / accumulators | yes | yes | yes |
| `join`, `cogroup`, `sort_by`, `glom`, `cache`, `checkpoint` on RDD | yes | yes | yes |
| S3 `text_file` / `save_as_text_file` | yes | yes (`s3` feature) | yes (`s3` feature) |
| Pregel custom vertex programs | yes | yes (`run_pregel`) | yes (`runPregelF64`) |

---

## SQL Queries

`atomic-sql` wraps [Apache DataFusion](https://github.com/apache/datafusion) — a full query optimizer with 30+ rewrite rules, Arrow columnar execution, and Parquet/CSV/JSON readers.

**Python:**

```python
from atomic_compute import SqlContext

ctx = SqlContext()
ctx.register_parquet("orders", "data/orders.parquet")   # local path

df = ctx.sql("""
    SELECT customer_id,
           COUNT(*)      AS order_count,
           SUM(amount)   AS total_spent
    FROM orders
    WHERE status = 'completed'
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 100
""")

df.show()
df.write_parquet("/tmp/top_customers/")

# Python-only: export to PyArrow Table → Pandas
table = df.to_arrow()
pandas_df = table.to_pandas()
```

**Python — register an RDD as a SQL table** (Python only; schema required):

```python
from atomic_compute import Context, SqlContext

rdd_ctx = Context()
sql_ctx = SqlContext()

rdd = rdd_ctx.parallelize([{"id": 1, "val": 2.5}, {"id": 2, "val": 3.0}])
sql_ctx.register_rdd("data", rdd, {"id": "int64", "val": "float64"})
df = sql_ctx.sql("SELECT * FROM data WHERE val > 2.0")
```

**TypeScript:**

```typescript
import { SqlContext } from "@atomic-compute/js";

const ctx = new SqlContext();
ctx.registerParquet("orders", "data/orders.parquet");

const df = ctx.sql(`
    SELECT customer_id, COUNT(*) AS order_count
    FROM orders
    GROUP BY customer_id
`);
df.show();
df.writeParquet("/tmp/output/");
```

---

## Natural Language Queries (`atomic-nlq`)

Atomic's NLQ layer makes LLM-native query planning a first-class feature, not a prompt-engineering wrapper.

The LLM doesn't produce SQL. It produces a structured JSON plan that `IrParser` converts directly to a DataFusion `LogicalPlan`. Novel operators (`LlmFilterNode`, `LlmMapNode`, `EmbedNode`, `VectorSearchNode`) are DataFusion `Extension` nodes — they participate in predicate push-down, projection pruning, and `LlmBatchingRule` groups per-row LLM calls into batched API requests before the physical plan runs.

```text
User: "find customers who bought luxury items and estimate lifetime value"
         │
         ▼  LlmPlanner (Anthropic API: schema + NL query)
  Structured JSON plan (not SQL)
         │
         ▼  IrParser → DataFusion LogicalPlan
  Aggregate {
    LlmFilterNode { prompt: "is this a luxury item?", col: "category" }
      TableScan("orders")
  }
         │
         ▼  LlmBatchingRule → batch N rows into one API call
         ▼  Physical planner → RddScanExec + LlmFilterExec
         │
         ▼  atomic-compute workers
```

No other distributed compute framework has wired LLM calls into a distributed query optimizer as first-class plan nodes.

---

## Deployment — Ship a Static Binary in 60 Seconds

```bash
# Install the CLI
cargo install --path crates/atomic-cli

# Cross-compile a fully static Linux binary (no deps, no JVM, no Python)
atomic build --target x86_64-unknown-linux-musl

# Upload to workers via SSH with host-key verification + SHA-256 integrity check
atomic ship --workers user@10.0.0.101,user@10.0.0.102

# Start workers (same binary, different flag)
./my_app --worker --port 10001
```

The `ship` command verifies the remote host against `~/.ssh/known_hosts`, uploads via SFTP, verifies the SHA-256 checksum on the remote, and renames atomically. No Docker, no registry, no Kubernetes required.

---

## Feature Matrix

| Category | Feature | Status |
| --- | --- | --- |
| **Core** | `#[task]` compile-time dispatch | yes |
| | `task_fn!` inline anonymous tasks | yes |
| | Local thread-pool execution | yes |
| | Distributed TCP execution | yes |
| | Lazy pipeline staging (multi-op `TaskEnvelope`) | yes |
| | Speculative execution | yes |
| | Job cancellation | yes |
| **RDD API** | `map`, `filter`, `flat_map`, `reduce_by_key`, `group_by_key` | yes |
| | `join`, `left_outer_join`, `right_outer_join`, `full_outer_join` (shuffle-based) | yes |
| | `cogroup`, `cogroup_shuffle` — distributed, no driver collect | yes |
| | `flat_map_values`, `map_partitions_to_pair` | yes |
| | `repartition_shuffle(n)` — element-level redistribution | yes |
| | `sort_by_key` — distributed (sample → range partition → local sort) | yes |
| | `fold_by_key`, `aggregate_by_key`, `subtract_by_key` | yes |
| | `tree_reduce`, `tree_aggregate` | yes |
| | `to_local_iterator`, `collect_as_map`, `count_approx` | yes |
| | `to_debug_string` (DAG lineage printer) | yes |
| | Custom partitioner (`partition_by`) | yes |
| | `cache`, `persist`, `unpersist`, `checkpoint` | yes |
| | `MemoryAndDisk` / `DiskOnly` storage levels | yes |
| **Shuffle** | Hash shuffle + disk spill | yes |
| | Adaptive partition coalescing | yes |
| | Shuffle-map stage fault recovery | yes |
| **SQL** | DataFusion query engine (30+ optimizer rules) | yes |
| | Parquet, CSV, JSON readers | yes |
| | RDD-backed table provider | yes |
| | DataFrame write (Parquet, CSV) | yes |
| | SQL UDF registration (Python callable) | yes |
| **Streaming** | Micro-batch DStream (`StreamingContext`) — Rust + Python + JS | yes |
| | `map`, `filter`, `flat_map`, `reduce_by_key`, `join`, `update_state_by_key` | yes |
| | Checkpoint (bincode, atomic write) | yes |
| | Kafka source | planned |
| | Event-time watermarking | planned |
| **Graph** | `Graph<VD,ED>` + Pregel engine — Rust | yes |
| | `Graph(vertices, edges)` + 6 algorithms — Python + JS | yes |
| | PageRank, SSSP, SCC, LabelPropagation, TriangleCount, CC | yes |
| **Language Bindings** | Python (`atomic-compute` on PyPI) — RDD + SQL + Graph + Streaming | yes |
| | TypeScript/JavaScript (`@atomic-compute/js` on npm) — RDD + SQL + Graph + Streaming | yes |
| | `BroadcastVar`, `Accumulator` — Python + JS | yes |
| | `join`, `sort_by`, `glom`, `cache`, `checkpoint` on RDD — Python + JS | yes |
| | Python → Arrow (`df.to_arrow()`) | yes |
| | Python RDD → SQL bridge (`register_rdd`) | yes |
| **Infrastructure** | S3 object store — Rust only (`s3` feature) | yes |
| | Mutual TLS (`tls` feature, rustls) | yes |
| | Prometheus `/metrics` endpoint | yes |
| | Dynamic worker heartbeat + removal | yes |
| | `atomic build` (musl static binary) | yes |
| | `atomic ship` (SSH/SFTP, host-key verified) | yes |
| | S3 I/O for Python + JS bindings (`s3` feature) | yes |
| | Pregel custom vertex programs — Python + JS | yes |
| **NLQ** | LLM-native DataFusion plan nodes | yes |
| | `LlmBatchingRule` optimizer | yes |
| | `InMemoryVectorIndex` | yes |

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│  Driver (Python / TypeScript / Rust)                        │
│                                                             │
│  Context → TypedRdd → StagedPipeline → TaskEnvelope         │
│                                    │                        │
│              AtomicSqlContext → DataFusion LogicalPlan       │
│                                    │                        │
│              NlqContext → LlmPlanner → IrParser             │
└────────────────────────┬────────────────────────────────────┘
                         │  TCP (optional mTLS)
            ┌────────────┼────────────┐
            │            │            │
      ┌─────▼──┐   ┌─────▼──┐  ┌─────▼──┐
      │ Worker │   │ Worker │  │ Worker │
      │        │   │        │  │        │
      │ TASK_  │   │ TASK_  │  │ TASK_  │
      │REGISTRY│   │REGISTRY│  │REGISTRY│
      │(same   │   │(same   │  │(same   │
      │binary) │   │binary) │  │binary) │
      └────────┘   └────────┘  └────────┘
```

**Key architectural properties:**

- `TASK_REGISTRY` is linked at compile time via `inventory::submit!`. Workers cannot execute tasks they weren't compiled with — there is no remote code execution surface.
- All distributed wire types use `rkyv` for zero-copy deserialization. No reflection, no dynamic dispatch on the hot path.
- `LocalScheduler` and `DistributedScheduler` share the same `NativeBackend` dispatch. Local-mode tests cover exactly the same codepath as distributed-mode jobs.
- Python UDFs are `cloudpickle`-serialized and executed by the embedded PyO3 runtime in `atomic-worker`. JavaScript UDFs are shipped as source strings and evaluated by the embedded V8 runtime. Both go through the same `TaskEnvelope` wire format as Rust `#[task]` functions.

---

## Crate Layout

| Crate | Purpose |
| --- | --- |
| `atomic-data` | Shared types — RDD traits, task envelopes, wire protocol, shuffle primitives, cache |
| `atomic-compute` | Execution runtime — context, executor, `NativeBackend`, RDD implementations |
| `atomic-scheduler` | `LocalScheduler` (thread-pool) + `DistributedScheduler` (TCP, speculative, heartbeat) |
| `atomic-sql` | SQL layer — `AtomicSqlContext`, `DataFrame`, RDD-backed DataFusion table providers |
| `atomic-streaming` | Micro-batch streaming — `StreamingContext`, `DStream`, `JobScheduler` |
| `atomic-graph` | Graph processing — `Graph<VD,ED>`, Pregel engine, built-in algorithms |
| `atomic-nlq` | Natural language query — LLM-native DataFusion plan nodes, `LlmBatchingRule` |
| `atomic-py` | Python bindings (maturin/PyO3) — full RDD + SQL API, Arrow integration |
| `atomic-js` | JavaScript/TypeScript bindings (napi-rs) — full RDD + SQL API |
| `atomic-worker` | Polyglot worker binary — embedded PyO3 + V8 runtimes |
| `atomic-cli` | Cross-compilation (`cargo-zigbuild`) + secure SSH/SFTP binary distribution |
| `atomic-runtime-macros` | `#[task]` and `task_fn!` proc-macros |

---

## Honest Comparison With Spark

| Dimension | Spark | Atomic |
| --- | --- | --- |
| Task dispatch | Runtime pickle / bytecode shipping | Compile-time `#[task]` dispatch table |
| Worker startup | JVM cold start (seconds) | Native binary (milliseconds) |
| Memory model | GC + off-heap tricks | Rust ownership + rkyv zero-copy |
| Closure safety | Runtime serialization failures | Compile-time — "does it build?" = "does it dispatch?" |
| Deployment | JVM on every node + cluster manager | Static musl binary, SSH upload |
| SQL optimizer | Catalyst (10yr+, highly mature) | DataFusion (excellent, newer) |
| Streaming | Structured Streaming + Kafka, exactly-once | Micro-batch; no Kafka yet |
| Kubernetes | Full operator | Not yet |
| Ecosystem | Delta Lake, MLflow, hundreds of connectors | Early |
| NLQ / LLM | Plugin / prompt wrapper | First-class plan nodes |
| Stability | Exabyte-tested | Early-stage; strong test suite |

Atomic is likely **faster** for small-to-medium CPU-bound jobs where JVM overhead and GC dominate. Spark wins for very large shuffles, complex joins with AQE, and Kafka-scale streaming. Choose Atomic if you want to avoid the JVM stack entirely and accept being an early adopter.

---

## Getting Started

### Rust (examples)

```bash
cargo build --release
cargo run --example task_wordcount
cargo run --example pi
cargo run --example sort
```

### Python (bindings)

```bash
cd crates/atomic-py
pip install maturin
maturin develop --release
pytest tests/
```

### JavaScript / TypeScript (bindings)

```bash
cd crates/atomic-js
npm install
npm run build
npm test
```

### Distributed mode — local loopback (same machine)

Driver and worker run as two separate processes on the same machine. Useful for testing distributed code locally — no shipping needed.

```bash
# 1. Build
cargo build --release

# 2. Start a worker in one terminal
RUST_LOG=info ./target/release/my_app --worker --port 10001

# 3. Run the driver in another terminal
ATOMIC_DEPLOYMENT_MODE=distributed \
ATOMIC_LOCAL_IP=127.0.0.1 \
ATOMIC_WORKERS=127.0.0.1:10001 \
./target/release/my_app
```

### Distributed mode — real cluster (separate machines)

Workers run on remote hosts. You must cross-compile, ship the binary, and start workers before running the driver.

```bash
# 1. Cross-compile a static Linux binary
cargo install --path crates/atomic-cli   # install once
atomic build --target x86_64-unknown-linux-musl

# 2. Ship to worker hosts (SSH key from agent; host-key verified)
atomic ship --workers user@10.0.0.101,user@10.0.0.102

# 3. Start workers on each remote host (SSH in, or via systemd)
ssh user@10.0.0.101 "RUST_LOG=info ./my_app --worker --port 10001 &"
ssh user@10.0.0.102 "RUST_LOG=info ./my_app --worker --port 10001 &"

# 4. Run the driver locally
ATOMIC_DEPLOYMENT_MODE=distributed \
ATOMIC_LOCAL_IP=10.0.0.100 \
ATOMIC_WORKERS=10.0.0.101:10001,10.0.0.102:10001 \
./target/release/my_app
```

See [docs/getting-started.md](docs/getting-started.md), [docs/configuration.md](docs/configuration.md), and [docs/deployment.md](docs/deployment.md) for full documentation.

---

## Status

**Beta** — all core features are implemented and tested. The test suite covers local execution, distributed TCP dispatch, shuffle, streaming, graph, and SQL. Production readiness depends on your risk tolerance and workload:

- yes **Ready**: Local-mode jobs, SQL analytics (DataFusion), graph algorithms, Python/JS prototyping, musl static binary deployment
- ⚠️ **Early adopter**: Distributed mode on real workloads — core is solid, but cluster management (K8s) and streaming sources (Kafka) are missing
- no **Not yet**: Kafka streaming, Kubernetes operator, event-time watermarking, sort-based shuffle

---

## License

[Apache 2.0](LICENSE)
