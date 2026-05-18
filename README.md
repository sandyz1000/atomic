# Atomic

A distributed data processing framework written in stable Rust, inspired by Apache Spark and Vega.

> **Not production-ready.** Atomic is an experimental research project. Critical gaps remain:
> distributed shuffle disk spill, complete fault recovery for shuffle-map stages, LRU eviction
> for the partition cache, and a hardened test suite. Do not use it in production systems.

See [ROADMAP.md](ROADMAP.md) for the full production readiness plan.

---

## What is Atomic?

Atomic is a rewrite and redesign of [vega](https://github.com/rajasekarv/vega), a Spark-inspired
distributed compute engine for Rust. Vega proved that Rust could support a Spark-like RDD model,
but it required **nightly Rust** to serialize closures across the network — a fragile foundation
that made the project unmaintainable.

Atomic solves this by replacing closure serialization entirely. Tasks are registered at compile
time via a `#[task]` macro and dispatched to workers by a stable string ID. There are no nightly
features, no unsafe closure transmutes, and no runtime reflection. Driver and worker run **the
same binary** — the dispatch table is built at compile time and cannot drift.

---

## How Atomic Improves on Vega

**Vega's core limitation:** sending a closure from driver to worker required serializing a Rust
function pointer — only possible on nightly Rust via unstable intrinsics.

**Atomic's approach:**

- Tasks are plain Rust functions annotated with `#[task]`. The macro registers them into a
  compile-time dispatch table (via `inventory`). Workers look up tasks by ID — no closure,
  no unsafe transmute.
- Partition data is encoded with `rkyv` for zero-copy deserialization on the worker side.
- The driver API (`ctx.parallelize(...).map_task(...).collect()`) looks like Spark/Vega, but
  under the hood it builds a `PipelineOp` chain dispatched to workers over TCP without any
  function serialization.
- Python and JavaScript UDFs are first-class distributed operations via embedded PyO3 and
  V8 runtimes (deno_core). Python users get a PySpark-equivalent REPL experience without requiring Rust.
- SQL queries are executed by [DataFusion](https://github.com/apache/datafusion) — a full query
  optimizer, 30+ rewrite rules, Arrow columnar execution, and Parquet/CSV/JSON readers.
- Micro-batch streaming (`atomic-streaming`) follows the Spark Streaming DStream model.
- Graph processing (`atomic-graph`) follows the Spark GraphX / Pregel model.
- Local and distributed execution share the same `dispatch_pipeline` contract. Switching modes
  is a `Config` flag, not a code change.

---

## Quick Example

**Rust native task:**

```rust
#[task]
fn double(x: i32) -> i32 { x * 2 }

let ctx = Context::new_with_config(Config::local())?;
let result = ctx.parallelize_typed(vec![1, 2, 3, 4], 2)
    .map_task(Double)
    .collect()?;
// [2, 4, 6, 8]
```

**Inline task lambda:**

```rust
let result = ctx.parallelize_typed(vec![1i32, 2, 3, 4], 2)
    .map_task(task_fn!(|x: i32| -> i32 { x * 2 }))
    .filter_task(task_fn!(|x: i32| -> bool { x > 4 }))
    .collect()?;
// [6, 8]
```

**SQL query over an RDD:**

```rust
let sc = Arc::new(Context::new_with_config(Config::local())?);
let rdd = sc.parallelize_typed(batches, 4);   // batches: Vec<RecordBatch>
let ctx = AtomicSqlContext::with_compute(Arc::clone(&sc));
ctx.register_rdd("events", rdd)?;
let df = ctx.sql("SELECT user_id, COUNT(*) FROM events GROUP BY 1").await?;
df.show().await?;
```

**Python UDF (local or distributed):**

```python
import atomic
ctx = atomic.Context()
result = ctx.parallelize([1, 2, 3, 4], num_partitions=2) \
            .map(lambda x: x * 2) \
            .filter(lambda x: x > 4) \
            .collect()
# [6, 8]
```

**Micro-batch streaming:**

```rust
let ssc = StreamingContext::new(ctx, Duration::from_secs(1));
let queue = Arc::new(Mutex::new(VecDeque::new()));
let stream = ssc.queue_stream(queue.clone(), true);
ssc.foreach_rdd(stream, |rdd, _t| { /* process batch RDD */ });
ssc.start()?;
ssc.await_termination()?;
```

---

## Crate Layout

| Crate | Purpose |
| --- | --- |
| `atomic-data` | Shared types: RDD traits, task envelopes, wire protocol, shuffle primitives, partition cache |
| `atomic-compute` | Execution runtime: context, executor, `NativeBackend`, RDD impls, UDF dispatch, persist/cache |
| `atomic-scheduler` | Local thread-pool (`LocalScheduler`) and distributed TCP (`DistributedScheduler`) schedulers |
| `atomic-sql` | SQL layer built on DataFusion — `AtomicSqlContext`, `DataFrame`, RDD-backed table providers |
| `atomic-streaming` | Spark Streaming–style micro-batch streaming — `StreamingContext`, `DStream`, `JobScheduler` |
| `atomic-graph` | GraphX-style graph processing — `Graph`, Pregel engine, PageRank, shortest path, SCC, LPA |
| `atomic-cli` | Cross-compilation (`cargo-zigbuild`) + secure SSH/SFTP binary distribution to workers |
| `atomic-nlq` | Natural language query layer (LLM-first analytics) — planned; partially scaffolded |
| `atomic-runtime-macros` | `#[task]` and `task_fn!` proc-macros for compile-time task registration |
| `atomic-py` | Python extension module (maturin/PyO3) — Spark-like Python driver API |
| `atomic-js` | JavaScript library (napi-rs) — Spark-like JS/TS driver API |
| `atomic-worker` | Polyglot worker binary with embedded Python (PyO3) + V8 (deno_core) runtimes |
| `atomic-utils` | Shared utilities (bounded priority queue, random helpers, etc.) |

---

## What is Implemented

### Core Engine

- [x] Stable-Rust task dispatch via `#[task]` macro and `task_fn!` — no nightly required
- [x] `rkyv` zero-copy wire protocol for partition data
- [x] Local thread-pool execution via `LocalScheduler` (full DAG / stage / shuffle support)
- [x] Distributed TCP execution via `DistributedScheduler` (capacity-aware placement)
- [x] Multi-op lazy pipeline staging (`PipelineOp` chains dispatched as single `TaskEnvelope`)
- [x] Two-phase shuffle execution — lazy `ShuffleDependency` + `ShuffledRdd` DAG
- [x] `DashMapShuffleCache` + `ShuffleManager` HTTP server + `ShuffleFetcher` with exponential-backoff retry
- [x] `MapOutputTracker` — tracks shuffle bucket URIs per worker
- [x] `partition_id` in `TaskResultEnvelope` for correct result ordering after retries
- [x] Per-task failure counter, resubmit queue, `MaxTaskFailures` error
- [x] RDD persist/cache — `cache()` / `persist(StorageLevel)` backed by `PartitionStore` (MemoryOnly)
- [x] `AtomicApp::build()` — unified entry point; reads `--worker`/`--workers`/`--local-ip` from CLI
- [x] Explicit `Config` struct at entry point — replaces global env-var reading

### RDD API

- [x] Transformations: `map_task`, `filter_task`, `flat_map_task`, `fold_task`, `reduce_task`
- [x] Transformations: `map`, `filter`, `flat_map`, `map_values`, `flat_map_values`, `key_by`
- [x] Pair operations: `reduce_by_key`, `group_by_key`, `count_by_key`, `count_by_value`, `group_by`
- [x] Set/combine: `union`, `zip`, `cartesian`, `coalesce`, `map_partitions`
- [x] Actions: `collect`, `count`, `take`, `first`, `fold`, `reduce`, `aggregate`, `for_each`, `for_each_partition`, `is_empty`, `top`, `take_ordered`, `max`, `min`, `count_by_value`
- [x] All actions dispatch staged pipelines to workers in distributed mode

### SQL Layer (`atomic-sql`)

- [x] `AtomicSqlContext` wrapping DataFusion `SessionContext`
- [x] Parquet, CSV, JSON readers (via DataFusion)
- [x] SQL parsing, logical plan, 30+ optimizer rules (predicate push-down, projection pruning, etc.)
- [x] Arrow `RecordBatch` columnar execution
- [x] `RddTableProvider` + `RddScanExec` — SQL over a live `TypedRdd<RecordBatch>`
- [x] `UdfRegistry` for `ScalarUDF` / `AggregateUDF` registration
- [x] `DataFrame` lazy result wrapper

### Streaming (`atomic-streaming`)

- [x] `StreamingContext` + `DStreamGraph` + batch loop
- [x] Input DStreams: `QueueInputDStream`, `SocketInputDStream`, `FileInputDStream`
- [x] Transformation DStreams: `MappedDStream`, `FlatMappedDStream`, `FilteredDStream`, `WindowedDStream`
- [x] `ForEachDStream` — primary output operation
- [x] `Checkpoint` serialization (bincode, atomic write)
- [x] `JobScheduler` batch loop thread

### Graph Processing (`atomic-graph`)

- [x] `Graph<VD, ED>` — vertex + edge RDDs
- [x] Pregel bulk-synchronous execution engine
- [x] PageRank
- [x] Shortest path (Dijkstra over Pregel)
- [x] Strongly connected components
- [x] Label propagation (community detection)
- [x] Triangle count
- [x] Connected components

### Language Bindings and Tooling

- [x] Python UDF support (PyO3 / pickle) — executed in worker subprocess
- [x] JavaScript UDF support (V8/deno_core / fn.toString) — executed in worker V8 runtime
- [x] `atomic-py` — Spark-like Python driver API (`parallelize`, `map`, `filter`, `fold`, `collect`, etc.)
- [x] `atomic-js` — Spark-like JavaScript driver API
- [x] `atomic-worker` — polyglot worker binary (native + Python + JS tasks)
- [x] `atomic-cli` — cross-compile (`cargo-zigbuild`, no Docker) + secure SSH/SFTP distribution
  - Host-key verification against `~/.ssh/known_hosts`; SHA-256 integrity check; atomic rename
- [x] Distributed integration test — real driver + worker over TCP

---

## What is NOT Yet Implemented

| Feature | Status |
| --- | --- |
| Distributed shuffle disk spill | Memory-only; OOM risk on large shuffles |
| Failed shuffle-map stage recompute | Detected, not fully requeued |
| LRU eviction for `PartitionStore` | Unbounded in-memory growth |
| `unpersist()` | Cache invalidation API missing |
| `MemoryAndDisk` / `DiskOnly` storage levels | Treated as `MemoryOnly` |
| Broadcast variables | Not implemented |
| Accumulators | Not implemented |
| DAG optimizer | No predicate push-down, pipeline fusion, or partition pruning at RDD level |
| Speculative execution | Not implemented |
| Adaptive query execution | Not implemented |
| Dynamic resource allocation | Not implemented |
| Object store integration (S3/GCS/HDFS) | Not implemented |
| Streaming: distributed mode | Local only; no distributed receiver scheduling |
| Streaming: `updateStateByKey` / `mapWithState` | Not implemented |
| Streaming: shuffle DStream end-to-end | Scaffolded, not wired |
| Streaming: checkpointing wired to batch loop | Checkpoint type exists; not integrated |
| Web dashboard / metrics endpoint | Structured logs only |
| TLS / auth for worker communication | Plain TCP, no encryption |
| PyPI release pipeline | Not set up |
| npm release pipeline | Not set up |

---

## Installation

### Rust (native tasks)

```bash
cargo build --release
```

### Python

```bash
cd crates/atomic-py
pip install maturin
maturin develop --release
```

### JavaScript / Node.js

```bash
cargo build --release -p atomic-js
const { Context } = require('./crates/atomic-js');
```

### Worker binary

```bash
cargo build --release -p atomic-worker
RUST_LOG=info ./target/release/atomic-worker --worker --port 10001
```

### Cross-compile and ship to remote workers

```bash
# Install atomic-cli
cargo install --path crates/atomic-cli

# Build for Linux musl target and ship via SSH
atomic build --target x86_64-unknown-linux-musl
atomic ship --workers user@host1,user@host2
```

---

## Running Examples

```bash
# Local word count
cargo run --example task_wordcount

# Pi estimation (Monte Carlo)
cargo run --example pi

# Distributed sort
cargo run --example sort

# Group-by
cargo run --example group_by
```

---

## Design Notes

- Distributed tasks reference pre-registered functions by ID via `#[task]` + `TASK_REGISTRY`. Workers cannot receive arbitrary Rust code at runtime — only data payloads and op IDs. This is intentional: it keeps the execution model auditable and the worker surface area small.
- Python/JS UDFs are the explicit escape hatch for dynamic code. They go through a clearly bounded path (`PythonUdf` / `JavaScriptUdf` actions) with their own serialization format.
- `rkyv` is used for the Rust native path; JSON is used for the Python/JS path.
- `atomic-py` is a `cdylib` — it must be built with `maturin`, not `cargo build`. `atomic-worker` must be excluded from `cargo test --workspace` because it activates PyO3's `auto-initialize` feature.
- DataFusion is the SQL IR backbone. `atomic-sql` uses `Extension(UserDefinedLogicalNode)` to add RDD-backed scan operators without leaving the DataFusion ecosystem.

---

## Benchmarks and Comparison with Spark

Atomic has not been formally benchmarked against Spark. Expected tradeoffs:

| Dimension | Spark | Atomic |
| --- | --- | --- |
| JVM startup | Slow (seconds) | N/A — native |
| Worker startup | Slow (JVM) | Fast (milliseconds) |
| Per-partition memory | GC pressure + off-heap tricks | Rust ownership + rkyv zero-copy |
| CPU-bound transforms | JIT can close gaps | Native speed; no JIT overhead |
| DAG optimizer | Mature (Catalyst) | Basic (DataFusion for SQL; RDD layer unoptimized) |
| Ecosystem | Very large | Early stage |

Atomic is likely faster for small-to-medium CPU-bound jobs. Spark has the advantage for very large jobs where its DAG optimizer, speculative execution, and dynamic resource management offset startup costs.

---

## License

Licensed under the [Apache 2.0 License](LICENSE).
