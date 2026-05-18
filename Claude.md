# Atomic Project Notes

## Project Goal

Atomic is a stable-Rust rewrite and refactor of Vega.

- Preserve the useful high-level execution model from Vega (RDD DAG, lazy transforms, shuffle).
- Remove dependence on unstable Rust features and closure serialization.
- Replace trait-object function shipping with explicit compile-time task registration.
- Use rkyv for distributed wire payloads.

## Repository Structure

- `crates/atomic-data`: shared types — RDD traits, task envelopes, distributed structs, shuffle primitives, dependency DAG, partition cache.
- `crates/atomic-compute`: execution runtime — context, executor, `NativeBackend`, UDF dispatch, RDD implementations, persist/cache layer.
- `crates/atomic-scheduler`: DAG building, stage planning, job tracking, `LocalScheduler`, `DistributedScheduler`.
- `crates/atomic-sql`: structured data and SQL query layer — built on DataFusion (see below).
- `crates/atomic-streaming`: Spark Streaming–style micro-batch streaming on top of `atomic-compute`.
- `crates/atomic-cli`: cross-compilation and secure binary distribution to remote workers.
- `crates/atomic-utils`: shared utilities.
- `notes/`: architecture notes and design documents.

## RDD API Convention

### The `_task` family — canonical API for local and distributed execution

All narrow transforms and reductions have a `_task` variant. Always use these:

| Method | Trait required | TaskAction |
| --- | --- | --- |
| `rdd.map_task(F)` | `UnaryTask<T, U>` | `Map` |
| `rdd.filter_task(F)` | `UnaryTask<T, bool>` | `Filter` |
| `rdd.flat_map_task(F)` | `UnaryTask<T, Vec<U>>` | `FlatMap` |
| `rdd.fold_task(zero, F)` | `BinaryTask<T>` | `Fold` |
| `rdd.reduce_task(F)` | `BinaryTask<T>` | `Reduce` |

In **local mode**: execute in-process (same result as closure variants).
In **distributed mode**: accumulate lazily into a `StagedPipeline`; the full op chain is dispatched as one `TaskEnvelope` per partition when an action fires.

Closure-based variants (`map`, `filter`, `flat_map`, `reduce`, `fold`) are **deprecated** — they always run on the driver's local scheduler and cannot dispatch to workers.

### Two equivalent ways to register a task function

```rust
// Named — preferred for reuse across pipeline stages
#[task]
fn double(x: i32) -> i32 { x * 2 }
rdd.map_task(Double)         // #[task] generates PascalCase struct

// Inline — for one-off lambdas (op_id is a content hash of the closure tokens)
rdd.map_task(task_fn!(|x: i32| -> i32 { x * 2 }))
```

`task_fn!` generates a zero-sized struct with a **content-hash op_id** (`task_fn::<fnv1a-hex>`)
registered via `inventory::submit!` — identical to `#[task]` at the dispatch level. The hash is
derived from the closure's normalized token text, so it is stable across line-number shifts and
reformatting. It changes only when the closure body changes, which is the correct behavior.

### How the staged pipeline works

```text
parallelize_typed(data, n)
  └─ flat_map_task(Tokenize)  → encodes partitions → StagedPipeline{source, ops:[FlatMap]}
       └─ map_task(PairOne)   → appends op          → StagedPipeline{source, ops:[FlatMap,Map]}
            └─ collect()      → dispatches to workers → results collected on driver
```

All action methods (`collect`, `count`, `take`, `fold_task`, `reduce_task`, …) check `self.staged`. When a pipeline is staged, they dispatch the full op chain to workers and aggregate the results on the driver.

### Entry point for distributed programs

Build a `Config` at the entry point and pass it to `Context::new_with_config()`. For programs with both worker and driver modes, use `AtomicApp::build()`:

```rust
let app = AtomicApp::build().await?;   // parses --worker / --workers / --local-ip
let ctx = app.driver_context()?;
```

Workers are started with the same binary: `./my_app --worker --port 10001`.

## Architecture Rules

### Serialization

- Use rkyv for distributed wire payloads.
- Do not reintroduce generic closure serialization for distributed execution.
- Do not depend on Cap'n Proto or Vega-style serializable function wrappers.

### Execution Model

- Local mode runs work on threads in-process via `LocalScheduler`.
- Distributed mode dispatches task envelopes to workers via `DistributedScheduler` + `NativeBackend`.
- Tasks are registered at compile time with the `#[task]` macro and dispatched by string ID.
- Driver and workers run the **same binary** — the dispatch table is linked at compile time.
- Workers are configured via environment variables, not runtime probing.

### The Only Backend: NativeBackend

There is one execution backend: `NativeBackend`. It looks up each `op_id` in the compile-time `TASK_REGISTRY` and calls the registered handler. Docker and WASM backends are not part of this project.

### Shuffle

- `reduce_by_key` and `group_by_key` are **lazy** — they build a `ShuffleDependency + ShuffledRdd` and return.
- Execution is triggered by actions (`collect`, `count`, etc.).
- The scheduler splits the DAG at shuffle boundaries into map and reduce stages.
- Shuffle data is stored in `DashMapShuffleCache` and served via the `ShuffleManager` HTTP server.
- `ShuffleFetcher` reads from workers over HTTP.

## Distributed Contract

Distributed tasks use types from `atomic_data::distributed`.

- `TaskEnvelope`: rkyv-encoded metadata + `Vec<PipelineOp>` pipeline + input partition bytes.
- `TaskResultEnvelope`: rkyv-encoded result or failure, with `partition_id` for ordering.
- `WorkerCapabilities`: advertised per-worker limits (`max_tasks`); scheduler skips workers at capacity.

## Current Implementation State

### Done

- `#[task]` proc-macro + `TASK_REGISTRY` compile-time dispatch.
- `task_fn!` macro for inline task lambdas — identical to `#[task]` at dispatch level.
- `NativeBackend` — executes task pipelines by op_id lookup.
- `LocalScheduler` — full DAG/stage/shuffle support, thread-pool execution.
- `DistributedScheduler` — TCP dispatch, capacity-aware placement.
- rkyv distributed envelope types (`TaskEnvelope`, `TaskResultEnvelope`, `WorkerCapabilities`).
- Lazy shuffle pipeline (`ShuffleDependency` + `ShuffledRdd` + `Aggregator`).
- `DashMapShuffleCache` + `ShuffleManager` HTTP server.
- `ShuffleFetcher` + `MapOutputTracker`.
- `partition_id` in `TaskResultEnvelope` for correct result ordering after retries.
- Python UDF support (PyO3 / pickle) and JavaScript UDF support (V8 / deno_core / fn.toString).
- Unified `_task` API: `map_task`, `filter_task`, `flat_map_task`, `fold_task`, `reduce_task` — work identically in local and distributed mode.
- All action methods (`collect`, `count`, `take`, `first`, `reduce`, `fold`, `aggregate`, `for_each`, `for_each_partition`, `count_by_value`, `is_empty`, `top`, `take_ordered`, `max`, `min`) dispatch staged pipelines to workers in distributed mode.
- `AtomicApp::build()` — unified entry point; reads `--worker`/`--workers`/`--local-ip` from CLI.
- Explicit `Config` struct at entry point — replaces global `OnceCell<Configuration>` env-var reading.
- Distributed integration test (driver + real worker over TCP, `cargo test -p atomic-tests test_distributed`).
- **RDD persist/cache**: `TypedRdd::cache()` / `TypedRdd::persist(StorageLevel)` — partitions stored in global `PARTITION_CACHE` (`PartitionStore`) as typed `Arc<Vec<T>>`; no serialization required; subsequent actions hit the store instead of recomputing the DAG.
- **`atomic-cli`**: cross-compilation with `cargo-zigbuild` (no Docker); secure SSH/SFTP binary distribution via `russh`; host-key verification against `~/.ssh/known_hosts`; SHA-256 integrity check on remote; atomic rename; no credentials in process list.
- **`atomic-streaming` Phase 4**: `MappedDStream`, `FlatMappedDStream`, `FilteredDStream`, `WindowedDStream` all implement `compute()` using `MapperRdd`, `FlatMapperRdd`, and `UnionRdd`.

### Not Done Yet

- Distributed shuffle end-to-end: each worker needs to run its own `ShuffleManager` and register its URI with the driver's `MapOutputTracker`.
- `ShuffleFetcher` retry on transient network errors.
- Failed shuffle-map stage recompute / fault recovery.
- `CacheTracker` distributed protocol — locality-aware scheduling using cached partition locations (deferred; local cache works correctly without it).
- LRU eviction for `PartitionStore` — currently unbounded in-memory growth.
- `unpersist()` — explicit cache invalidation API.
- Windowed reduce-by-key / `updateStateByKey` / `mapWithState` in streaming.

## atomic-sql Architecture

### Query Engine: DataFusion

`atomic-sql` uses [Apache DataFusion](https://github.com/apache/datafusion) (`datafusion = "53"`)
as its SQL query engine.  DataFusion provides:

- SQL parsing (sqlparser-rs, bundled)
- Logical plan + 30+ optimizer rules (predicate push-down, projection pruning, etc.)
- Physical plan operators (hash join, sort-merge join, aggregation, exchange, etc.)
- Apache Arrow `RecordBatch` columnar execution
- Built-in Parquet, CSV, JSON readers

`atomic-sql` adds an integration layer on top:

| Type | File | Role |
| --- | --- | --- |
| `AtomicSqlContext` | `context.rs` | Primary entry point — wraps DataFusion `SessionContext` |
| `DataFrame` | `dataframe.rs` | Lazy result — wraps DataFusion `DataFrame` |
| `AtomicTableProvider` | `table.rs` | `TableProvider` backed by pre-loaded `Vec<Vec<RecordBatch>>` |
| `AtomicScanExec` | `exec_plan.rs` | `ExecutionPlan` leaf that streams pre-loaded batches |
| `RddTableProvider` | `rdd_table.rs` | `TableProvider` backed by a live `Arc<dyn Rdd<Item=RecordBatch>>` |
| `RddScanExec` | `rdd_table.rs` | `ExecutionPlan` leaf that runs one RDD partition via atomic-compute |
| `UdfRegistry` | `udf.rs` | Helper for registering `ScalarUDF` / `AggregateUDF` |

### Row Format

Arrow `RecordBatch` is the columnar format throughout `atomic-sql`.

### Entry Point — Standalone

```rust
use atomic_sql::AtomicSqlContext;

let ctx = AtomicSqlContext::new();
ctx.register_parquet("orders", "data/orders.parquet", Default::default()).await?;
let df = ctx.sql("SELECT customer_id, SUM(amount) FROM orders GROUP BY 1").await?;
df.show().await?;
```

### Entry Point — With atomic-compute (RDD-backed)

Use `AtomicSqlContext::with_compute(sc)` to register a `TypedRdd<RecordBatch>` as
a SQL table.  Each RDD partition is materialized in parallel by atomic's scheduler;
DataFusion then applies filter/project/aggregate/join on the returned Arrow batches.

```rust
let sc = Arc::new(Context::new_with_config(Config::local())?);
let rdd = sc.parallelize_typed(batches, num_partitions);

let ctx = AtomicSqlContext::with_compute(Arc::clone(&sc));
ctx.register_rdd("events", rdd)?;          // schema inferred from first batch
let df = ctx.sql("SELECT user_id, COUNT(*) FROM events GROUP BY 1").await?;
df.show().await?;
```

Data flow:

```text
TypedRdd<RecordBatch>  ──register_rdd()──►  RddTableProvider
DataFusion PhysicalPlan (leaf = RddScanExec)
  ├─ execute(partition=0) ── atomic-compute scheduler ──► thread/worker
  ├─ execute(partition=1) ── atomic-compute scheduler ──► thread/worker
  └─ ...
DataFusion: Filter / Project / Aggregate / Join  →  DataFrame.collect()
```

### What Is NOT in atomic-sql

- Streaming SQL — deferred; use DataFusion's streaming APIs directly if needed.
- Custom optimizer rules — DataFusion's built-in rules handle all rewrites.
- Custom physical operators — DataFusion provides hash join, agg, sort, etc.
- The old Catalyst-inspired code (analyzer, optimizer, joins, columnar, commands)
  was entirely replaced by DataFusion.

---

## RDD Persist / Cache

`TypedRdd::cache()` and `TypedRdd::persist(level)` wrap the target RDD in a `CachedRdd<T>`.

### How it works

```text
rdd.map_task(Double).cache()
       ↓
TypedRdd<T> backed by CachedRdd { inner: MapperRdd { inner: ParallelCollection } }

action1: rdd.collect()
  CachedRdd::compute(p0) → miss → MapperRdd computes → stores Arc<Vec<T>> → returns
  CachedRdd::compute(p1) → miss → MapperRdd computes → stores Arc<Vec<T>> → returns

action2: rdd.count()
  CachedRdd::compute(p0) → HIT  → returns from PartitionStore (no recompute)
  CachedRdd::compute(p1) → HIT  → returns from PartitionStore (no recompute)
```

### Key types

| Type | File | Role |
| --- | --- | --- |
| `CachedRdd<T>` | `rdd/cached.rs` | RDD wrapper that intercepts `compute()` to check/fill the cache |
| `PartitionStore` | `cache/mod.rs` | `DashMap<(rdd_id, partition), Box<dyn Any+Send+Sync>>` — type-erased, no serialization |
| `PARTITION_CACHE` | `cache/mod.rs` | Global `OnceLock<PartitionStore>` initialized at `Context` startup |
| `StorageLevel` | `cache/mod.rs` | `MemoryOnly` (default), `MemoryAndDisk`, `MemoryOnlySer`, `DiskOnly` — only `MemoryOnly` implemented |

### Constraints

- `T` needs only `Data + Clone + 'static` — no serialization bounds.
- `CachedRdd` IDs come from a module-level `NEXT_CACHED_ID: AtomicUsize` starting at `0x7000_0000` — globally unique across all `Context` instances.
- `PARTITION_CACHE` is a process-wide singleton; multiple `Context` instances in one process share it (cache keys include RDD id, which is globally unique).

---

## atomic-cli Architecture

`crates/atomic-cli` cross-compiles the project and ships the binary to remote workers over secure SSH/SFTP.

### Commands

| Command | Description |
| --- | --- |
| `atomic build` | Cross-compile with `cargo-zigbuild` (auto-installed if absent); default target `x86_64-unknown-linux-musl` |
| `atomic ship` | SFTP-upload binary to workers; host-key verify → SHA-256 check → atomic rename |
| `atomic submit` | `build` + `ship` + run driver locally |
| `atomic stop` | Send graceful shutdown frame over TCP to each worker |

### Security guarantees

- Host keys verified against `~/.ssh/known_hosts`; unknown hosts refused (no `StrictHostKeyChecking=no`).
- Host-key mismatch raises a hard error with MITM warning.
- Binary uploaded to `<path>.tmp`; SHA-256 verified remotely; only then renamed to final path (atomic on POSIX).
- SSH private key never appears in a shell command or process argument list.
- Uses `russh 0.60` (pure Rust SSH) + `russh-sftp` — no `scp` subprocess, no shell injection surface.

### Worker lifecycle

`atomic-cli` is responsible for **distribution only** — it does not start or stop worker processes. Worker lifecycle (systemd, SSH, etc.) is managed separately.

---

## Framework Design Vision: PoC → Production Workflow

Atomic is designed around a progressive adoption model: start fast in TypeScript or Python, then rewrite hot paths in Rust for production. The same binary serves as both driver and worker.

### 1. Start fast, scale later

Users build quick prototypes using the same Spark-like API in TypeScript (`atomic-js`) or Python (`atomic-py`). When a job is confirmed correct and needs production throughput, CPU-hot paths are
rewritten as Rust `#[task]` functions in `atomic-compute`.

**TypeScript (PoC):**

```typescript
const ctx = new Context();
const result = ctx.parallelize(data, 4)
  .map(x => x * 2)
  .filter(x => x > threshold)
  .collect();
```

**Rust (production rewrite of the hot path):**

```rust
#[task]
fn double_and_filter(x: i32) -> Option<i32> {
    let v = x * 2;
    if v > THRESHOLD { Some(v) } else { None }
}
ctx.parallelize_typed(data, 4).flat_map_task(DoubleAndFilter).collect()?;
```

The driver-side TypeScript/Python script does not change — only the registered `#[task]` functions
in the Rust binary change.

### 2. Same binary = driver + worker

`AtomicApp::build()` reads `--worker`/`--workers`/`--local-ip` from CLI flags at startup.

- Run normally: the binary acts as the **driver** and coordinates the job.
- Run with `--worker --port N`: the **same binary** acts as a worker and waits for task envelopes.

No separate worker binary or daemon is needed. Driver and workers run identical code; the dispatch
table (`TASK_REGISTRY`) is the same in both modes because it is built at compile time.

### 3. Extending the worker with new `#[task]` functions

When Rust `#[task]` functions are added or changed:

1. Recompile: `cargo build --release`
2. Redeploy: `atomic ship --workers user@host1,user@host2`

The `atomic-cli` `ship` command uploads the new binary via SFTP with host-key verification and
SHA-256 integrity check. Worker nodes are restarted with the new binary by the operator
(systemd reload, SSH, etc.) — `atomic-cli` handles distribution only.

The driver-side script (TS or Python) does not need to change — it references the same op IDs.

### 4. `atomic-cli` ships the binary

```bash
atomic build --target x86_64-unknown-linux-musl   # cross-compile via cargo-zigbuild
atomic ship --workers user@host1,user@host2        # SFTP upload + SHA-256 verify
```

Each remote worker node receives the same binary. The binary embeds both the Rust task registry
and the PyO3/V8 runtimes for Python and JS UDFs — no separate runtime installation needed.

### 5. UDFs are the dynamic escape hatch

Python and JavaScript UDFs bypass binary redeployment:

- Python lambdas are `pickle.dumps()`-serialized on the driver and shipped inside `TaskEnvelope`.
- JavaScript functions are shipped as source strings and evaluated by the embedded V8 runtime.

Use `#[task]` for static-typed Rust hot paths; use UDFs for rapidly-iterating or exploratory logic.

| Change type | Requires binary redeployment? |
| --- | --- |
| Modify a Python UDF lambda | **No** — pickled and sent in TaskEnvelope |
| Modify a JavaScript UDF function | **No** — source string sent in TaskEnvelope |
| Modify a Rust `#[task]` function | **Yes** — compiled into the binary |
| Add new scheduler/shuffle infrastructure | **Yes** — compiled into the binary |

---

## Guardrails For Future Changes

- Reuse Vega's DAG, stage, and shuffle ideas when they fit.
- Do not copy Vega's closure serialization model.
- Do not add Docker or WASM backends — the project uses `NativeBackend` only.
- Keep local and distributed execution paths conceptually aligned.
- `#[task]` functions are the only way to register distributed work.
- Prefer explicit backend routing and compile-time dispatch over dynamic runtime guessing.
- The JS (`atomic-js`) and Python (`atomic-py`) APIs must mirror Rust `TypedRdd` — keep them in
  parity when adding new RDD operations. The three APIs serve the PoC→Production workflow.

---

## atomic-streaming Architecture

`crates/atomic-streaming` implements Spark Streaming–style micro-batch streaming on top of `atomic-compute`.

### Streaming Entry Point

```rust
let ctx = Context::local()?;
let ssc = StreamingContext::new(ctx, Duration::from_secs(1));
let queue = Arc::new(Mutex::new(VecDeque::new()));
let stream = ssc.queue_stream(queue.clone(), true);
ssc.foreach_rdd(stream, |rdd, time_ms| { /* process rdd */ });
ssc.start()?;
ssc.await_termination()?;
```

### Key Types

| Type | File | Role |
| --- | --- | --- |
| `StreamingContext` | `context.rs` | Entry point — wraps `Arc<Context>` + `DStreamGraph` + batch loop |
| `DStreamGraph` | `dstream/mod.rs` | DAG of input streams + output operations |
| `DStreamBase` | `dstream/mod.rs` | Untyped, object-safe base for all DStreams |
| `DStream<T>` | `dstream/mod.rs` | Typed DStream — `compute(time_ms) -> Option<Arc<dyn Rdd<Item=T>>>` |
| `OutputOperation` | `dstream/mod.rs` | Trait for output ops — `generate_job(time_ms) -> Option<StreamingJob>` |
| `StreamingJob` | `dstream/mod.rs` | A single runnable batch job (time + closure) |
| `JobScheduler` | `scheduler/job.rs` | Drives the batch loop thread |
| `ForEachDStream<T, F>` | `dstream/mapped.rs` | The primary output operation |
| `QueueInputDStream<T>` | `dstream/input.rs` | In-memory queue of RDDs (used for testing) |
| `SocketInputDStream` | `dstream/input.rs` | TCP socket reader (line-by-line) |
| `FileInputDStream` | `dstream/input.rs` | Watches a local directory for new text files |
| `Checkpoint` | `checkpoint.rs` | Serializable checkpoint state (bincode-encoded) |

### Batch Loop

A dedicated `std::thread` in `JobScheduler` fires every `batch_duration`:

```text
JobScheduler::run_batch_loop()
  ├─ DStreamGraph::start(zero_time_ms)   — starts input streams
  └─ loop every batch_ms:
       ├─ DStreamGraph::generate_jobs(batch_time_ms)
       │    └─ OutputOperation::generate_job(time_ms) for each output op
       │         └─ DStream::get_or_compute(time_ms)   — lazy, cached per batch
       │              └─ DStream::compute(time_ms)     — builds RDD DAG
       └─ StreamingJob::run()            — executes via Arc<Context>::run_job()
```

### DStream Trait Contract

`DStream<T>::compute(valid_time_ms)` is called at most once per batch time per DStream. Results are cached in a `Mutex<HashMap<u64, Arc<dyn Rdd<Item=T>>>>` stored on each DStream. The RDD returned is a fresh lazy DAG that gets executed by `Context::run_job()` when the output operation fires.

### Input DStreams

- **`QueueInputDStream<T>`** — pops RDDs from a `VecDeque`. No background threads. Primary tool for testing.
- **`SocketInputDStream`** — `start()` spawns a thread that reads lines from TCP. `compute()` drains the buffer into a `ParallelCollection` RDD.
- **`FileInputDStream`** — `compute()` uses `std::fs::read_dir()` to find files modified in the current batch window. Tracks seen files to avoid re-reading. Returns lines as `ParallelCollection<String>`.

### Transformation DStreams

`MappedDStream`, `FlatMappedDStream`, `FilteredDStream`, `WindowedDStream` are fully implemented.

| DStream | `compute()` implementation |
| --- | --- |
| `MappedDStream<T,U,F>` | Wraps parent RDD with `MapperRdd::new(id, parent_rdd, f)` |
| `FlatMappedDStream<T,U,F>` | Wraps parent RDD with `FlatMapperRdd::new(id, parent_rdd, f)` — adapts `Fn(T)->Vec<U>` to `Fn(T)->Box<dyn Iterator<Item=U>>` |
| `FilteredDStream<T,F>` | Uses `FlatMapperRdd` as a filter: passing elements emit `once(x)`, others emit `empty()` |
| `WindowedDStream<T>` | Calls `parent.get_or_compute(t)` for each `t` in the window, then unions via `UnionRdd::new` |

RDD IDs for streaming-created RDDs use a module-level `static AtomicUsize` starting at `0x5000_0000` (mapped) and `0x6000_0000` (windowed) to avoid collisions with compute-layer RDD IDs.

### Checkpointing

`Checkpoint` is serialized with `bincode` v2 and written atomically (write to `.tmp`, then `rename()`). `Checkpoint::read_latest()` finds the most recent `checkpoint-<ms>` file. Checkpointing is wired to the batch loop in Phase 5 (TODO).

### Streaming Features Not Yet Implemented

- No Hadoop, no `blas`, no `hdrs`, no `atomic-sql` dependency.
- No distributed receiver scheduling (`ReceiverTracker` is a local stub).
- No dynamic executor allocation (`ExecutorAllocationManager` is a stub).
- No windowed reduce-by-key, `updateStateByKey`, or `mapWithState` (all deferred to Phase 4).
- No distributed streaming mode — local mode uses the same `Arc<Context>` as `atomic-compute`.

---

## Planned Features

### atomic-nlq — Natural Language Query Layer (LLM-first analytics)

The long-term direction is a general-purpose analytics platform where users express intent
in natural language and the system compiles it to an execution plan — without routing through
SQL as an intermediate representation.

**DataFusion is the IR backbone.**  DataFusion's `LogicalPlan` with `Extension` nodes *is* the
IR.  Relational operators (Scan, Filter, Aggregate, Join, …) use DataFusion's built-in plan
nodes directly.  Novel LLM-specific operators (`LlmMap`, `LlmFilter`, `Embed`, `VectorSearch`)
are DataFusion `Extension(UserDefinedLogicalNode)` nodes — the same pattern already used by
`RddScanExec` in `atomic-sql`.  No custom AST enum is needed.

#### Full pipeline

```text
User: "find customers who bought luxury items and estimate lifetime value"
         │
         ▼  LlmPlanner (Anthropic API: schema + UDF list + NL query)
  Structured JSON plan (LLM output)
         │
         ▼  IrParser
  DataFusion LogicalPlan tree
    Aggregate {
      input: Extension(LlmFilterNode { prompt: "is this a luxury item?", col: "category" })
        input: TableScan("orders")
    }
         │
         ▼  DataFusion analyzer  (schema resolution, type checking — built-in)
         ▼  DataFusion optimizer (predicate push-down, projection pruning, … — built-in)
         ▼  LlmBatchingRule      (custom: groups per-row LLM calls into batched API calls)
         │
         ▼  Physical planner
  PhysicalPlan
    ├── RddScanExec          (already built in atomic-sql)
    └── LlmFilterExec        (new: per-partition Anthropic API calls → filtered RecordBatches)
         │
         ▼
  atomic-compute RDD DAG → LocalScheduler / DistributedScheduler → workers
```

#### Why DataFusion instead of a custom IR enum

| Concern | DataFusion approach |
| --- | --- |
| Relational nodes (Scan, Filter, Join, …) | Built-in `LogicalPlan` variants — zero code |
| Novel nodes (LlmMap, Embed, VectorSearch) | `Extension(Arc<dyn UserDefinedLogicalNode>)` — one struct per operator |
| Schema resolution + type checking | DataFusion analyzer passes — built-in |
| 30+ optimizer rules (predicate push-down, etc.) | Built-in, run automatically |
| Custom optimization (LLM call batching) | `OptimizerRule` trait — one impl per custom pass |
| UDFs / scalar functions | `ScalarUDF` + DataFusion `FunctionRegistry` — already wired in `atomic-sql` |
| Arrow columnar execution | `ExecutionPlan` + `RecordBatch` — already used by `RddScanExec` |

#### Novel plan nodes (each requires two impls)

```text
LlmMap(prompt_template, model, col)   ← transform each row with LLM
LlmFilter(prompt_template, model)     ← boolean predicate via LLM
Embed(col, model)                     ← produce vector embedding column
VectorSearch(query_col, index, k)     ← ANN / similarity join
```

Each node follows the pattern established by `RddScanExec`:

```rust
// 1. Logical node — used by LlmPlanner + optimizer
struct LlmFilterNode { prompt: String, model: String, input: LogicalPlan }
impl UserDefinedLogicalNode for LlmFilterNode { ... }

// 2. Physical node — called by DataFusion executor per partition
struct LlmFilterExec { ... }
impl ExecutionPlan for LlmFilterExec {
    fn execute(&self, partition, ctx) -> SendableRecordBatchStream {
        // call Anthropic API on this partition's RecordBatch → return filtered batches
    }
}
```

#### UDF Integration

UDFs are registered in DataFusion's `FunctionRegistry` (already used in `atomic-sql` via
`UdfRegistry`) plus a companion `description` field the LLM planner exposes as available tools.

```rust
// planned API
registry.register_scalar("is_luxury",    "Returns true if the category is a luxury item",
    |category: &str| -> bool { ... });
registry.register_scalar("estimate_ltv", "Estimates customer lifetime value from customer_id",
    |customer_id: i64| -> f64 { /* ML model */ });
```

The LLM planner system prompt includes:

- Schema (tables, columns, Arrow types)
- UDF list (name + description + signature) — same mechanism as Anthropic tool definitions
- Instruction to produce a JSON plan tree using DataFusion node names + extension node names

The LLM embeds `Call("is_luxury", col("category"))` nodes; DataFusion resolves these as
registered `ScalarUDF` calls, identical to how SQL `SELECT is_luxury(category)` would work —
but without going through SQL.

#### Planned crate: `crates/atomic-nlq`

| Component | Role | DataFusion mapping |
| --- | --- | --- |
| `NlqContext` | Entry point — wraps `AtomicSqlContext` + Anthropic client + `NlqRegistry` | Orchestrator |
| `LlmPlanner` | Calls Anthropic API with schema + UDF list + NL query → JSON plan | Produces input for `IrParser` |
| `IrParser` | Deserializes JSON plan into `LogicalPlan` with extension nodes | Replaces custom `IrNode` enum |
| `LlmFilterNode` / `LlmMapNode` / `EmbedNode` / `VectorSearchNode` | Custom logical nodes | `UserDefinedLogicalNode` impls |
| `LlmFilterExec` / `LlmMapExec` / `EmbedExec` / `VectorSearchExec` | Per-partition physical execution | `ExecutionPlan` impls |
| `LlmBatchingRule` | Groups per-row LLM calls into batched API requests | `OptimizerRule` impl |
| `NlqRegistry` | UDF name → description + signature + DataFusion `ScalarUDF` | Extends DataFusion `FunctionRegistry` |

#### Guardrails for this feature

- Use DataFusion's `LogicalPlan` directly — do not define a parallel AST enum.
- The LLM never produces SQL; it produces a structured JSON tree that `IrParser` converts to `LogicalPlan`.
- Schema resolution and basic type checking are handled by DataFusion's analyzer (built-in) — only extension-node–specific validation is custom.
- `LlmBatchingRule` must run before the physical planner to avoid one API call per row.
- UDF implementations may be Rust closures, Python (PyO3), or JavaScript (V8/deno_core) — matching the existing UDF dispatch model in `atomic-compute`.
- `NlqContext::query(nl)` is the only public entry point; internal plan construction is not exposed.
