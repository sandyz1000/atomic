# Atomic Project Notes

## Project Goal

Atomic is a stable-Rust rewrite and refactor of Vega.

- Preserve the useful high-level execution model from Vega (RDD DAG, lazy transforms, shuffle).
- Remove dependence on unstable Rust features and closure serialization.
- Replace trait-object function shipping with explicit compile-time task registration.
- Use rkyv for distributed wire payloads.

## Repository Structure

- `crates/atomic-data`: shared types — RDD traits, task envelopes, distributed structs, shuffle primitives, dependency DAG.
- `crates/atomic-compute`: execution runtime — context, executor, `NativeBackend`, UDF dispatch, RDD implementations.
- `crates/atomic-scheduler`: DAG building, stage planning, job tracking, `LocalScheduler`, `DistributedScheduler`.
- `crates/atomic-sql`: structured data and SQL query layer — built on DataFusion (see below).
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

// Inline — preferred for one-off lambdas
rdd.map_task(task_fn!(|x: i32| -> i32 { x * 2 }))
```

`task_fn!` generates a zero-sized struct with a stable `file!:line!:col!` op_id registered via `inventory::submit!` — identical to `#[task]` at the dispatch level.

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
- Python UDF support (PyO3 / pickle) and JavaScript UDF support (QuickJS).
- Unified `_task` API: `map_task`, `filter_task`, `flat_map_task`, `fold_task`, `reduce_task` — work identically in local and distributed mode.
- All action methods (`collect`, `count`, `take`, `first`, `reduce`, `fold`, `aggregate`, `for_each`, `for_each_partition`, `count_by_value`, `is_empty`, `top`, `take_ordered`, `max`, `min`) dispatch staged pipelines to workers in distributed mode.
- `AtomicApp::build()` — unified entry point; reads `--worker`/`--workers`/`--local-ip` from CLI.
- Explicit `Config` struct at entry point — replaces global `OnceCell<Configuration>` env-var reading.
- Distributed integration test (driver + real worker over TCP, `cargo test -p atomic-tests test_distributed`).

### Not Done Yet

- Distributed shuffle end-to-end: each worker needs to run its own `ShuffleManager` and register its URI with the driver's `MapOutputTracker`.
- `ShuffleFetcher` retry on transient network errors.
- Failed shuffle-map stage recompute / fault recovery.

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

## Guardrails For Future Changes

- Reuse Vega's DAG, stage, and shuffle ideas when they fit.
- Do not copy Vega's closure serialization model.
- Do not add Docker or WASM backends — the project uses `NativeBackend` only.
- Keep local and distributed execution paths conceptually aligned.
- `#[task]` functions are the only way to register distributed work.
- Prefer explicit backend routing and compile-time dispatch over dynamic runtime guessing.

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
