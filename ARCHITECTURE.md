# Atomic — Architecture

## Summary

Atomic is a distributed data-processing framework for stable Rust, inspired by Apache Spark and
written as a redesign of [Vega](https://github.com/rajasekarv/vega). It keeps Vega's core ideas
(RDD DAG, lazy transforms, shuffle) but replaces every mechanism that required nightly Rust or
fragile runtime tricks, and adds a full SQL layer, micro-batch streaming, and graph processing.

---

## What Vega Got Right

Vega proved that Rust could express a Spark-like RDD model cleanly:
- Lazy transformation chains building a DAG of narrow and shuffle dependencies.
- Two-phase scheduling — shuffle-map stage, then result stage.
- Partition-level parallelism with a local fallback.

Atomic preserves all of this.

---

## What Vega Got Wrong (and How Atomic Fixes It)

### 1. Nightly-only closure serialization

**Vega** shipped closures to workers by serializing Rust function pointers — only possible via
unstable nightly intrinsics. This made Vega dependent on a specific nightly toolchain.

**Atomic** replaces closure serialization entirely. Tasks are plain Rust functions annotated with
`#[task]`. At link time, each function submits a `TaskEntry` via `inventory::submit!`. Workers
build a `HashMap<op_id, handler>` at startup and dispatch by string lookup. No closures are ever
serialized; only the `op_id` string and the rkyv-encoded partition data travel over the wire.

```
Driver:  encode(op_id="myapp::double", data=partition_bytes)  →  TCP  →  Worker
Worker:  TASK_REGISTRY.get("myapp::double")(action, payload, data)
```

Driver and worker run **the same binary**. The dispatch table is built at compile time.

### 2. Unsafe / ad-hoc wire protocol

**Vega** used `serde` with dynamically selected codecs.

**Atomic** uses `rkyv` for all native wire payloads — zero-copy on the read side,
`bytecheck`-validated before use.

### 3. Global mutable configuration via environment variables

**Vega** stored driver/worker configuration in process-global `OnceCell` variables.

**Atomic** uses an explicit `Config` struct built at the entry point and passed to
`Context::new_with_config()`. Environment variables still work for legacy use.

### 4. No UDF story for non-Rust developers

**Vega** had no mechanism for dynamic/non-Rust tasks.

**Atomic** adds first-class Python and JavaScript UDF support:

- Python UDFs are serialized with `pickle` and executed in an embedded PyO3 runtime.
- JavaScript UDFs are sent as source strings and evaluated by an embedded V8 runtime (deno_core, one `JsRuntime` per worker thread).
- UDFs follow the same `PipelineOp` dispatch path as native tasks.

### 5. No SQL, streaming, or graph layers

**Vega** was RDD-only.

**Atomic** adds:

- **`atomic-sql`** — full SQL layer backed by DataFusion (SQL parser, Catalyst-style optimizer,
  Arrow columnar execution, Parquet/CSV/JSON, RDD-backed table providers).
- **`atomic-streaming`** — Spark Streaming–style micro-batch streaming over `atomic-compute`.
- **`atomic-graph`** — GraphX-style graph processing with Pregel and common algorithms.

---

## Core Architecture

```
Driver
  Context::parallelize_typed(data)
    └─ flat_map_task(F)     ┐
    └─ map_task(G)          │  StagedPipeline{source_partitions, ops:[FlatMap, Map]}
    └─ collect()            ┘  → dispatch_pipeline() → TCP → Worker

Worker
  NativeBackend::execute(TaskEnvelope)
    └─ for each op in pipeline:
        TASK_REGISTRY.get(op_id)(action, payload, data) → out_data
    └─ return TaskResultEnvelope{data: out_data, partition_id}
```

### Key types

| Type | Location | Role |
|------|----------|------|
| `Context` | `atomic-compute` | Driver entry point; builds RDDs, dispatches jobs |
| `TypedRdd<T>` | `atomic-compute` | Type-safe RDD wrapper; accumulates lazy op pipeline |
| `StagedPipeline` | `atomic-compute` | Accumulated ops waiting to be dispatched |
| `TaskEnvelope` | `atomic-data` | Wire type: one per partition; carries ops + encoded data |
| `TaskResultEnvelope` | `atomic-data` | Wire type: result or failure, carries `partition_id` |
| `NativeBackend` | `atomic-compute` | Worker-side op executor; looks up and runs handlers |
| `TASK_REGISTRY` | `atomic-compute` | Compile-time `HashMap<op_id, fn>` built via `inventory` |
| `REGISTRY_FINGERPRINT` | `atomic-compute` | `Lazy<u64>` — FNV-1a fold of all `(op_id, body_hash)` pairs; compared against workers at registration |
| `LocalScheduler` | `atomic-scheduler` | In-process thread-pool scheduler with full DAG/shuffle |
| `DistributedScheduler` | `atomic-scheduler` | TCP dispatcher with capacity-aware placement |
| `ShuffleDependency` | `atomic-data` | Typed shuffle edge in the RDD DAG |
| `ShuffledRdd` | `atomic-compute` | Reduce-side RDD; calls `ShuffleFetcher::fetch` on compute |
| `ShuffleManager` | `atomic-data` | Per-worker HTTP server for shuffle reads |
| `ShuffleFetcher` | `atomic-data` | HTTP client with exponential-backoff retry |
| `MapOutputTracker` | `atomic-data` | Tracks which worker holds which shuffle bucket |
| `DashMapShuffleCache` | `atomic-data` | In-memory bucket store (lock-free via DashMap) |
| `CachedRdd<T>` | `atomic-compute` | RDD wrapper that intercepts `compute()` to check/fill cache |
| `PartitionStore` | `atomic-data` | Type-erased in-memory partition cache (DashMap keyed by (rdd_id, partition)) |

### Distributed shuffle flow

```
1. Driver detects ShuffleDependency in the RDD DAG
2. Driver encodes parent partitions as rkyv Vec<(K,V)> bytes
3. Driver sends ShuffleMap TaskEnvelope to each worker (one per input partition)
4. Worker: decode Vec<(K,V)> → hash(K) % N → write buckets to DashMapShuffleCache
5. Worker returns shuffle_server_uri (its ShuffleManager address) in TaskResultEnvelope
6. Driver registers all URIs with MapOutputTracker
7. Driver runs reduce stage: ShuffledRdd::compute → ShuffleFetcher::fetch (HTTP)
8. ShuffleFetcher reads (K,C) pairs from each worker, merges via Aggregator
```

### `#[task]` dispatch — why it works without nightly

```rust
#[task]
fn double(x: i32) -> i32 { x * 2 }
// expands to:
inventory::submit!(TaskEntry {
    op_id: "myapp::double::a1b2c3d4",   // module::fn::body-hash suffix
    body_hash: 0xa1b2c3d4_..._u64,      // FNV-1a of function body tokens
    handler: __double_dispatch,
});

// At startup:
TASK_REGISTRY = inventory::iter::<TaskEntry>.map(|e| (e.op_id, e.handler)).collect()

// At dispatch:
TASK_REGISTRY.get("myapp::double::a1b2c3d4")(action, payload, data)
```

No nightly features. No unsafe transmutes. The function pointer is a plain `fn` item.

### Binary version safety

Two complementary mechanisms prevent silent wrong results when driver and worker binaries diverge:

**1. Body hash in op_id** — the last segment of every op_id is a short FNV-1a hash of the function body tokens (e.g. `"myapp::double::a1b2c3d4"`). If the function body changes, the op_id changes, and workers will log a clear "task not found" error rather than silently executing the old implementation.

**2. Registry fingerprint at handshake** — `REGISTRY_FINGERPRINT` is a single `u64` computed at startup by sorting all `(op_id, body_hash)` pairs and folding them with FNV-1a. Workers advertise this in `WorkerCapabilities.registry_fingerprint` during the TCP handshake. `DistributedScheduler::register_worker` compares it against `self.driver_fingerprint` (set via `.with_driver_fingerprint(*REGISTRY_FINGERPRINT)` at `Context` construction):

```
driver fingerprint = 0xdeadbeef...
worker advertises  = 0xdeadbeef...  → OK, proceed
worker advertises  = 0xbadcafe0...  → log::error!("fingerprint mismatch — redeploy workers")
worker advertises  = 0x0000...      → log::warn!("old binary — fingerprint not advertised")
```

The fingerprint check fires at registration time, before any tasks are dispatched, giving early failure rather than silent divergence.

### Captured state via task struct fields

```rust
#[task]
struct FilterAbove { threshold: f64 }

impl UnaryTask<f64, bool> for FilterAbove {
    fn call(&self, x: f64) -> bool { x > self.threshold }
}

let threshold = compute_on_driver(&data);
rdd.filter_task(FilterAbove { threshold })  // serialized in PipelineOp.payload
```

---

## SQL Architecture (`atomic-sql`)

DataFusion is the SQL engine. `atomic-sql` adds an integration layer:

| Type | File | Role |
|------|------|------|
| `AtomicSqlContext` | `context.rs` | Entry point — wraps DataFusion `SessionContext` |
| `DataFrame` | `dataframe.rs` | Lazy result — wraps DataFusion `DataFrame` |
| `AtomicTableProvider` | `table.rs` | `TableProvider` backed by pre-loaded `Vec<RecordBatch>` |
| `AtomicScanExec` | `exec_plan.rs` | `ExecutionPlan` leaf that streams pre-loaded batches |
| `RddTableProvider` | `rdd_table.rs` | `TableProvider` backed by a live `TypedRdd<RecordBatch>` |
| `RddScanExec` | `rdd_table.rs` | `ExecutionPlan` leaf that runs one RDD partition via atomic-compute |
| `UdfRegistry` | `udf.rs` | Helper for registering `ScalarUDF` / `AggregateUDF` |

Data flow for RDD-backed SQL:

```
TypedRdd<RecordBatch>  ──register_rdd()──►  RddTableProvider
DataFusion PhysicalPlan (leaf = RddScanExec)
  ├─ execute(partition=0) ── atomic-compute scheduler ──► thread/worker
  └─ execute(partition=1) ── atomic-compute scheduler ──► thread/worker
DataFusion: Filter / Project / Aggregate / Join  →  DataFrame.collect()
```

---

## Streaming Architecture (`atomic-streaming`)

Micro-batch streaming over `atomic-compute`. A dedicated `JobScheduler` thread fires every
`batch_duration` and calls `DStreamGraph::generate_jobs(batch_time_ms)`.

### Key types

| Type | File | Role |
|------|------|------|
| `StreamingContext` | `context.rs` | Entry point — wraps `Arc<Context>` + `DStreamGraph` |
| `DStreamGraph` | `dstream/mod.rs` | DAG of input streams + output operations |
| `DStream<T>` | `dstream/mod.rs` | Typed DStream — `compute(time_ms) → Option<Arc<dyn Rdd<Item=T>>>` |
| `JobScheduler` | `scheduler/job.rs` | Drives the batch loop thread |
| `QueueInputDStream<T>` | `dstream/input.rs` | In-memory queue of RDDs (for testing) |
| `SocketInputDStream` | `dstream/input.rs` | TCP socket reader (line-by-line) |
| `FileInputDStream` | `dstream/input.rs` | Watches a directory for new text files |
| `MappedDStream` | `dstream/mapped.rs` | Wraps parent RDD with `MapperRdd` |
| `FlatMappedDStream` | `dstream/mapped.rs` | Wraps parent RDD with `FlatMapperRdd` |
| `FilteredDStream` | `dstream/mapped.rs` | Filter via `FlatMapperRdd` |
| `WindowedDStream` | `dstream/windowed.rs` | Unions parent batches over a sliding window |
| `Checkpoint` | `checkpoint.rs` | Bincode-encoded checkpoint state; atomic write |

### Batch loop

```
JobScheduler::run_batch_loop()
  ├─ DStreamGraph::start(zero_time_ms)
  └─ loop every batch_ms:
       ├─ DStreamGraph::generate_jobs(batch_time_ms)
       │    └─ OutputOperation::generate_job(time_ms)
       │         └─ DStream::get_or_compute(time_ms)  ← lazy, cached per batch
       │              └─ DStream::compute(time_ms)    ← builds RDD DAG
       └─ StreamingJob::run()  ← executes via Arc<Context>::run_job()
```

---

## Graph Architecture (`atomic-graph`)

GraphX-style graph processing with RDD-backed vertex and edge sets.

### Key types

| Type | File | Role |
|------|------|------|
| `Graph<VD, ED>` | `graph.rs` | Vertex RDD + edge RDD pair |
| `Pregel<VD, ED, Msg>` | `pregel.rs` | Bulk-synchronous message-passing engine |
| `PageRank` | `algo/page_rank.rs` | PageRank via Pregel |
| `ShortestPath` | `algo/shortest_path.rs` | SSSP via Pregel |
| `StronglyConnectedComponent` | `algo/strongly_connected_component.rs` | Kosaraju's algorithm |
| `LabelPropagation` | `algo/label_propagation.rs` | Community detection |
| `TriangleCount` | `algo/triangle_count.rs` | Per-vertex triangle count |
| `ConnectedComponent` | `algo/connected_component.rs` | Union-find via Pregel |

---

## `atomic-cli` Architecture

Cross-compiles the project and ships the binary to remote workers over SSH/SFTP.

| Command | Description |
|---------|-------------|
| `atomic build` | Cross-compile with `cargo-zigbuild` (auto-installed if absent); default target `x86_64-unknown-linux-musl` |
| `atomic ship` | SFTP-upload binary; host-key verify → SHA-256 check → atomic rename |
| `atomic submit` | `build` + `ship` + run driver locally |
| `atomic stop` | Send graceful shutdown frame over TCP to each worker |

Security guarantees:

- Host keys verified against `~/.ssh/known_hosts`; unknown hosts refused.
- Binary uploaded to `<path>.tmp`; SHA-256 verified remotely; renamed to final path only on success.
- SSH private key never appears in a shell command or process argument list.
- Uses `russh 0.60` (pure Rust SSH) + `russh-sftp` — no `scp` subprocess.

---

## RDD Persist / Cache

`cache()` / `persist(level)` wrap the target RDD in a `CachedRdd<T>`.

```
rdd.map_task(Double).cache()
       ↓
CachedRdd { inner: MapperRdd { inner: ParallelCollection } }

action1: collect()
  CachedRdd::compute(p0) → miss → MapperRdd computes → stores Arc<Vec<T>> → returns
  CachedRdd::compute(p1) → miss → computes → stores → returns

action2: count()
  CachedRdd::compute(p0) → HIT → returns from PartitionStore (no recompute)
  CachedRdd::compute(p1) → HIT → returns from PartitionStore
```

`T` needs only `Data + Clone + 'static` — no serialization bounds.
`PARTITION_CACHE` is a process-wide singleton; keys include RDD id (globally unique).

---

## Known Gaps (as of v1.0)

All P0–P3 roadmap items are complete. The remaining known limitations are:

| Gap | Description |
| --- | --- |
| `MemoryAndDisk` lazy eviction | `persist_with_disk()` writes all partitions eagerly at persist time; true write-on-LRU-eviction requires an eviction hook in `PartitionStore` |
| Shuffle HTTP TLS | Worker TCP task port is TLS-wrapped (`tls` feature); `ShuffleManager` HTTP server is still plain HTTP |
| `ShuffleFetcher` transient retry | Network-level retry on temporary fetch failures not implemented — only stage-level retry on full map-stage failure |
| Sort-based shuffle | Only hash partitioning; range-shuffle for globally sorted output not implemented |
| Streaming distributed receivers | `ReceiverTracker` is a local stub; Kafka / Kinesis sources not implemented |
| `atomic-nlq` physical wiring | `LlmFilterExec` / `LlmMapExec` / `EmbedExec` scaffolded; full DataFusion physical planner wiring in progress |
| Python `text_file` S3 | Python binding reads local files only (`std::fs`); `s3://` requires the Rust driver |
| Python/JS streaming `start()` background thread | `start()` spawns a Rust background thread using `Python::attach` for the GIL; `JsStreamingContext::start()` is a no-op stub (JS is single-threaded NAPI) — use `run_one_batch()` in tests |
| Pregel custom vertex programs | Python/JS expose only the 6 built-in graph algorithms; the generic `pregel::run` with custom vertex/message functions is Rust-only |
| Accumulator custom merge functions | `Accumulator` supports int/float add, list append, string concat; user-defined merge closures are deferred to Phase 5 |
| No web dashboard | Job/stage timing is in structured logs + Prometheus metrics only |

See [ROADMAP.md](ROADMAP.md) for the full history of completed and planned items.
