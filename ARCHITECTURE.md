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
- **`atomic-streaming`** — Spark Streaming–style micro-batch streaming over `atomic-compute`;
  `DistributedSource` dispatches source partitions (file splits + the Kafka Direct source) to workers.
- **`atomic-structured`** — Spark Structured Streaming–style continuous SQL/DataFrame queries on the
  batch loop: tumbling/sliding/session windows, stream-stream joins, watermarks, a
  mergeable-aggregate state store backed by `atomic-sql`, distributed sharded state with
  report-back affinity, and exactly-once Kafka→Kafka delivery.
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
| `WorkerCapabilities` | `atomic-data` | Advertised at TCP handshake — `max_tasks`, `registry_fingerprint`, `shuffle_server_port` |
| `WORKER_PARTITION_CACHE` | `atomic-compute` | Worker-side LRU partition store; fills `TaskResultEnvelope.cached_partitions` |
| `state_locs` | `atomic-scheduler` | `DashMap<state_id, SocketAddrV4>` — driver's record of which worker holds each state shard |
| `TaskResultEnvelope.held_state_ids` | `atomic-data` | Worker reports the state IDs it merged; driver registers them in `state_locs` |

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

## Structured Streaming Architecture (`atomic-structured`)

Spark Structured Streaming–style continuous SQL/DataFrame queries on the `atomic-streaming` batch
loop. Each micro-batch registers its source rows as an `input` table in a DataFusion
`SessionContext`, runs the user's query, then emits the result through a `Sink`.

### Key types

| Type | File | Role |
|------|------|------|
| `StreamingDataFrame` | `frame.rs` | Builder — `read_stream` → `with_watermark` → `window` / `session_window` / `join_stream` → `write_stream` |
| `StreamWriter` | `frame.rs` | Terminal builder — output mode, trigger, sink, `.start()` |
| `StreamSource` | `source.rs` | `next_batch(time_ms)` + `schema()`; provides `pending_offsets()` for exactly-once Kafka |
| `Sink` | `sink.rs` | `add_batch` (at-least-once) + `add_batch_with_offsets` (exactly-once, Kafka feature) |
| `BatchEngine` | `query.rs` | Per-batch processing abstraction — `StatelessEngine` (SQL pass-through), `WindowedEngine`, `SessionEngine`, `StreamJoinEngine` |
| `QueryRunner` | `query.rs` | Pairs engine + sink; routes batch through `add_batch_with_offsets` when `pending_offsets()` is non-None |
| `WatermarkTracker` | `watermark.rs` | Monotonic `max(event_time) − delay`; gates window finalization |
| `SessionEngine` | `session_window.rs` | Per-group dynamic-bound sessions; merges bridging events; finalizes on `end + gap ≤ watermark` |
| `StreamJoinEngine` | `stream_join.rs` | Time-bounded stream-stream join; per-side watermark-bounded buffers |
| `KafkaSource` | `kafka.rs` | `enable.auto.commit=false`; atomically drains `SourceShared` (buffer + TPL) in `next_batch` |
| `KafkaSink` | `kafka.rs` | At-least-once (`new`) or exactly-once (`transactional`) Kafka output |
| `OffsetCommit` | `kafka.rs` | `tpl: TopicPartitionList` + `group_metadata: Arc<ConsumerGroupMetadata>` for transactional commit |

### Query execution flow

```
QueryRunner::run_batch(epoch)
  ├─ engine.process(epoch)           ← source.next_batch → register "input" → SQL → RecordBatch[]
  ├─ engine.pending_offsets()
  │    Some(offsets) [kafka feature] → sink.add_batch_with_offsets(epoch, batches, offsets)
  │         └─ begin_transaction → produce rows → send_offsets_to_transaction → commit
  │              (source offsets + output records atomic in one broker transaction)
  └─ None                            → sink.add_batch(epoch, batches)
                                          └─ engine.post_commit(epoch) [advances Kafka offsets, at-least-once]
```

### Window models

- **Tumbling** — `window(col, size)`; one window per row via `date_bin`.
- **Sliding** — `window(col, size).slide(step)`; row fans out to every covering window via Arrow `take` expansion.
- **Session** — `session_window(col, gap)`; dynamic bounds; bridging events merge sessions.

### Distributed state

`.distributed(n)` on the windowed/session builders and `join_stream(...).distributed(n)` shards
the keyed state across `n` workers:

```
Driver (each batch)
  for shard s in 0..n:
    pick worker = pin_state_shard(s, state_locs[state_id])   ← registered worker first, modulo fallback
    send TaskEnvelope { action: MergeState { state_id }, data: per_shard_partials }

Worker
  STATE_MERGE_REGISTRY.get(state_id)(existing_state, partials) → new_state
  persist shard to checkpoint_dir
  return TaskResultEnvelope { held_state_ids: [state_id], … }

Driver
  register_state_locs(held_state_ids, worker_addr)   ← shard stays on same worker next batch
```

Worker death: `remove_worker` calls `invalidate_state_for_worker`; modulo fallback routes
to a live worker that cold-loads the shard from per-shard checkpoint storage.

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

## NLQ Architecture (`atomic-nlq`)

LLM-powered natural-language analytics. The user states intent in plain language; an LLM plans a
`WorkflowPlan` (a dependency graph of tool calls); an executor runs it on Atomic, iterating until
the question is answered.

### Two cooperating layers

1. **Agentic workflow** — `LlmPlanner` → `WorkflowPlan` → `WorkflowExecutor` → `AgentLoop`.
   Tools are the builtin `sql_query` plus any user-registered Python/JS tools.
2. **LLM-native SQL operators** — DataFusion `Extension(UserDefinedLogicalNode)` nodes
   (`LlmFilterNode`, `LlmMapNode`, `EmbedNode`, `VectorSearchNode`) and `LlmBatchingRule` so
   per-row LLM work can run *within* a SQL step and be grouped into batched API calls.

### Key types

| Type | File | Role |
|------|------|------|
| `NlqContext` | `context.rs` | Entry point — wraps `AtomicSqlContext` + `OpenAiClient` + `AgentLoop` |
| `LlmPlanner` | `planner/llm_planner.rs` | Calls OpenAI → `WorkflowPlan` JSON |
| `WorkflowPlan` / `WorkflowStep` | `workflow/mod.rs` | LLM's tool-call dependency graph |
| `WorkflowExecutor` | `workflow/executor.rs` | Runs steps in parallel dependency waves (`tokio::JoinSet`) |
| `AgentLoop` | `workflow/agent_loop.rs` | plan → execute → evaluate → repeat; returns `AgentResult` |
| `ToolRegistry` | `registry.rs` | Builtin `sql_query` + user Python/JS tools |
| `OpenAiClient` | `openai/client.rs` | OpenAI chat/embeddings client with retry |
| `LlmBatchingRule` | `optimizer/` | Batches per-row LLM calls within a DataFusion SQL plan |
| `InMemoryVectorIndex` | `vector/` | In-memory ANN index for `VectorSearchNode` |

### Request flow

```
User NL query
  └─ LlmPlanner (OpenAI: schema + tool list + query) → WorkflowPlan
       └─ WorkflowExecutor (dependency waves)
            ├─ Builtin(SqlQuery) → AtomicSqlContext.sql()
            │     └─ DataFusion + LlmBatchingRule → LlmFilterExec / LlmMapExec / EmbedExec
            ├─ Python(code) → atomic-worker PyO3 runtime
            └─ JavaScript(code) → atomic-worker V8 runtime
       └─ AgentLoop.evaluate (OpenAI) → { done, answer, visualization? }
            └─ repeat until done or max_rounds
```

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

