# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Developement Pattern

Act as a senior Rust engineer. Review this project and refactor it to follow idiomatic Rust best practices.

### Goals:

- Apply idiomatic Rust style (rustfmt, clippy, clear error handling, minimal unwrap/expect outside tests).
- Improve project structure by grouping code by responsibility (domain, adapters, config) and avoiding generic ‘utils’.
- Make file and module names cohesive and consistent (snake_case, names reflect responsibility, e.g. circuit_breaker.rs, tts_pipeline.rs).
- Clean up API naming so types and functions are descriptive and consistent (e.g. JobStatus, run_synthesis_job, FooError, FooResult<T>).

Keep behavior the same; focus on structure, naming, and readability.

## Critical Rules

- `README.md` in the root directory may be edited when explicitly asked by the user.
- For any serialization/deserialization question, use **rkyv** for Rust native paths before considering other approaches.
- `atomic-py` is a `cdylib` — build it only with `maturin`, never with `cargo build` alone. `cargo test` for it also requires `maturin develop`.
- `atomic-worker` must be **excluded** from `cargo test --workspace` — it activates PyO3's `auto-initialize` feature, which breaks the link step in non-Python test binaries.

---

## Development Commands

```bash
# Build all Rust crates
cargo build

# Build release
cargo build --release

# Run all workspace tests (excludes atomic-py and atomic-worker by design)
cargo test --workspace --exclude atomic-py --exclude atomic-worker

# Run a single test file
cargo test -p atomic -- test_pair_ops

# Run a specific test by name
cargo test -p atomic -- test_reduce_by_key_basic

# Run the distributed integration test (spawns a real worker process)
cargo test -p atomic -- test_distributed

# Run integration binaries
cargo run --bin integration
cargo run --bin integration_shuffle_wordcount
cargo run --bin integration_multi_stage
cargo run --bin integration_fault_tolerance

# Run examples
cargo run --example task_wordcount
cargo run --example pi
cargo run --example sort
cargo run --example group_by

# Python binding (requires maturin)
cd crates/atomic-py
pip install maturin
maturin develop --release        # installs into current venv
pytest tests/                    # run Python tests

# Worker binary
cargo build --release -p atomic-worker
RUST_LOG=info ./target/release/atomic-worker --worker --port 10001

# Cross-compile and ship to remote workers
cargo install --path crates/atomic-cli
atomic build --target x86_64-unknown-linux-musl
atomic ship --workers user@host1,user@host2

# Build with TLS support
cargo build --release --features tls

# Build with S3 support
cargo build --release --features s3

# Run distributed tests (requires pre-built binary)
cargo build --release -p atomic
cargo test -p atomic -- --test-threads=1 --ignored

# Run CI locally (mirrors GitHub Actions)
cargo test --workspace --exclude atomic-py --exclude atomic-worker -- --test-threads=4
cargo fmt --all -- --check
cargo clippy --workspace --exclude atomic-py --exclude atomic-worker -- -D warnings
```

---

## Project Goal

Atomic is a stable-Rust rewrite and refactor of Vega.

- Preserve the useful high-level execution model from Vega (RDD DAG, lazy transforms, shuffle).
- Remove dependence on unstable Rust features and closure serialization.
- Replace trait-object function shipping with explicit compile-time task registration.
- Use rkyv for distributed wire payloads.

## Repository Structure

- `crates/atomic-data`: shared types — RDD traits, task envelopes, distributed structs, shuffle primitives, dependency DAG, partition cache.
- `crates/atomic-compute`: execution runtime — context, executor, `NativeBackend`, UDF dispatch, RDD implementations, persist/cache layer. Backend runtimes (in `runtimes/`): `native.rs` (Rust `#[task]` registry), `py.rs` (embedded PyO3 worker pool), `js.rs` (V8/deno_core thread-local runtime).
- `crates/atomic-scheduler`: DAG building, stage planning, job tracking, `LocalScheduler`, `DistributedScheduler`.
- `crates/atomic-sql`: structured data and SQL query layer — built on DataFusion (see below).
- `crates/atomic-streaming`: Spark Streaming–style micro-batch streaming on top of `atomic-compute`.
- `crates/atomic-graph`: GraphX-style graph processing — `Graph<VD,ED>`, Pregel engine, built-in algorithms.
- `crates/atomic-nlq`: natural language query layer — agentic `WorkflowPlan` loop (`LlmPlanner` → `WorkflowExecutor` → `AgentLoop`), plus LLM-native DataFusion plan nodes + `LlmBatchingRule` and an in-memory vector index. Uses OpenAI.
- `crates/atomic-py`: Python bindings via PyO3/maturin — full RDD API, mirrors `TypedRdd`.
- `crates/atomic-js`: Node.js bindings via NAPI — full RDD API, mirrors `TypedRdd`.
- `crates/atomic-worker`: standalone worker binary with embedded PyO3 + V8 runtimes.
- `crates/atomic-cli`: cross-compilation and secure binary distribution to remote workers.
- `crates/atomic-utils`: shared utilities.
- `crates/atomic-tests`: integration test suite (distributed, shuffle, streaming, graph, SQL).
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

Both `#[task]` and `task_fn!` emit a `body_hash: u64` field in their `TaskEntry`. The
`REGISTRY_FINGERPRINT` static folds all `(op_id, body_hash)` pairs into a single `u64` that
represents the entire compiled-in task set. Workers advertise this fingerprint on the TCP handshake;
the driver rejects (with a clear log error) any worker whose fingerprint does not match.

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

There is one execution backend: `NativeBackend` (`runtimes/native.rs`). It holds a
`HashMap<TaskRuntime, Box<dyn OpDispatcher>>` registry populated in `Default`. Each runtime
has a concrete `OpDispatcher` impl in its own file:

| File | Runtime | Responsibility |
| --- | --- | --- |
| `runtimes/native.rs` | `TaskRuntime::Native` | compile-time `TASK_REGISTRY` lookup + shuffle-map writes |
| `runtimes/py.rs` | `TaskRuntime::Python` | embedded PyO3 worker pool (`PyWorkerPool`) dispatch |
| `runtimes/js.rs` | `TaskRuntime::JavaScript` | V8/deno_core thread-local runtime dispatch |

Adding a new runtime requires only one new `impl OpDispatcher` + one `HashMap::insert` in
`NativeBackend::default()` — `execute()` never changes. Docker and WASM backends are not part
of this project.

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
- `WorkerCapabilities`: advertised per-worker limits (`max_tasks`, `registry_fingerprint`); scheduler skips workers at capacity and logs a mismatch error when fingerprints differ.

### Binary version safety

The `#[task]` op_id format is `"crate::module::fn_name::body_hash_short"` — the last segment is a short FNV-1a hash of the function body tokens. Changing the function body changes the op_id, so stale workers fail loudly at dispatch instead of silently executing old code.

`REGISTRY_FINGERPRINT` (`atomic_compute::task_registry`) is a `Lazy<u64>` computed at startup by FNV-1a-folding all sorted `(op_id, body_hash)` pairs. `DistributedScheduler` stores this as `driver_fingerprint` (set via `Context::new_with_config` → `.with_driver_fingerprint()`). Workers advertise their fingerprint in `WorkerCapabilities.registry_fingerprint`; `register_worker` compares and logs a clear error on mismatch before any tasks are dispatched.

## Current Implementation State

### Done

- `#[task]` proc-macro + `TASK_REGISTRY` compile-time dispatch. Op_id format: `"crate::module::fn::body_hash_short"` — body hash suffix catches stale workers at dispatch.
- `task_fn!` macro for inline task lambdas — identical to `#[task]` at dispatch level.
- **Binary version safety**: `TaskEntry.body_hash: u64` (FNV-1a of body tokens); `REGISTRY_FINGERPRINT: Lazy<u64>` folds all `(op_id, body_hash)` pairs; `WorkerCapabilities.registry_fingerprint` carries the value; `DistributedScheduler::register_worker` compares driver vs worker fingerprint and logs an error on mismatch — prevents silent wrong results when binaries diverge.
- `NativeBackend` — `HashMap<TaskRuntime, Box<dyn OpDispatcher>>` registry; `execute()` is pure orchestration, runtime routing is in each `OpDispatcher` impl (`runtimes/native.rs`, `runtimes/py.rs`, `runtimes/js.rs`).
- **`OpDispatcher` trait + per-runtime files**: the backend lives in `runtimes/` — `native.rs`, `py.rs` (embedded PyO3 `PyWorkerPool`), `js.rs`. Adding a runtime = one new impl + one `HashMap::insert` in `Default`.
- **`RddCore<T>` struct** (`rdd/core.rs`): shared fields + `RddBase` delegation helpers for narrow-dependency RDDs; applied to `mapper.rs`, `flatmapper.rs`, `map_partitions.rs`.
- **`TypedRdd::map_rdd` helper**: private method replacing the 3-line `new_rdd_id` / `clone context` / `Arc::new` construction idiom at 18+ call sites.
- **`group_by_key` delegates to `group_by_key_n`**: duplicate ~40-line body removed.
- `LocalScheduler` — full DAG/stage/shuffle support, thread-pool execution.
- `DistributedScheduler` — TCP dispatch, capacity-aware placement.
- rkyv distributed envelope types (`TaskEnvelope`, `TaskResultEnvelope`, `WorkerCapabilities`).
- Lazy shuffle pipeline (`ShuffleDependency` + `ShuffledRdd` + `Aggregator`).
- `DashMapShuffleCache` + `ShuffleManager` HTTP server.
- `ShuffleFetcher` + `MapOutputTracker` — per-chunk fetch retry with exponential backoff (`fetch_chunk_with_retry`, `MAX_FETCH_RETRIES`).
- `partition_id` in `TaskResultEnvelope` for correct result ordering after retries.
- Python UDF support (PyO3 / pickle) and JavaScript UDF support (V8 / deno_core / fn.toString).
- Unified `_task` API: `map_task`, `filter_task`, `flat_map_task`, `fold_task`, `reduce_task` — work identically in local and distributed mode.
- All action methods (`collect`, `count`, `take`, `first`, `reduce`, `fold`, `aggregate`, `for_each`, `for_each_partition`, `count_by_value`, `is_empty`, `top`, `take_ordered`, `max`, `min`) dispatch staged pipelines to workers in distributed mode.
- `AtomicApp::build()` — unified entry point; reads `--worker`/`--workers`/`--local-ip` from CLI.
- Explicit `Config` struct at entry point — replaces global `OnceCell<Configuration>` env-var reading.
- Distributed integration test (driver + real worker over TCP, `cargo test -p atomic-tests test_distributed`).
- **RDD persist/cache**: `TypedRdd::cache()` / `TypedRdd::persist(StorageLevel)` — partitions stored in global `PARTITION_CACHE` (`PartitionStore`) as typed `Arc<Vec<T>>`; no serialization required; subsequent actions hit the store instead of recomputing the DAG.
- **`atomic-cli`**: cross-compilation with `cargo-zigbuild` (no Docker); secure SSH/SFTP binary distribution via `russh`; host-key verification against `~/.ssh/known_hosts`; SHA-256 integrity check on remote; atomic rename; no credentials in process list.
- **`atomic-streaming` Phase 4**: `MappedDStream`, `FlatMappedDStream`, `FilteredDStream`, `WindowedDStream` all implement `compute()` using `MapperRdd`, `FlatMapperRdd`, and `UnionRdd`. Stateful + windowed pair ops are also implemented: `PairDStream::reduce_by_key` / `update_state_by_key` (with `StateMap`) and `ReducedWindowedDStream` (windowed reduce with an inverse function). Checkpoints are written at each batch boundary by `JobScheduler` when a `checkpoint_dir` is configured.
- **`atomic-graph`**: `Graph<VD,ED>` (vertex RDD + edge RDD pair), Pregel bulk-synchronous message-passing engine, built-in algorithms: PageRank, ShortestPath (Dijkstra), StronglyConnectedComponent (Kosaraju), LabelPropagation (community detection), TriangleCount, ConnectedComponent (union-find).
- **`atomic-py`**: PyO3/maturin Python bindings — full RDD API (`parallelize`, `map`, `filter`, `flat_map`, `reduce_by_key`, `group_by_key`, `collect`, etc.), mirrors `TypedRdd`.
- **`atomic-js`**: NAPI Node.js bindings — identical RDD API to `atomic-py`, mirrors `TypedRdd`.
- **`atomic-worker`**: standalone worker binary with embedded PyO3 and V8 (deno_core) runtimes; accepts `TaskEnvelope` over TCP.
- **`atomic-nlq` implemented**: `NlqContext`, `LlmPlanner` → `WorkflowPlan`, `WorkflowExecutor`, `AgentLoop`, `ToolRegistry` (builtin `sql_query` + user Python/JS tools), `OpenAiClient`, DataFusion LLM extension nodes (`LlmFilterNode`/`LlmMapNode`/`EmbedNode`/`VectorSearchNode`) + `LlmBatchingRule`, physical executors, `InMemoryVectorIndex`. The earlier `IrParser`/`ir/` design was replaced by the `WorkflowPlan` tool graph.
- **LRU eviction for `PartitionStore`**: configurable max partition count (default 1024); LRU eviction when full.
- **`unpersist()` / `is_cached()`**: `TypedRdd::unpersist()` removes all cached partitions from `PARTITION_CACHE`; `is_cached()` checks presence. `collect_partitions()` returns `Vec<Vec<T>>` (one per partition).
- **`MemoryAndDisk` / `DiskOnly` storage levels**: `TypedRdd::persist_with_disk(level)`. `MemoryAndDisk` is **lazy** — it registers per-partition spill handlers (`register_spill_path`); `PartitionStore::put` writes to disk on LRU eviction and `get` reads back on a memory miss. `DiskOnly` eagerly materialises all partitions to disk. Disk path `{work_dir}/rdd-cache/{rdd_id}/{partition}.bin` (bincode-encoded); `CachedRdd::spill_path()` + `disk_write_partition` / `disk_read_partition` helpers in `cached.rs`.
- **Metrics endpoint (Prometheus)**: `SchedulerMetrics` in `atomic-scheduler/src/metrics.rs`; `start_metrics_server(port)` serves `GET /metrics` in Prometheus text format via hyper. Enable with `Config { metrics_port: Some(9090), .. }`.
- **S3 object store** (`s3` feature): `aws-sdk-s3` + `aws-config`; `Context::text_file("s3://bucket/prefix")` lists keys and returns a lazy `TypedRdd<String>`; `TypedRdd::save_as_text_file("s3://...")` uploads `part-N` objects. Credentials from standard AWS chain (env vars, `~/.aws/credentials`, IAM role).
- **`Context::text_file(uri)`**: dispatches by URI scheme — `s3://` (requires `s3` feature), `file://` or bare local path, directory → one partition per file.
- **`TypedRdd::save_as_text_file(uri)`**: writes one `part-N` file per partition; supports local path and `s3://` (with `s3` feature).
- **RDD checkpointing (lineage truncation)**: `TypedRdd::checkpoint(dir)` — materialises all partitions, writes to `{dir}/{rdd_id}/{partition}.bin` (local or `s3://`), and returns a new `TypedRdd` backed by `CheckpointRdd` with no parent dependencies. `CheckpointStore::Local` or `CheckpointStore::S3`.
- **Speculative execution**: `Config::speculation_multiplier: Option<f64>` — when `Some(m)`, `DistributedScheduler` monitors task durations; once ≥50% of partitions in a stage complete, any task running longer than `m × median_duration` gets a speculative copy on a different worker; first result wins. Set `ATOMIC_SPECULATION_MULTIPLIER` env var or `Config { speculation_multiplier: Some(1.5), .. }`.
- **Adaptive shuffle coalescing (P2.1/P2.2)**: `Config::coalesce_shuffle_threshold_bytes` (env: `ATOMIC_COALESCE_SHUFFLE_THRESHOLD_BYTES`). After shuffle-map stage, `Mutators::compute_coalescing()` queries `SHUFFLE_CACHE` for bucket sizes and stores coalesced partition count in `MapOutputTracker::coalesced_partitions`. `ShuffledRdd::number_of_splits()` and `compute()` use this to merge small reduce partitions.
- **Sort-based shuffle**: above `Config::sort_shuffle_threshold` (env: `ATOMIC_SORT_SHUFFLE_THRESHOLD`, default 200) each map task writes the **consolidated** layout — one DATA blob + one offset INDEX (`SHUFFLE_DATA_KEY`/`SHUFFLE_INDEX_KEY` sentinels) instead of `R` per-bucket entries — collapsing the `M×R` file/entry explosion on wide shuffles. Both write paths consolidate (`shuffle_map.rs::shuffle_map_handler`, `dependency.rs::do_shuffle_task_typed`); `ShuffleCache::get_slice` serves a partition's slice (ranged disk read in `SpillableShuffleCache`); the HTTP server (`manager.rs::get_cached_data`) slices transparently so the reduce client is unchanged. With a `KeyComparator<K>` (set where `K: Ord`, e.g. `sort_by_key`), the map side writes **sorted runs** and `ShuffledRdd::compute` does a **k-way sort-merge** (via `ShuffleFetcher::fetch_runs`) instead of a `HashMap` — `sort_by_key` uses this and drops its post-shuffle local sort. Non-`Ord` keys and small shuffles keep the legacy hash/HashMap path. **Distributed**: the partitioner is shipped to workers as a serializable `PartitionerSpec` inside the `ShuffleMap` op payload (`shuffle_map::ShuffleMapPayload`), so workers partition with the RDD's *real* partitioner (range, not hash). Call `register_sort_shuffle_map!(K, V)` for `Ord` keys: the dispatcher (`runtimes/native.rs`) then routes range-partitioned shuffles to `sort_shuffle_map_handler` (range-partition via `PartitionerSpec::into_partitioner` + per-bucket sort), making **distributed `sort_by_key` globally ordered**. The driver-side reduce's stable sort (Timsort) merges the pre-sorted runs in ~O(N log M).
- **Dynamic resource allocation (P2.3)**: `Config::heartbeat_interval_secs` / `heartbeat_timeout_ms`. `DistributedScheduler::start_heartbeat()` probes `GET /health` on each worker's `ShuffleManager`; removes dead workers via `remove_worker()`. `dynamically_add_worker()` for runtime worker registration. `WorkerCapabilities::shuffle_server_port` carries the health-check port.
- **TLS for worker communication (P3.1)**: `tls` feature flag. `Executor::with_tls(cert, key, ca)` enables mTLS via `rustls`; `Executor::handle_connection` is now generic over `AsyncRead + AsyncWrite + Unpin`. `Config::tls_ca_cert/tls_cert/tls_key` (env: `ATOMIC_TLS_*`). Plain TCP when not configured. The `ShuffleManager` HTTP server also accepts TLS when `tls_enabled()` (`make_tls_acceptor`, `manager.rs`).
- **CI integration test suite (P3.4)**: `.github/workflows/ci.yml` with `test-local` (ubuntu + macos), `test-distributed` (pre-built binary + `--ignored` tests), and `lint` jobs. Distributed tests in `tests/test_distributed.rs` are now `#[ignore]`.
- **PyPI release pipeline (P3.2)**: `.github/workflows/release-py.yml` — maturin wheels for 4 targets + sdist; PyPI OIDC publishing.
- **npm release pipeline (P3.3)**: `.github/workflows/release-js.yml` — napi-rs `.node` bindings for 4 targets; npm publish.
- **S3 bindings for `atomic-py` / `atomic-js`**: `Context.text_file(uri)` now dispatches through `atomic-compute`'s `TextFileRdd` — supports `s3://bucket/prefix` when crate is built with the `s3` feature flag. `Rdd.save_as_text_file(uri)` also accepts `s3://` URIs. Enable with `--features s3` at build time.
- **Custom Pregel programs (`atomic-py` / `atomic-js`)**: `PyGraph.run_pregel(initial_msg, max_iterations, vprog, send_msg, merge_msg)` and `JsGraph.runPregelF64(...)` expose the generic `pregel::run` API with user-defined vertex programs. Message type is `f64`; `send_msg` returns a list of `(target_vertex_id, message)` pairs.
- **NLQ fully wired + tested**: all physical executors (`LlmFilterExec`, `LlmMapExec`, `EmbedExec`, `VectorSearchExec`) and `LlmBatchingRule` are complete. `crates/atomic-nlq/tests/test_context.rs` adds context-level tests (no API key required). `examples/nlq/` shows full NLQ usage with fallback to direct SQL when `OPENAI_API_KEY` is absent.
- **RDD Spark behavioral parity (5 gaps closed)**:
  - **Shuffle-based joins/cogroup**: `join`, `left_outer_join`, `right_outer_join`, `full_outer_join`, `cogroup` route through two shuffle stages — no full-dataset driver collect. Driver-side variants retained as `*_local`. `cogroup_shuffle(other, n)` exposed as a building block.
  - **Distributed `sort_by_key`**: sample → `RangePartitioner` → shuffle into range-ordered buckets → local sort. No full-dataset driver collect.
  - **`repartition_shuffle(n)`**: element-level redistribution via stride-based bucket assignment + modulo `CustomPartitioner`. Distinct from `repartition(n)` which only reassigns whole partitions.
  - **`flat_map_values(f)`**: keys preserved, values flat-mapped. Wires existing `FlatMappedValuesRdd` into `TypedRdd`.
  - **`map_partitions_to_pair(f)`**: pair-aware `map_partitions` whose output feeds into `reduce_by_key`, `join`, `cogroup`. Backed by the complete `MapPartitionsPairRdd` with correct `cogroup_iterator_any` protocol.

### Not Done Yet

All P0, P1, P2, and P3 ROADMAP items are complete. Remaining known gaps:

- **Streaming distributed receivers**: `ReceiverTracker` tracks receivers/blocks locally, but distributed (cross-node) receiver scheduling and Kafka / Kinesis sources are not implemented.
- **`atomic-nlq` real-API test**: `LlmFilterExec` / `LlmMapExec` / `EmbedExec` / `VectorSearchExec` are fully wired; `test_full_nlq_pipeline` auto-skips when `OPENAI_API_KEY` is absent.
- **`CacheTracker` distributed protocol**: locality-aware scheduling deferred; local cache works without it.
- **`sort_by` (non-pair RDD) distributed**: the key function `Fn(&T) -> K` makes K a local type param without serialization bounds; `sort_by` still collects to driver. Use `sort_by_key` on pair RDDs for distributed sort.
- **Sort-shuffle bounded-memory merge**: the driver-side reduce materialises and sorts the fetched runs in memory (Timsort merges the pre-sorted runs, but holds them all). True bounded memory needs a streaming per-run fetch (builds on the `get_slice` ranged reads). Functionally correct today; an optimization for very large reduce partitions.
- **Custom partitioner distributed**: a user `CustomPartitioner` (`partition_by`) is a closure and cannot be serialized, so `PartitionerSpec::Custom` degrades to hash partitioning on workers (its `num_partitions` is honored). Hash/Range partitioners ship correctly.

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

## Framework Design Vision: Two First-Class Execution Models

Atomic supports two production-grade ways to get user code onto workers. Neither is a second-class
citizen:

- **Dynamic closure shipping (Python / TypeScript).** The function travels with the job — Python
  lambdas via `cloudpickle`, JS functions via captured source. Runs on the worker's embedded
  PyO3 / V8 runtime. No rebuild, no redeploy. Many jobs run this way permanently.
- **Compiled task dispatch (Rust).** Rust has no closure serialization, so `#[task]` functions are
  compiled into the binary and dispatched by string ID. The worker runs the same binary as the
  driver. This buys native speed and the compile-time registry/fingerprint safety guarantee.

Because all three languages share one job API, there is also an **optional** progression between the
models — prototype dynamically, then harden a hot path as a compiled Rust task. That path is shown
below; it is one workflow the dual model enables, not the definition of the bindings.

### 1. Start dynamic; compile a hot path only if you need to

Users build jobs with the same RDD API in TypeScript (`atomic-js`) or Python (`atomic-py`) — and may
run them in production as-is. When a specific job is CPU-bound and needs more throughput, that hot
path *may* be re-expressed as a Rust `#[task]` function in `atomic-compute`.

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

### 5. Python / JS closures: the dynamic execution path

Python and JavaScript functions are a first-class dynamic path — they ship with the job and never
require binary redeployment:

- Python lambdas are `cloudpickle`-serialized on the driver (`PythonUdfPayload.fn_bytes`) and shipped
  inside `TaskEnvelope`; the worker's embedded PyO3 runtime executes them.
- JavaScript functions are captured as source via `Function.prototype.toString()`
  (`JsUdfPayload.fn_source`, with optional `context_json` for closure-like capture) and evaluated by
  the embedded V8 runtime.

Use `#[task]` when you want native Rust speed and the compile-time dispatch guarantee; use the
Python/JS closure path for dynamic, rapidly-iterating, or exploratory logic. Both are supported in
production.

| Change type | Requires binary redeployment? |
| --- | --- |
| Modify a Python UDF lambda | **No** — pickled and sent in TaskEnvelope |
| Modify a JavaScript UDF function | **No** — source string sent in TaskEnvelope |
| Modify a Rust `#[task]` function | **Yes** — compiled into the binary |
| Add new scheduler/shuffle infrastructure | **Yes** — compiled into the binary |

---

## Guardrails For Future Changes

- Reuse Vega's DAG, stage, and shuffle ideas when they fit.
- Do not copy Vega's **Rust** closure serialization model (serializing Rust fn pointers via nightly
  intrinsics). This is unrelated to the Python/JS closure-shipping path, which is a supported,
  first-class execution model.
- Do not add Docker or WASM backends — the project uses `NativeBackend` only.
- Keep local and distributed execution paths conceptually aligned.
- `#[task]` functions are the only way to register distributed work.
- Prefer explicit backend routing and compile-time dispatch over dynamic runtime guessing.
- The JS (`atomic-js`) and Python (`atomic-py`) APIs must mirror Rust `TypedRdd` — keep them in
  parity when adding new RDD operations. The three APIs are co-equal surfaces over the same engine.

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

`Checkpoint` is serialized with `bincode` v2 and written atomically (write to `.tmp`, then `rename()`). `Checkpoint::read_latest()` finds the most recent `checkpoint-<ms>` file. `JobScheduler` writes a checkpoint at the end of each batch when `StreamingContext`'s `checkpoint_dir` is set (`scheduler/job.rs`).

### Streaming Features Not Yet Implemented

- No Hadoop, no `blas`, no `hdrs`, no `atomic-sql` dependency.
- No *distributed* receiver scheduling — `ReceiverTracker` registers receivers and tracks blocks locally (`register_receiver` / `add_block` / `get_blocks_and_clear`), but cross-node scheduling is a TODO.
- No dynamic executor allocation (`ExecutorAllocationManager` is a stub — `unimplemented!`).
- No `mapWithState`. (`updateStateByKey` via `PairDStream::update_state_by_key` and windowed reduce-with-inverse via `ReducedWindowedDStream` *are* implemented.)
- No distributed streaming mode — local mode uses the same `Arc<Context>` as `atomic-compute`.

---

## atomic-graph Architecture

`crates/atomic-graph` implements GraphX-style graph processing on top of `atomic-compute`.

### Core Types

| Type | File | Role |
| --- | --- | --- |
| `Graph<VD, ED>` | `graph.rs` | Pair of vertex RDD + edge RDD; primary entry point |
| `Pregel<VD, ED, Msg>` | `pregel.rs` | Bulk-synchronous message-passing engine |

### Built-in Algorithms

| Algorithm | Description |
| --- | --- |
| `PageRank` | Iterative PageRank via Pregel |
| `ShortestPath` | Dijkstra's via Pregel |
| `StronglyConnectedComponent` | Kosaraju's two-pass algorithm |
| `LabelPropagation` | Community detection |
| `TriangleCount` | Per-vertex triangle count |
| `ConnectedComponent` | Union-find via Pregel |

### Pregel Model

Each superstep: every active vertex receives messages from the previous step, runs a user-defined
`vertex_program`, sends messages to neighbors via `send_message`, and merges incoming messages via
`merge_message`. Vertices are deactivated when they send no messages. Terminates when no messages
remain.

```rust
let graph = Graph::new(vertices_rdd, edges_rdd);
let ranks = PageRank::new(0.85, 20).run(&graph)?;
```

---

## atomic-nlq Architecture

`crates/atomic-nlq` is **implemented** — the agentic workflow loop, planner, executor, tool
registry, OpenAI client, DataFusion LLM extension nodes, and in-memory vector index are all wired
and tested. `tests/test_context.rs` runs without an API key; `test_full_nlq_pipeline` skips when
`OPENAI_API_KEY` is absent.

### Model

A natural-language analytics **agent**. The user states intent in plain language; an LLM plans a
**`WorkflowPlan`** — a dependency graph of tool calls — and an executor runs it on Atomic, iterating
until the question is answered. The LLM does **not** emit SQL or a DataFusion plan directly; it emits
a tool graph. SQL is one tool among several.

Two cooperating layers:

1. **Agentic workflow (primary path).** `LlmPlanner` → `WorkflowPlan` → `WorkflowExecutor` →
   `AgentLoop` evaluates the results and decides whether to run another round. Tools are the builtin
   `sql_query` plus any user-registered Python/JS tools.
2. **LLM-native SQL operators (inside a SQL step).** The DataFusion session carries `LlmFilterNode` /
   `LlmMapNode` / `EmbedNode` / `VectorSearchNode` (`Extension(UserDefinedLogicalNode)`) and the
   `LlmBatchingRule` optimizer rule, so per-row LLM work can run *within* a SQL query and be batched
   into grouped API calls.

> Historical note: an earlier design had the LLM emit JSON that an `IrParser` converted straight into
> a DataFusion `LogicalPlan`. That `ir/` module is gone — the planner now produces a `WorkflowPlan`
> (tool graph). The DataFusion extension nodes from that design survive as layer 2 above.

### Full Pipeline

```text
User: "find customers who bought luxury items and estimate lifetime value"
         │
         ▼  LlmPlanner (OpenAI: schema + tool list + NL query)
  WorkflowPlan — a dependency graph of tool calls (JSON):
    [ {id:"a", tool:"sql_query",  args:{query:"SELECT … FROM orders"}, depends_on:[]},
      {id:"b", tool:"llm_filter", args:{…},                           depends_on:["a"]} ]
         │
         ▼  WorkflowExecutor — runs steps in parallel dependency waves (tokio JoinSet)
  per step, dispatched by ToolRuntime:
    ├── Builtin(SqlQuery) → AtomicSqlContext.sql() → Arrow IPC DataFrame
    │     └─ a SQL step may itself contain LLM operators:
    │        LogicalPlan → LlmBatchingRule → LlmFilterExec / LlmMapExec / EmbedExec
    ├── Python(code)      → atomic-worker PyO3 runtime
    └── JavaScript(code)  → atomic-worker V8 runtime
         │
         ▼  AgentLoop.evaluate (OpenAI) → { done, answer, visualization? }
  repeat until done or max_rounds; emit AgentEvents throughout
         │
         ▼
  atomic-compute RDD DAG → LocalScheduler / DistributedScheduler → workers
```

### NLQ Component Types

| Component | File | Role | Status |
| --- | --- | --- | --- |
| `NlqContext` | `context.rs` | Entry point — wraps `AtomicSqlContext` + OpenAI client + `AgentLoop` | Implemented |
| `WorkflowPlan` / `WorkflowStep` | `workflow/mod.rs` | The LLM's tool-call dependency graph | Implemented |
| `LlmPlanner` | `planner/llm_planner.rs` | Calls OpenAI → `WorkflowPlan` (JSON) | Implemented |
| `WorkflowExecutor` | `workflow/executor.rs` | Runs steps in parallel dependency waves (`JoinSet`) | Implemented |
| `AgentLoop` | `workflow/agent_loop.rs` | plan → execute → evaluate → repeat; returns `AgentResult` | Implemented |
| `AgentEvent` | `workflow/streaming.rs` | Streamed progress events + `VisualizationSpec` | Implemented |
| `ToolRegistry` / `ToolDefinition` | `registry.rs` | Builtin + Python/JS tools; DataFusion UDF registration | Implemented |
| `OpenAiClient` | `openai/client.rs` | OpenAI chat/embeddings client (with retry) | Implemented |
| `LlmFilterNode` / `LlmMapNode` / `EmbedNode` / `VectorSearchNode` | `nodes/` | `UserDefinedLogicalNode` impls (layer 2, in SQL) | Implemented |
| `LlmFilterExec` / `LlmMapExec` / `EmbedExec` / `VectorSearchExec` | `physical/` | `ExecutionPlan` impls (layer 2) | Implemented |
| `LlmBatchingRule` | `optimizer/` | Batches per-row LLM calls within a SQL plan | Implemented |
| `InMemoryVectorIndex` / `VectorIndexProvider` | `vector/` | In-memory ANN index + pluggable trait | Implemented |

### NlqContext Entry Point

```rust
// SQL-backed analytics need the compute backend; build_with_compute wires AtomicSqlContext.
let sc = Arc::new(atomic_compute::context::Context::new_with_config(cfg)?);
let ctx = NlqContext::build_with_compute(NlqConfig::default(), sc);  // reads OPENAI_API_KEY

ctx.register_rdd("orders", orders_rdd)?;     // register a table (or via dash-delta::register_delta_table)
ctx.register_tool(my_python_tool);           // optional user Python/JS tools

// Non-streaming — returns AgentResult { answer, steps, rounds, visualization }:
let result = ctx.query("find customers who bought luxury items").await?;
println!("{}", result.answer);

// Streaming — drives a live UI; emits AgentEvent through tx:
let (tx, rx) = tokio::sync::mpsc::channel(64);
ctx.query_streaming("find customers who bought luxury items", tx).await?;

// Dry-run — inspect the WorkflowPlan without executing it:
let plan = ctx.plan("…").await?;
```

### Guardrails for atomic-nlq

- The LLM produces a `WorkflowPlan` (a tool-call dependency graph) — not SQL, and not a DataFusion
  `LogicalPlan`. Keep planning at the tool level; `sql_query` is one builtin tool.
- Within a SQL step, LLM operators are DataFusion `Extension(UserDefinedLogicalNode)` nodes — do not
  add a parallel AST. `LlmBatchingRule` must run before the physical plan to batch per-row LLM calls.
- `NlqContext::query` / `query_streaming` are the public entry points; planner/executor internals are
  not exposed. `plan(nl)` is a dry-run that returns the `WorkflowPlan`.
- The LLM provider is OpenAI (`OPENAI_API_KEY`) by design. `OpenAiClient` is constructed directly in
  `context.rs`; there is no provider trait (a known testability seam — offline tests skip).
