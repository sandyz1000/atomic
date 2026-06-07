# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased] — Binary Version Safety + Code Quality

### New features

- **`TaskEntry.body_hash: u64`** — every `#[task]` and `task_fn!` entry now carries an FNV-1a hash
  of the function body tokens, computed at compile time by the proc-macro.
- **Op_id body-hash suffix** — `#[task]` op_ids are now `"crate::module::fn::body_hash_short"`
  (e.g. `"myapp::transform::normalize::a1b2c3d4"`). Changing the function body changes the op_id,
  so stale workers fail at dispatch with a clear "task not registered" error rather than silently
  executing old code.
- **`REGISTRY_FINGERPRINT: Lazy<u64>`** (`atomic_compute::task_registry`) — FNV-1a fold of all
  sorted `(op_id, body_hash)` pairs; represents the entire compiled-in task set as a single value.
  Two independently compiled binaries with identical task implementations produce the same fingerprint.
- **`WorkerCapabilities.registry_fingerprint: u64`** — workers advertise their fingerprint during
  the TCP handshake (default `0` for old workers that have not been redeployed).
- **`DistributedScheduler::with_driver_fingerprint(u64)`** builder — stores the driver fingerprint
  without introducing a dependency from `atomic-scheduler` onto `atomic-compute`.
  `Context::new_with_config` chains `.with_driver_fingerprint(*REGISTRY_FINGERPRINT)` automatically.
- **`DistributedScheduler::register_worker` mismatch detection** — compares driver vs worker
  fingerprints on registration. Logs `warn!` for old workers (fingerprint = 0) and `error!` for
  active mismatches. Check fires before any task is dispatched.

### Internal improvements

- `Job::run_id` and `Job::job_id` are now `pub` fields; getter methods `run_id()` / `job_id()` removed.
- `TypedRdd::get_context()` removed; internal callers use `self.context.clone()` directly.
- 57 test functions with 4–7-word names shortened to 2–3 words across 10 test files.

---

## [Unreleased] — QOL Refactor

### Changed

- **Backend file renames**: `native.rs` → `native_executor.rs`, `python_pool.rs` → `python_executor.rs`, `js_runtime.rs` → `js_executor.rs`. Names now reflect responsibility (executor), not implementation detail (pool/runtime).
- **`NativeBackend` refactored** (`backend/native_executor.rs`): now holds `HashMap<TaskRuntime, Box<dyn OpDispatcher>>` for O(1) per-op routing. `OpDispatcher` trait (`backend/mod.rs`) allows adding a new runtime via one new `impl` + one `HashMap::insert` — `execute()` never changes.
- **`RddCore<T>` applied to `map_partitions.rs`**: `MapPartitionsRdd` and `MapPartitionsPairRdd` now use the shared `RddCore<T>` composition struct, eliminating ~30 lines of duplicated boilerplate. Pattern already applied to `mapper.rs` and `flatmapper.rs`.
- **`TypedRdd::map_rdd` helper**: private method replaces the 3-line `new_rdd_id` / `clone context` / `Arc::new(SomeRdd::new(id, rdd, ...))` construction idiom at 18+ call sites.
- **`group_by_key` delegates to `group_by_key_n`**: removed ~40-line duplicate body; `group_by_key_n` is now the single source of truth.
- **`TaskRuntime` derives `Hash`** in `atomic-data`: enables `HashMap<TaskRuntime, _>` keying without a wrapper type.

### Fixed

- `_market_u` typo in `rdd/cartesian.rs` corrected to `_marker_u`.
- Removed no-op `ConfigBuilder::app_name` method that ignored its parameter and did a pointless self-clone.
- Removed misleading `log::debug!` calls with trailing commas in `pair.rs` and `map_partitions.rs`.
- Removed stale commented-out `impl PairRdd` block from `pair.rs`.
- Removed stale `// #[derive(Serialize, Deserialize)]` comment from `partitionwise_sampled.rs`.

---

## [Unreleased] — RDD Spark Behavioral Parity

### RDD API — Spark Parity

Five unintentional divergences from Spark's RDD semantics have been closed. The core
transformation/action API (~85-90% of real-world usage) was already semantically correct; these
fixes address the remaining behavioral gaps.

- **Shuffle-based joins/cogroup** — `join`, `left_outer_join`, `right_outer_join`,
  `full_outer_join`, and `cogroup` now route through two shuffle stages instead of collecting both
  RDDs to the driver. O(partition_size) memory per worker; no single-machine bottleneck at scale.
  - Driver-side (small-data) variants retained as `join_local`, `left_outer_join_local`,
    `right_outer_join_local`, `full_outer_join_local`, `cogroup_local`.
  - `cogroup_shuffle(other, num_partitions)` exposed as a public low-level building block.

- **Distributed `sort_by_key`** — replaced driver-side collect-and-sort with a three-step
  distributed approach: sample ~20 keys per partition → build `RangePartitioner` from sample
  boundaries → shuffle into range-ordered buckets → sort within each partition. No full-dataset
  driver collect.

- **`repartition_shuffle(n)`** — true element-level redistribution across `n` partitions using a
  stride-based bucket assignment and a modulo custom partitioner. `repartition(n)` / `coalesce(n,
  true)` continue to use whole-partition reassignment (no type-bound requirements); use
  `repartition_shuffle` when element uniformity matters.

- **`flat_map_values(f)`** — like `flat_map` but keys are preserved and emitted once per output
  value. Wraps the existing (but previously unexposed) `FlatMappedValuesRdd`.

- **`map_partitions_to_pair(f)`** — pair-aware `map_partitions` where the output correctly
  participates in `reduce_by_key`, `join`, and `cogroup`. The critical difference from plain
  `map_partitions`: `cogroup_iterator_any` boxes key and value separately, satisfying the cogroup
  protocol. Backed by the now-complete `MapPartitionsPairRdd`.

---

## [1.0.0] — Unreleased

### Added

#### Core Execution
- `#[task]` proc-macro and `TASK_REGISTRY` for compile-time task dispatch — no closure serialization
- `task_fn!` macro for inline task lambdas (content-hash stable `op_id`)
- `NativeBackend` — single execution backend; dispatches by `op_id` string
- `LocalScheduler` — full DAG/stage/shuffle support, thread-pool execution
- `DistributedScheduler` — TCP dispatch, capacity-aware placement, speculative execution
- Unified `_task` API: `map_task`, `filter_task`, `flat_map_task`, `fold_task`, `reduce_task`

#### RDD API
- Full `TypedRdd<T>` API: `map`, `filter`, `flat_map`, `reduce_by_key`, `group_by_key`, `combine_by_key`
- Pair RDD operations: `join`, `left_outer_join`, `right_outer_join`, `full_outer_join`, `cogroup` (shuffle-based), `*_local` driver-side variants, `cogroup_shuffle`, `keys`, `values`, `map_values`, `flat_map_values`
- Partition operations: `map_partitions`, `map_partitions_with_index`, `map_partitions_to_pair`, `glom`, `coalesce`, `repartition`, `repartition_shuffle`
- Set operations: `union`, `cartesian`, `zip`, `distinct`, `subtract`, `intersection`
- Sort operations: `sort_by`, `sort_by_key` (distributed, range-partition), `sort_by_key_range`
- Actions: `collect`, `count`, `take`, `first`, `reduce`, `fold`, `aggregate`, `for_each`, `for_each_partition`, `count_by_value`, `is_empty`, `top`, `take_ordered`, `max`, `min`
- Pair actions: `count_by_key`, `lookup`, `collect_partitions`
- `AtomicApp::build()` — unified driver/worker entry point

#### Caching & Persistence
- `cache()` / `persist(StorageLevel)` — in-process `PartitionStore` with LRU eviction (1024 partitions)
- `persist_with_disk()` — `MemoryAndDisk` and `DiskOnly` storage levels (bincode-encoded)
- `unpersist()` / `is_cached()` / `collect_partitions()`

#### Shuffle
- Lazy shuffle pipeline: `ShuffleDependency` + `ShuffledRdd` + `Aggregator`
- `DashMapShuffleCache` + `ShuffleManager` HTTP server
- `ShuffleFetcher` + `MapOutputTracker`
- Adaptive shuffle coalescing: `Config::coalesce_shuffle_threshold_bytes`
- Partition result ordering via `partition_id` in `TaskResultEnvelope`

#### I/O
- `Context::text_file(uri)` — `s3://`, `file://`, local path, directory (one partition per file)
- `TypedRdd::save_as_text_file(uri)` — local and `s3://` (requires `s3` feature)
- RDD checkpointing: `TypedRdd::checkpoint(dir)` — lineage truncation, local or S3

#### Streaming (`atomic-streaming`)
- Micro-batch `StreamingContext` with `DStreamGraph` and batch loop
- `QueueInputDStream`, `SocketInputDStream`, `FileInputDStream`
- `MappedDStream`, `FlatMappedDStream`, `FilteredDStream`, `WindowedDStream`
- `ReducedWindowedDStream`, `JoinDStream`, `UpdateStateByKeyDStream`
- Bincode checkpoint serialization

#### SQL (`atomic-sql`)
- `AtomicSqlContext` — wraps DataFusion 53 `SessionContext`
- `DataFrame` lazy result type with full SQL operator set
- `register_csv`, `register_parquet`, `register_json`, `register_rdd`, `register_batches`
- `DataFrame::write_parquet`, `DataFrame::write_csv`

#### Graph (`atomic-graph`)
- `Graph<VD, ED>` — vertex RDD + edge RDD
- Pregel bulk-synchronous message-passing engine
- Built-in algorithms: PageRank, ShortestPath (Dijkstra), SCC (Kosaraju), LabelPropagation, TriangleCount, ConnectedComponents

#### Language Bindings (v1.0)

- `atomic-py` (PyPI: `atomic-compute`): full RDD and SQL API via PyO3/maturin; Python `.pyi` type stubs
- `atomic-js` (npm: `@atomic-compute/js`): full RDD and SQL API via napi-rs

#### Language Bindings — Phase 4 additions

**`atomic-py` and `atomic-js` now expose:**

- **RDD API gaps**: `join`, `left_outer_join`, `sort_by`, `sort_by_key`, `glom`, `cache`, `persist`, `unpersist`, `checkpoint`
- **Broadcast variables**: `ctx.broadcast(value)` → `BroadcastVar` — pickled/JSON-serialized, `.value()` for read-back
- **Accumulators**: `ctx.accumulator(zero)` → `Accumulator` — supports int/float add, list append, string concat; `.add(delta)`, `.value()`, `.reset()`
- **Graph bindings** (`Graph` class): construct from vertex/edge lists; `pageRank`, `connectedComponents`, `stronglyConnectedComponents`, `labelPropagation`, `triangleCount`, `shortestPath`
- **Streaming bindings** (`StreamingContext`, `DStream`): full pair-stream API — `map`, `filter`, `flatMap`, `reduceByKey`, `groupByKey`, `join`, `leftOuterJoin`, `updateStateByKey`, `mapValues`; deterministic `runOneBatch()` for tests

#### Infrastructure
- `atomic-cli`: cross-compilation via `cargo-zigbuild`; SSH/SFTP binary distribution; host-key verification; SHA-256 integrity check
- `atomic-worker`: standalone worker binary with embedded PyO3 and V8 runtimes
- TLS (mTLS) for worker TCP communication (`tls` feature, rustls)
- S3 object store support (`s3` feature, `aws-sdk-s3`)
- Prometheus metrics endpoint: `Config::metrics_port`, `GET /metrics`
- Speculative execution: `Config::speculation_multiplier`
- Dynamic resource allocation: heartbeat-based dead-worker removal
- CI pipeline (GitHub Actions): local tests, distributed tests, lint, `cargo deny` audit
- PyPI release pipeline (`release-py.yml`): maturin wheels for 4 targets
- npm release pipeline (`release-js.yml`): napi-rs bindings for 4 targets

#### Natural Language Query
- `atomic-nlq`: `NlqContext`, `LlmPlanner`, `IrParser`, `LlmBatchingRule` — fully wired end-to-end
- `LlmFilterExec`, `LlmMapExec`, `EmbedExec`, `VectorSearchExec` — physical executors complete
- `InMemoryVectorIndex` (cosine similarity, for testing)
- 31 unit tests; `test_full_nlq_pipeline` auto-skips without `ANTHROPIC_API_KEY`
- `examples/nlq/` — runnable demo (NLQ path with API key; SQL fallback without)

#### Language Bindings — Phase 5 additions

- **S3 I/O for `atomic-py` / `atomic-js`**: `Context.text_file(uri)` and `Rdd.save_as_text_file(uri)` now dispatch through `atomic-compute`'s `TextFileRdd`, supporting `s3://bucket/prefix` when built with `--features s3`. Local paths unchanged.
- **Custom Pregel programs for `atomic-py` / `atomic-js`**: `PyGraph.run_pregel(initial_msg, max_iterations, vprog, send_msg, merge_msg)` and `JsGraph.runPregelF64(...)` expose the full generic Pregel API with user-defined vertex programs. Message type is `f64`; `send_msg` returns `[(target_vertex_id, message)]` pairs.
- **pyo3 0.28 compatibility**: Updated `atomic-py` bindings (`sql.rs`, `shared.rs`, `context.rs`, `graph.rs`) for pyo3 0.28 API changes — `PyObject` → `Py<PyAny>`, `Python::with_gil` → `Python::attach`, `bool::into_pyobject` `Borrowed` handling.
- **Streaming scheduler timer fix**: Batch loop no longer recomputes `batch_time_ms` from wall clock after sleep — uses the pre-computed boundary to prevent duplicate batch execution under load.

---

## [0.1.0] — 2025-01-01

Initial private development release.

[1.0.0]: https://github.com/sandip-dey/atomic/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/sandip-dey/atomic/releases/tag/v0.1.0
