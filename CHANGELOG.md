# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased] — K8s, Structured Streaming, Kafka, Distributed Cache, Distributed Correctness, Distributed Streaming Sources, Task Safety

### New crate: `atomic-structured`

Spark Structured Streaming–style continuous SQL/DataFrame queries on the micro-batch
loop. Three delivery phases:

- **4a (stateless)** — per-batch SQL transform over an `input` table. Zero state
  carryover between batches. `OutputMode::Append`.
- **4b (windowed)** — tumbling event-time windows with watermark + late-data drop.
  Mergeable aggregates (`count`, `sum`, `min`, `max`, `avg`). `OutputMode::Append`
  for finalized windows and `OutputMode::Update` for partial updates.
- **4c (complete + Kafka + recovery)** — `OutputMode::Complete` emits the full state
  table every batch. Kafka source/sink (`kafka` feature). Checkpoint + recovery: the
  state store is serialized with bincode and written atomically after each batch;
  `StreamingQuery::restore(dir)` replays it on restart.

**Sinks:** `MemorySink` (test), `ConsoleSink` (stdout), `FileSink` (Parquet per
batch), `KafkaSink` (`kafka` feature).

**Sources:** `QueueSource` (test), `KafkaSource` (`kafka` feature).

**State store** — bincode-serialized `BTreeMap<group_key, aggregate_cells>` written
to `{checkpoint_dir}/state.bin` on each batch; loaded on recovery.

**Windows beyond tumbling:**

- **Sliding windows** — `WindowedBuilder::slide(step)` (`WindowedSpec.slide_ms`). A row
  at `t` fans out to every covering window via an Arrow `take` expansion before the
  `GROUP BY window_start`; `None` slide means tumbling (`slide == size`).
- **Session windows** — `StreamingDataFrame::session_window(col, gap)` →
  `SessionBuilder`. `SessionEngine` keeps per-group sessions with dynamic bounds: an
  event within `gap` extends a session, a bridging event merges two sessions, and a
  session finalizes (and evicts) when `session_end + gap <= watermark`. `Append` emits
  on finalize, `Update` re-emits on every change; `Complete` is rejected (unbounded key
  space).
- **Stream-stream joins** — `StreamingDataFrame::join_stream(right, left_key, right_key,
  JoinType, time_bound)`. `StreamJoinEngine` buffers each side in a watermark-bounded
  store, probes the opposite buffer per batch, and (for outer joins) emits unmatched
  rows once the watermark passes their wait bound. Equi-join without a time bound is
  rejected at plan time to keep state bounded.

### New feature: distributed streaming sources

`atomic-streaming` generalizes the Kafka Direct model into a `DistributedSource` trait
(`dstream/distributed_source.rs`): `plan_batch(batch_time_ms)` returns per-partition
read tasks (offset range / file split) that dispatch to workers as ordinary
`TaskEnvelope`s.

- **`DistributedFileSource`** — a watched directory becomes file-split tasks
  (`TaskAction::ReadFileSplit` + `FileSplitPayload`); `StreamingContext::distributed_file_stream`
  collects the worker blocks into one RDD per batch. The Kafka Direct source is the
  second `DistributedSource` impl.
- **`ReceiverTracker`** now tracks per-batch partition placement and gains a
  `distributed: bool` (it no longer logs "local mode" unconditionally). A partition on a
  dead worker is re-planned next batch — offsets/splits are driver-authoritative, so
  recovery is at-least-once re-dispatch.
- Push/receiver-model sources (custom `Receiver`, socket fan-in) stay driver-local by
  design; only the Direct/pull model distributes.

### New feature: polyglot task serialization safety

Driver-side preflight for the dynamic (Python/JS/TS) task path, turning silent or late
worker failures into typed driver-side errors:

- **`atomic-js`** rejects native/bound functions (`f.toString()` containing
  `[native code]`) at stage time with a clear message, and exposes explicit context
  capture through `*WithContext` ops (`mapWithContext`, `filterWithContext`, …) so
  captured free variables cross the wire instead of becoming `undefined` on the worker.
- **`atomic-py`** round-trips every dispatched task through `cloudpickle.loads` on the
  driver before the job leaves, so a value that pickles but fails to load (open files,
  locks, C-extension handles) fails fast with a typed error instead of an opaque worker
  `PyErr`.

### Change: `atomic-js` is a TypeScript project

The hand-maintained native-binding loader moved to `index.ts`, compiled to
`dist/index.js` (the package `main`). `napi build --no-js` no longer emits a root
`index.js`; the loader supports only the published targets (macOS arm64, Linux
x64/arm64 — glibc and musl).

### New feature: distributed structured-streaming state

The structured-streaming window/join engines previously kept all keyed state in a
single driver-local store. State can now be **sharded across the cluster**:

- **New compute-layer primitive** (reusable, content-agnostic): `TaskAction::MergeState`
  + `StateMergePayload` (`atomic-data`), a worker-resident `WORKER_STATE_STORE`
  (`atomic-data::state_store`), and a `register_state_merge!` / `STATE_MERGE_REGISTRY`
  (`atomic-compute`) that dispatches a named, byte-level state-merge function. The
  data/compute layers carry no streaming types.
- **Sharded engines**: `DistributedStateEngine` (windowed — tumbling + sliding),
  `DistributedSessionEngine`, and `DistributedJoinEngine`. Each batch's partials/events/
  rows route by a stable key hash to a `MergeState` task that merges into the shard's
  persistent state and returns only the cells to emit; the driver assembles the output.
  Append / Update / Complete modes and the watermark are carried per shard.
- **Opt-in API**: `.distributed(num_shards)` on the windowed and session builders, and
  `join_stream(...).distributed(num_shards)` for stream-stream joins.
- **Per-shard checkpoint + recovery**: the `MergeState` handler persists each shard's
  post-merge state under the query's checkpoint dir and reloads a cold shard after a
  restart (verified by a recover-across-restart test).
- The same `dispatch_pipeline` path runs the merge in-process in local mode (shards
  share the process store) and on workers in distributed mode. Each shard is pinned to
  a stable worker (`DistributedScheduler::pin_state_shard`, reusing the cache-locality
  preferred-worker path) so its state stays local across batches; under worker
  add/remove the pin reshuffles and moved shards reload from shared (`s3://`) checkpoint
  storage. Local mode is fully correct and tested.

### New feature: transactional Kafka sink (exactly-once output)

`KafkaSink::transactional(brokers, topic, transactional_id)` configures an
idempotent producer with a stable `transactional.id` and commits each batch as one
Kafka transaction (begin → produce → commit, with abort on a produce error). A
batch's rows become visible to read-committed consumers atomically, and a batch
retried after a crash fences the previous producer instead of double-writing.
`KafkaSink::new` remains the at-least-once mode. The structured-streaming file sink
is already idempotent (deterministic `part-{epoch}.parquet`), verified by the
`recovery_no_double_emit` test. Broker-gated test:
`transactional_sink_commits_atomically` (`--features kafka -- --ignored`).

### New feature: report-back state-shard affinity (autoscaling-robust)

Structured-streaming state shards (`TaskAction::MergeState`) now use the same
report-back pattern as distributed RDD caching: the worker embeds its held
`state_id`s in `TaskResultEnvelope.held_state_ids`; the driver registers them in
`DistributedScheduler::state_locs` (`DashMap<u64, SocketAddrV4>`).
`pin_state_shard` prefers the registered worker over the modulo fallback, so a
shard already in memory on a worker is routed back to that worker every subsequent
batch — no state reload needed on a stable cluster. On worker death,
`remove_worker` calls `invalidate_state_for_worker` to clear stale registrations,
and the modulo fallback then picks a live worker that cold-loads from the per-shard
checkpoint. Three unit tests: `register_state_locs_and_pin_prefers_registered`,
`invalidate_state_for_worker_clears_its_shards`,
`pin_state_shard_falls_back_when_worker_gone`.

### New feature: end-to-end exactly-once Kafka→Kafka delivery

`KafkaSource` now runs with `enable.auto.commit=false` and tracks the consumed
`TopicPartitionList` atomically alongside the row buffer (via `SourceShared` under
one `Mutex`). `pending_offsets()` returns an `OffsetCommit` (TPL +
`ConsumerGroupMetadata`) after each batch. When `QueryRunner::run_batch` detects a
transactional sink and a non-empty `pending_offsets`, it calls
`KafkaSink::add_batch_with_offsets` which runs: begin-transaction → produce rows →
`send_offsets_to_transaction` → commit. Source offsets and output records become
durable in a single broker transaction; a crash between them is impossible.
At-least-once mode (`KafkaSink::new` + `post_batch_commit`) is unchanged.
Broker-gated test: `exactly_once_kafka_to_kafka` (`--features kafka -- --ignored`).

### Performance: bounded reduce-side memory for wide sort-shuffles

The sort-shuffle reduce already merged its output lazily, but it first decoded
**every** fetched run into a `Vec` — O(total reduce-partition bytes) resident.
`ShuffleFetcher::fetch_runs_spilled` now streams each fetched run to an anonymous
temp file, and `SpilledRunIter` decodes it one `(K, V)` at a time into the same
lazy k-way merge. `ShuffledRdd::compute` uses this path once a reduce partition
draws from more than `ATOMIC_REDUCE_SPILL_THRESHOLD_RUNS` map outputs (default 64);
narrow shuffles keep the in-memory path. Peak input memory drops to one run plus
the O(#runs) merge working set. Output is byte-identical (verified by running the
`joins` example through both paths).

### New feature: Kubernetes deployment

- **`deploy/Dockerfile`** — single multi-stage image; one binary handles both driver
  and worker roles (`--worker`/driver mode read at launch).
- **Helm chart** (`deploy/helm/atomic/`) — worker `StatefulSet` + headless `Service`,
  optional driver `Job` or `Deployment`, `ConfigMap` for env vars, optional
  `HorizontalPodAutoscaler`, optional mTLS `Secret`, `/health` + `/metrics` probes.
- **`--workers dns:<svc>:<port>`** — DNS worker discovery. `start_worker_discovery`
  resolves the headless service every 10 s and calls `dynamically_add_worker` so the
  scheduler tracks new pods added by HPA autoscaling.

### New feature: Kafka streaming source/sink

`atomic-streaming` crate gains `KafkaInputDStream` (`kafka` feature, vendored
librdkafka). `StreamingContext::kafka_stream(brokers, group_id, topics)` returns a
`DStream<String>` over Kafka topics at-least-once; the structured crate's `KafkaSource`
and `KafkaSink` add Arrow-typed integration for Structured Streaming.

### New feature: distributed RDD caching + locality scheduling

A distributed `.cache()` / `.persist()` now actually pins partition bytes on workers:

- **`TaskAction::Cache { rdd_id }`** — terminal pipeline op; the worker stores the
  output bytes in `WORKER_PARTITION_CACHE` (process-global LRU `DashMap`) and reports
  `(rdd_id, partition)` in `TaskResultEnvelope.cached_partitions`.
- **`plan_cache_dispatch`** — pure decision function on the driver; when all partitions
  of a source `CachedRdd` are in `Mutators.cache_locs`, it builds per-partition
  `cache_source` envelopes.
- **Locality pinning** — `pick_preferred_worker` routes each cache-source task to the
  worker holding that partition; falls back to round-robin on no match.
- **Fault handling** — `remove_worker` invalidates that host's `cache_locs` entries;
  the next job recomputes from lineage on a surviving worker. `unpersist()` clears
  `cache_locs[rdd_id]` on the driver.
- **Local mode unchanged** — `PartitionStore` + `CachedRdd` path is untouched.

### New feature: distributed correctness improvements

- **Named custom-partitioner registry** (`register_partitioner!` macro +
  `partition_by_named(name, n)`) — ship a named partitioner to workers by string ID;
  workers look it up in the static registry. Enables custom partitioners in fully
  distributed pipelines.
- **`TypedPartitioner<K>`** — blanket impl over any `CustomPartitioner` that avoids
  `&dyn Any` downcasts; `PartitionerSpec::from_named` resolves to a `TypedPartitioner`
  on workers.
- **`sort_by_task`** — distributed non-pair sort keyed by a registered `#[task]`
  function. Avoids the closure serialization limitation of `sort_by`.
- **Lazy k-way sort-merge reduce** — `ShuffledRdd::compute` merges pre-sorted runs
  on demand via `itertools::kmerge_by` + `coalesce`; the merged output is never fully
  materialised in memory. Verified globally-ordered by `sort_shuffle_globally_ordered`.

### New feature: `atomic-js` SQL parity

`atomic-js` reaches full SQL parity with `atomic-py`:

- `SqlContext.registerRdd(name, rdd)` — register a live `JsRdd` as a SQL table
  (`RddTableProvider`).
- `DataFrame.toArrow()` — returns collected results as Arrow IPC bytes.
- `SqlContext` and `DataFrame` fully exported from `index.js`.

### Bug fixes

- **`checkpoint()` rdd_id mismatch** — partitions were written under the source RDD's
  id but read back under the new `CheckpointRdd`'s id (always a file-not-found miss).
  Fixed by allocating `checkpoint_id = ctx.new_rdd_id()` before the write loop and
  keying written files by `checkpoint_id`.

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
