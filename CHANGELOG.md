# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- Pair RDD operations: `join`, `left_outer_join`, `cogroup`, `keys`, `values`, `map_values`
- Partition operations: `map_partitions`, `map_partitions_with_index`, `glom`, `coalesce`, `repartition`
- Set operations: `union`, `cartesian`, `zip`, `distinct`, `subtract`, `intersection`
- Sort operations: `sort_by`, `sort_by_key`, `sort_by_key_range` (range partitioner)
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

#### Natural Language Query (scaffolded)
- `atomic-nlq`: `NlqContext`, `LlmPlanner`, `IrParser`, `LlmBatchingRule`
- `LlmFilterNode`, `LlmMapNode`, `EmbedNode`, `VectorSearchNode` extension nodes
- `InMemoryVectorIndex` (implemented)

---

## [0.1.0] — 2025-01-01

Initial private development release.

[1.0.0]: https://github.com/sandip-dey/atomic/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/sandip-dey/atomic/releases/tag/v0.1.0
