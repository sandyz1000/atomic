# Atomic — Roadmap

Atomic's roadmap mirrors Spark's path from research prototype to production system: correctness first, then reliability, then performance, then security and release packaging. All v1.0 items are complete.

---

## v1.0 — Shipped

| Area | Features |
| --- | --- |
| Core engine | `#[task]` compile-time dispatch, `task_fn!` inline lambdas, binary version safety (body hash + registry fingerprint), `StagedPipeline` lazy op accumulation |
| Execution | `LocalScheduler`, `DistributedScheduler` (TCP, capacity-aware), speculative execution, dynamic worker heartbeat + eviction |
| Fault recovery | Lineage-based recompute on executor loss — a lost shuffle-map output (dead worker) surfaces as `FetchFailed`, the `MapOutputTracker` entry is invalidated, and the producing map partition is recomputed via stage resubmission instead of failing the job |
| Shuffle | Disk spill, shuffle-map stage fault recovery, adaptive partition coalescing, filter push-down before shuffle, per-fetch exponential-backoff retry |
| Persistence | `cache()` / `persist()`, LRU eviction for `PartitionStore`, `MemoryAndDisk` / `DiskOnly` levels, `unpersist()`, RDD checkpointing with lineage truncation |
| SQL | `atomic-sql` on DataFusion — SQL parser, 30+ optimizer rules, Parquet/CSV/JSON, RDD-backed table providers |
| Streaming | Micro-batch `StreamingContext`, `reduce_by_key` / `join` / `updateStateByKey` DStreams, windowed reductions, batch-loop checkpointing |
| Graph | `Graph<VD,ED>` + Pregel engine; PageRank, SSSP, SCC, LabelPropagation, TriangleCount, ConnectedComponent |
| Language bindings | `atomic-py` (PyO3/maturin) + `atomic-js` (NAPI) — full RDD, SQL, Graph, Streaming, Broadcast, Accumulator, custom Pregel APIs |
| Object store | S3 via `aws-sdk-s3` (`s3` feature); `text_file("s3://…")`, `save_as_text_file` |
| Shared variables | Broadcast variables (driver-serialised, worker-cached), accumulators (delta merge) |
| NLQ | `atomic-nlq` fully wired — `LlmFilterExec`, `LlmMapExec`, `EmbedExec`, `VectorSearchExec`, `LlmBatchingRule` |
| Security | Mutual TLS via `rustls` (`tls` feature, opt-in) for task TCP **and** shuffle HTTP (`ShuffleManager` hyper server via `tokio-rustls`); `atomic-cli` SSH/SFTP deploy with host-key + SHA-256 verification |
| Observability | Prometheus `/metrics` endpoint |
| Kubernetes | `deploy/Dockerfile` (single image, both roles) + Helm chart (worker `StatefulSet`, headless `Service`, driver `Job`/`Deployment`, `ConfigMap`, optional `HorizontalPodAutoscaler`, optional mTLS, `/health`+`/metrics` probes); DNS worker discovery (`--workers dns:<svc>:<port>`) with live re-resolution feeding `dynamically_add_worker` for autoscaling |
| Distributed correctness | Named custom-partitioner registry (`register_partitioner!` / `partition_by_named`) + `TypedPartitioner` (Any-free); `sort_by_task` (distributed non-pair sort); lazy k-way sort-merge reduce (bounded-memory streaming output) |
| Distributed cache + locality | `TaskAction::Cache` writes partition bytes to a worker `WORKER_PARTITION_CACHE`; driver registers `cache_locs`; later jobs serve from cache **pinned to the holding worker** (`plan_cache_dispatch` + `cache_source`); worker death / `unpersist()` invalidate locations → recompute |
| Streaming (Kafka) | `KafkaInputDStream` + `StreamingContext::kafka_stream` (`kafka` feature, vendored librdkafka) |
| Structured Streaming | `atomic-structured` crate — continuous SQL/DataFrame queries on the batch loop; tumbling, sliding, and session event-time windows; stream-stream joins (inner/left/right outer, time-bounded with watermark eviction); watermark + late-data drop, mergeable aggregates (count/sum/min/max/avg), Append/Update/Complete output modes, state store with checkpoint + recovery; sinks: memory/console/file(parquet)/Kafka |
| Distributed streaming sources | `DistributedSource` trait dispatches source partitions to workers as `TaskEnvelope`s — `DistributedFileSource` (directory → file splits) and the Kafka Direct source; `ReceiverTracker` tracks per-batch placement and re-plans a lost partition on worker death (at-least-once) |
| Distributed structured-streaming state | Keyed window/session/join state sharded across workers via `TaskAction::MergeState` + `register_state_merge!` + worker-resident `WORKER_STATE_STORE`; `.distributed(n)` on windowed/session builders and `join_stream(...).distributed(n)`; per-shard checkpoint + cold-shard recovery; report-back shard affinity (`held_state_ids` → `state_locs`) keeps each shard pinned to its holder across batches, surviving autoscaling without reshuffling |
| End-to-end exactly-once Kafka→Kafka | `KafkaSource` with `enable.auto.commit=false` drains buffer and `TopicPartitionList` atomically under one lock; `pending_offsets()` returns an `OffsetCommit` (TPL + `ConsumerGroupMetadata`); `QueryRunner::run_batch` routes through `KafkaSink::add_batch_with_offsets` (begin → produce → `send_offsets_to_transaction` → commit) — source offsets and output records durable in a single broker transaction; at-least-once path unchanged |
| Polyglot UDF safety | Driver-side preflight for dynamic UDFs: `atomic-js` rejects native/bound functions at stage time and ships captured state through explicit `*WithContext` ops; `atomic-py` round-trips every UDF through `cloudpickle.loads` before dispatch, turning "pickles but won't load" into a typed driver-side error |
| Bindings parity | `atomic-js` SQL parity with `atomic-py` — `registerRdd` (RDD→SQL), `toArrow` (Arrow IPC), `SqlContext`/`DataFrame` exported |
| CI / Release | GitHub Actions: local tests, distributed tests (dynamic ports, no sequential mutex), lint; PyPI wheels (maturin + OIDC); npm bindings (napi-rs) |
| Persistence | `MemoryAndDisk` lazy eviction — `PartitionStore` spills to disk on LRU eviction via type-erased write/read closures registered at persist time; first compute deferred to action time |
| Shared variables | Python/JS accumulators accept optional user-defined `merge_fn` callable (Python lambda or JS function) in addition to built-in numeric merge |
| Streaming | Python/JS `window(duration_ms, slide_ms)` DStream transform; `checkpoint(dir)` / `from_checkpoint(dir)` on `PyStreamingContext` / `JsStreamingContext` |
| Dynamic workers | `POST /register` HTTP endpoint on `DistributedScheduler` for worker self-registration; JSON `WorkerRegistration` body |

---

## Known Gaps

All previously-open engineering gaps are now closed. The `Potential v1.x Features` section below is the next horizon.

### Deliberate non-goals

These read like gaps but are intentional design boundaries — each has a supported alternative, and closing them would conflict with a project guardrail (no closure serialization for distributed work).

- **Push/receiver-model source distribution** — only the Direct/pull model (Kafka + files + any offset-addressable source) distributes via `DistributedSource`. Custom `Receiver` / socket fan-in stay driver-local.
- **Multiple watermarks per query** — one monotonic, global watermark per query (Spark's own constraint); sliding/session windows and stream-stream joins share it.
- **Closure `sort_by` / closure custom partitioner distribution** — closures cannot serialize across workers. The registered-task paths `sort_by_task` and `partition_by_named` / `TypedPartitioner` are the supported distributed equivalents.

---

## Potential v1.x Features

New capabilities worth building after gap closure:

- **Web dashboard** — job/stage timeline UI (Spark History Server–style); Prometheus + Grafana is the current recommendation
- **Kinesis source** — follow-on from the Kafka source
- **GCS / Azure Blob connectors** — S3 covers AWS; GCS and Azure parity for multi-cloud
- **`mapWithState`** — arbitrary stateful streaming beyond `updateStateByKey`
- **Broadcast join / sort-merge join / skew handling** — for very large shuffles and complex joins
- **Kubernetes CRD operator** — beyond the Helm chart, a controller for declarative cluster management
- **`atomic-nlq` CI integration test** — end-to-end test requiring `OPENAI_API_KEY` in CI
- **Streaming NLQ** — NLQ queries over micro-batch DStreams

---

## Out of Scope

- Kerberos / SASL authentication
- HDFS connector (S3 covers the primary cloud use case)
