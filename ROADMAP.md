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
| Structured Streaming | `atomic-structured` crate — continuous SQL/DataFrame queries on the batch loop; tumbling event-time windows, watermark + late-data drop, mergeable aggregates (count/sum/min/max/avg), Append/Update/Complete output modes, state store with checkpoint + recovery; sinks: memory/console/file(parquet)/Kafka |
| Bindings parity | `atomic-js` SQL parity with `atomic-py` — `registerRdd` (RDD→SQL), `toArrow` (Arrow IPC), `SqlContext`/`DataFrame` exported |
| CI / Release | GitHub Actions: local tests, distributed tests (dynamic ports, no sequential mutex), lint; PyPI wheels (maturin + OIDC); npm bindings (napi-rs) |
| Persistence | `MemoryAndDisk` lazy eviction — `PartitionStore` spills to disk on LRU eviction via type-erased write/read closures registered at persist time; first compute deferred to action time |
| Shared variables | Python/JS accumulators accept optional user-defined `merge_fn` callable (Python lambda or JS function) in addition to built-in numeric merge |
| Streaming | Python/JS `window(duration_ms, slide_ms)` DStream transform; `checkpoint(dir)` / `from_checkpoint(dir)` on `PyStreamingContext` / `JsStreamingContext` |
| Dynamic workers | `POST /register` HTTP endpoint on `DistributedScheduler` for worker self-registration; JSON `WorkerRegistration` body |

---

## Known Gaps

Verified against the current codebase. These are partial implementations or missing features within otherwise-complete subsystems. Each has a scoped design note where the work is non-trivial.

| Gap | What's Missing | Approach |
| --- | --- | --- |
| Distributed streaming receivers | Structured Streaming + DStream sources run **driver-local**; `ReceiverTracker` doesn't place receivers across workers | Needs distributed-streaming execution first; receivers then dispatch as long-running worker tasks reporting blocks back. See `notes/structured-streaming-design.md` |
| Exactly-once streaming | Delivery is at-least-once (offsets committed after sink write) | Idempotent/transactional sinks (file with deterministic part names, Kafka txn) |
| Sliding / session windows, stream-stream joins, multiple watermarks | Only tumbling event-time windows + one watermark | Extend the windowed engine; documented out-of-scope for 4a–4c |
| Distributed-cache LRU-evict miss | A serve read that misses because the holder *evicted* (not died) fails the job rather than transparently recomputing | Per-task recompute fallback (re-plan as `Recompute` on miss). Worker **death** already recomputes |
| Sort-shuffle input-side memory bound | Reduce **output** is lazily streamed; fetched input runs are still held in memory | Chunked streaming per-run fetch (builds on `get_slice` ranged reads) |
| Distributed `sort_by` (non-pair, closure) | Closure `sort_by` still driver-collects; `sort_by_task` (registered key) is the distributed path | Inherent to closure serialization; use `sort_by_task` |

---

## Potential v1.x Features

New capabilities worth building after gap closure:

- **Distributed Structured Streaming** — run the streaming query across workers (prerequisite for distributed receivers); see `notes/structured-streaming-design.md`
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
