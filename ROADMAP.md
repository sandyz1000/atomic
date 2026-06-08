# Atomic — Roadmap

Atomic's roadmap mirrors Spark's path from research prototype to production system: correctness first, then reliability, then performance, then security and release packaging. All v1.0 items are complete.

---

## v1.0 — Shipped

| Area | Features |
| --- | --- |
| Core engine | `#[task]` compile-time dispatch, `task_fn!` inline lambdas, binary version safety (body hash + registry fingerprint), `StagedPipeline` lazy op accumulation |
| Execution | `LocalScheduler`, `DistributedScheduler` (TCP, capacity-aware), speculative execution, dynamic worker heartbeat + eviction |
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
| CI / Release | GitHub Actions: local tests, distributed tests (dynamic ports, no sequential mutex), lint; PyPI wheels (maturin + OIDC); npm bindings (napi-rs) |
| Persistence | `MemoryAndDisk` lazy eviction — `PartitionStore` spills to disk on LRU eviction via type-erased write/read closures registered at persist time; first compute deferred to action time |
| Shared variables | Python/JS accumulators accept optional user-defined `merge_fn` callable (Python lambda or JS function) in addition to built-in numeric merge |
| Streaming | Python/JS `window(duration_ms, slide_ms)` DStream transform; `checkpoint(dir)` / `from_checkpoint(dir)` on `PyStreamingContext` / `JsStreamingContext` |
| Dynamic workers | `POST /register` HTTP endpoint on `DistributedScheduler` for worker self-registration; JSON `WorkerRegistration` body |

---

## Known Gaps

Verified against the current codebase. These are partial implementations or missing features within otherwise-complete subsystems.

| Gap | What's Missing | Approach |
| --- | --- | --- |
| Streaming distributed receivers | `ReceiverTracker` is a local stub; no Kafka or Kinesis input DStream | Implement `KafkaInputDStream` via `rdkafka`; wire `ReceiverTracker` to place receivers across workers |

---

## Potential v1.x Features

New capabilities worth building after gap closure:

- **Web dashboard** — job/stage timeline UI (Spark History Server–style); Prometheus + Grafana is the current recommendation
- **Kafka / Kinesis sources** — follow-on from the streaming distributed receivers gap
- **GCS / Azure Blob connectors** — S3 covers AWS; GCS and Azure parity for multi-cloud
- **`mapWithState`** — arbitrary stateful streaming beyond `updateStateByKey`
- **`atomic-sql` streaming** — streaming SQL queries via DataFusion's streaming APIs
- **Worker auto-scaling** — hooks into AWS ASG, GKE node pools for elastic clusters
- **`atomic-nlq` CI integration test** — end-to-end test requiring `ANTHROPIC_API_KEY` or `OPENAI_API_KEY` in CI
- **Streaming NLQ** — NLQ queries over micro-batch DStreams

---

## Out of Scope

- Kerberos / SASL authentication
- HDFS connector (S3 covers the primary cloud use case)
- Docker or WASM backends
