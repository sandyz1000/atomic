# Atomic — Production Readiness Roadmap

This document tracks what is needed to take Atomic from experimental research to a system that
can run real workloads reliably. ML/NLQ features (`atomic-nlq`) are excluded from this plan.

**Inspired by Spark's path to production**, the priorities below mirror the same issues Spark's
early adopters had to solve: reliable shuffle, bounded memory, fault recovery, observability,
and secure distribution.

---

## Current Status Summary

**All P0 → P3 items are complete.** The full production readiness roadmap has been implemented.
Outstanding gaps are documented in the "Known Remaining Gaps" section below.

| Layer | State |
| ----- | ----- |
| Core RDD engine | ✅ `#[task]` dispatch, local + distributed, lazy pipeline staging, intelligent `task_fn!` op_ids |
| Shuffle | ✅ Disk spill, fault recovery (stage retry), adaptive coalescing, filter push-down |
| SQL (`atomic-sql`) | ✅ DataFusion-backed; Parquet/CSV/JSON; RDD-backed tables |
| Streaming (`atomic-streaming`) | ✅ `reduce_by_key`, `join`, `updateStateByKey`, checkpointing wired to batch loop |
| Graph (`atomic-graph`) | ✅ Pregel engine; PageRank, SSSP, SCC, LabelPropagation, TriangleCount, CC |
| RDD Cache/Persist | ✅ LRU eviction, `MemoryAndDisk`/`DiskOnly` disk spill, `unpersist()`, RDD checkpointing |
| Fault recovery | ✅ Per-task retry + shuffle-map stage recompute + speculative execution |
| Observability | ✅ Prometheus `/metrics` endpoint (`Config::metrics_port`) |
| Security | ✅ Mutual TLS via `rustls` (`tls` feature); plain TCP fallback when unconfigured |
| Distribution | ✅ `atomic-cli` cross-compile + SSH/SFTP ship; dynamic worker heartbeat + registration |
| Language bindings | ✅ `atomic-py` + `atomic-js`: RDD, SQL, Graph, Streaming, Broadcast, Accumulator |
| Object store | ✅ S3 via `aws-sdk-s3` (`s3` feature); `text_file("s3://…")`, `save_as_text_file` |
| Shared variables | ✅ Broadcast variables, accumulators |
| CI / Release | ✅ GitHub Actions: local tests, distributed tests, lint, PyPI wheels, npm bindings |

---

## Priority 0 — Correctness ✅ ALL DONE

### P0.1 — Shuffle Disk Spill ✅

`SpillableShuffleCache` writes shuffle buckets to `{work_dir}/shuffle-spill/` when the
in-memory size exceeds `Config::shuffle_spill_threshold`. Pluggable via `ShuffleStore` trait
(`MemoryShuffleStore` / `DiskShuffleStore`). Atomic rename (`.tmp` → final) prevents partial
reads. `ShuffleManager` HTTP handler reads from disk on memory miss.

---

### P0.2 — Shuffle-Map Stage Fault Recovery ✅

On `ShuffleFetcher` fetch failure, stale URIs are cleared from `MapOutputTracker` and the full
map stage is re-submitted (up to `max_failures` times) before the reduce stage retries.
`DashMapShuffleCache` entries from the re-run overwrite stale ones.

---

### P0.3 — LRU Eviction for `PartitionStore` ✅

`PartitionStore` uses an LRU-bounded structure with configurable max partition count (default
1024). Evicted partitions are recomputed on next access. Implemented in
`atomic-data/src/cache/mod.rs`.

---

### P0.4 — `unpersist()` API ✅

`TypedRdd::unpersist()` calls `PartitionStore::remove_rdd(rdd_id, n)` to evict all cached
partitions. `TypedRdd::is_cached()` checks whether any partition is currently held.
`TypedRdd::collect_partitions()` returns `Vec<Vec<T>>` (one per partition) for downstream use.

---

### P0.5 — `MemoryAndDisk` / `DiskOnly` Storage Levels ✅

`TypedRdd::persist_with_disk(level)` materialises all partitions upfront and writes them to
`{work_dir}/rdd-cache/{rdd_id}/{partition}.bin` (bincode-encoded, atomic `.tmp → rename`).
`CachedRdd::spill_path()` returns the disk path; `disk_write_partition` / `disk_read_partition`
helpers live in `atomic-compute/src/rdd/cached.rs`.

> **Note:** True "write on LRU eviction" (lazy spill) is not yet wired — `persist_with_disk`
> writes eagerly. An eviction hook in `PartitionStore` is needed for the full Spark behaviour.

---

## Priority 1 — Reliability and Completeness ✅ ALL DONE

### P1.1 — Broadcast Variables ✅

`Context::broadcast(value: T) -> BroadcastVar<T>` serialises `T` once on the driver and
attaches it to every `TaskEnvelope`. Workers deserialise and cache in a per-process store.
`BroadcastVar<T>::value()` returns the cached value. Implemented in
`atomic-data/src/broadcast.rs` + `atomic-compute/src/context.rs`.

---

### P1.2 — Accumulators ✅

`Context::accumulator(zero: T) -> Accumulator<T>` with `value()` accessor on driver.
Workers call `acc.add(delta)`; deltas are collected in `TaskResultEnvelope` and merged by the
scheduler after each stage. Implemented in `atomic-data/src/accumulator.rs`.

---

### P1.3 — Object Store Integration (S3 only) ✅

Uses the official AWS SDK (`aws-sdk-s3` + `aws-config`); GCS is out of scope.

- `Context::text_file(uri)` — `s3://bucket/prefix` lists keys (one partition per key),
  `file://` / bare local path / directory → `TextFileRdd<String>` (lazy per-partition reads).
- `TypedRdd::save_as_text_file(uri)` — writes `part-N` files locally or as S3 objects.
- Enable with `s3` feature flag: `cargo build --features s3`.
- Credentials via standard AWS chain (env vars, `~/.aws/credentials`, IAM role).
- Implemented in `atomic-compute/src/io/s3.rs` + `io/text_file_rdd.rs`.

---

### P1.4 — RDD Checkpointing (Lineage Truncation) ✅

`TypedRdd::checkpoint(dir)` materialises all partitions, writes each to
`{dir}/{rdd_id}/{partition}.bin` (bincode, atomic `.tmp → rename`), then returns a new
`TypedRdd` backed by `CheckpointRdd` — no parent dependencies. Supports local paths and
`s3://` (with `s3` feature). `CheckpointStore::Local` / `CheckpointStore::S3` in
`atomic-compute/src/rdd/checkpoint.rs`.

---

### P1.5 — Speculative Execution ✅

`Config::speculation_multiplier: Option<f64>` (env: `ATOMIC_SPECULATION_MULTIPLIER`).
`DistributedScheduler::with_speculation(m)` builder. Once ≥50% of a stage's tasks
complete, any task running longer than `m × median_duration` receives a speculative copy on
a different worker; first result wins. Implemented in `run_native_job_inner` inside
`atomic-scheduler/src/distributed.rs`.

---

### P1.6 — Streaming: Shuffle DStream ✅

`ShuffledDStream::compute(time_ms)` calls `reduce_by_key` on the parent RDD (two-phase
shuffle via `LocalScheduler`). `PairDStream::reduce_by_key`, `group_by_key`, `join`, and
`left_outer_join` are fully wired. Implemented in `atomic-streaming/src/dstream/shuffle.rs`
and `dstream/pair.rs`.

---

### P1.7 — Streaming: Checkpointing Wired to Batch Loop ✅

`JobScheduler` calls `StreamingContext::checkpoint_to(path)` after each batch. Checkpoint
captures `zero_time_ms` and per-DStream metadata (bincode-encoded, atomic `.tmp → rename`).
`StreamingContext::from_checkpoint(path)` restores and resumes the batch loop.

---

### P1.8 — Streaming: `updateStateByKey` ✅

`PairDStream::update_state_by_key(update_fn)` produces a `StateDStream<K, S>` that carries
a state `RDD<(K, S)>` across batches, merging new values with existing state each tick.
`ReducedWindowedDStream` also implemented for windowed reductions. In
`atomic-streaming/src/dstream/` (`pair.rs`, `windowed.rs`).

---

### P1.9 — Metrics Endpoint (Prometheus) ✅

`SchedulerMetrics` in `atomic-scheduler/src/metrics.rs` exposes:
`atomic_tasks_total{status}`, `atomic_task_duration_seconds`, `atomic_jobs_total{status}`,
`atomic_stage_duration_seconds`, `atomic_shuffle_bytes_{written,read}_total`,
`atomic_partition_cache_entries`, `atomic_broadcast_bytes_total`.
HTTP server (`GET /metrics`, Prometheus text format) via hyper on `Config::metrics_port`
(env: `ATOMIC_METRICS_PORT`). Default port `9090` when enabled.

---

## Priority 2 — Performance and Scalability ✅ ALL DONE

### P2.1 — DAG Optimizer ✅

Filter push-down before shuffle was already implemented via `StagedPipeline`: `filter_task().reduce_by_key()` carries the `Filter` op into the shuffle-map `TaskEnvelope` so it runs on workers before data is written to shuffle buckets.

Post-shuffle stage coalescing is implemented via `Config::coalesce_shuffle_threshold_bytes`
(env: `ATOMIC_COALESCE_SHUFFLE_THRESHOLD_BYTES`). After all shuffle-map tasks complete,
`Mutators::compute_coalescing()` queries `SHUFFLE_CACHE` for per-bucket byte sizes, computes
an optimal coalesced partition count via greedy merge, and stores it in
`MapOutputTracker::coalesced_partitions`. `ShuffledRdd::number_of_splits()` queries this and
returns the coalesced count; `compute()` maps coalesced partition IDs back to original buckets.

---

### P2.2 — Adaptive Partition Coalescing ✅

Implemented together with P2.1. Key additions:

- `ShuffleCache::bytes_for_reduce_partition()` — sums bucket bytes across all map tasks for one reduce partition; default implementation in the trait.
- `MapOutputTracker::coalesced_partitions: Arc<DashMap<usize, usize>>` — stores coalesced count per shuffle; `set_coalesced_partitions()` / `get_coalesced_partitions()`.
- `Mutators::coalesce_threshold_bytes` — set from `Config` via `LocalScheduler::new_with_coalesce()`.
- `ShuffledRdd::compute()` — when coalescing is active, fetches from a range of original buckets and merges into each coalesced partition.

---

### P2.3 — Dynamic Resource Allocation ✅

**Heartbeat**: `DistributedScheduler::start_heartbeat(interval_secs, timeout_ms)` — background
tokio task that probes `GET /health` on each worker's `ShuffleManager`. After
`MAX_WORKER_FAILURES` (3) failures, calls `remove_worker()` which evicts the worker and
clears stale `MapOutputTracker` shuffle URIs. Enable via `Config::heartbeat_interval_secs`
(env: `ATOMIC_HEARTBEAT_INTERVAL_SECS`).

**`/health` endpoint**: Added to `ShuffleService` in `manager.rs` — returns HTTP 200.

**`dynamically_add_worker(endpoint, caps)`**: Callable at runtime to add workers without restart.

**`WorkerCapabilities::shuffle_server_port`**: Carries the worker's HTTP port for heartbeat probing.

---

## Priority 3 — Security and Release ✅ ALL DONE

### P3.1 — TLS for Worker Communication ✅

Opt-in mutual TLS via `rustls` (no OpenSSL dependency). Requires the `tls` feature flag
(`cargo build --features tls`).

- `crates/atomic-compute/src/tls.rs`: `make_server_config()` / `make_client_config()` load PEM
  cert/key/CA files and produce `rustls::ServerConfig` / `ClientConfig` for mTLS.
- `Executor::with_tls(cert, key, ca)`: enables TLS on the worker listener. All connections are
  TLS-upgraded before `handle_connection` (which is now generic over `AsyncRead + AsyncWrite + Unpin`).
- `Config::tls_ca_cert / tls_cert / tls_key`: cert paths. Set via env vars `ATOMIC_TLS_CA_CERT`,
  `ATOMIC_TLS_CERT`, `ATOMIC_TLS_KEY`. `None` on all three (default) → plain TCP, no behaviour change.
- `start_worker()` in `context.rs` conditionally builds a `with_tls` executor when all three
  cert paths are set.
- Deps added (optional, `tls` feature): `tokio-rustls = "0.26"`, `rustls = "0.23"`,
  `rustls-pemfile = "2.0"`, `rcgen = "0.13"` (for cert generation in `atomic-cli`).

---

### P3.2 — PyPI Release Pipeline ✅

`.github/workflows/release-py.yml`: triggers on `v*` tags. Builds wheels for
`x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `x86_64-apple-darwin`,
`aarch64-apple-darwin` using `PyO3/maturin-action@v1`. Publishes via PyPI trusted publishing
(OIDC — no stored API token). Also builds and publishes an sdist.

---

### P3.3 — npm Release Pipeline ✅

`.github/workflows/release-js.yml`: triggers on `v*` tags. Builds `.node` native bindings for
4 targets using `npx napi build --platform --release --target`. Bundles with `napi prepublish`
and publishes to npm as `@atomic-compute/js`.

---

### P3.4 — Integration Test Suite in CI ✅

`.github/workflows/ci.yml`: three jobs triggered on push to `main` / `phase-*` branches and PRs:

- **test-local** (`ubuntu-latest` + `macos-latest`): `cargo test --workspace --exclude atomic-py
  --exclude atomic-worker -- --test-threads=4`
- **test-distributed** (`ubuntu-latest` only): pre-builds integration binaries, then
  `cargo test -p atomic -- --test-threads=1 --ignored` (runs the 4 distributed tests in
  `tests/test_distributed.rs` which spawn real worker + driver processes over TCP)
- **lint**: `cargo fmt --all -- --check` + `cargo clippy -D warnings`

The 4 distributed tests in `tests/test_distributed.rs` are now marked `#[ignore]` so they
are skipped in the default `cargo test` run and only activated by the `test-distributed` CI job.

---

## Summary Table

| ID | Feature | Status | Complexity | Notes |
| -- | ------- | ------ | ---------- | ----- |
| P0.1 | Shuffle disk spill | ✅ Done | Medium | `SpillableShuffleCache`, `Config::shuffle_spill_threshold` |
| P0.2 | Shuffle-map fault recovery | ✅ Done | High | Stage retry + `MapOutputTracker` invalidation |
| P0.3 | LRU eviction for PartitionStore | ✅ Done | Low | Default 1024 partitions; configurable |
| P0.4 | `unpersist()` API | ✅ Done | Low | `TypedRdd::unpersist()`, `is_cached()`, `collect_partitions()` |
| P0.5 | `MemoryAndDisk` / `DiskOnly` | ✅ Done | Medium | `persist_with_disk()`, bincode; lazy eviction spill TBD |
| P1.1 | Broadcast variables | ✅ Done | Medium | `BroadcastVar<T>`, embedded in `TaskEnvelope` |
| P1.2 | Accumulators | ✅ Done | Medium | `Accumulator<T>`, merged from `TaskResultEnvelope` |
| P1.3 | Object store (S3 only) | ✅ Done | Medium | `aws-sdk-s3`; `text_file` + `save_as_text_file`; `s3` feature |
| P1.4 | RDD checkpointing | ✅ Done | High | `CheckpointRdd`, `TypedRdd::checkpoint(dir)` |
| P1.5 | Speculative execution | ✅ Done | Medium | `Config::speculation_multiplier`; median-based straggler detection |
| P1.6 | Streaming shuffle DStream | ✅ Done | Medium | `reduce_by_key`, `group_by_key`, `join`, `left_outer_join` |
| P1.7 | Streaming checkpointing | ✅ Done | Medium | Wired to batch loop; `from_checkpoint()` restore |
| P1.8 | `updateStateByKey` | ✅ Done | High | `StateDStream`, `ReducedWindowedDStream` |
| P1.9 | Metrics endpoint | ✅ Done | Low | Prometheus `/metrics`, `Config::metrics_port` |
| P2.1 | DAG optimizer / pipeline fusion | ✅ Done | High | Filter push-down + `Config::coalesce_shuffle_threshold_bytes` |
| P2.2 | Adaptive partition coalescing | ✅ Done | Medium | Bucket-byte tracking; greedy merge; `ShuffledRdd` coalesced splits |
| P2.3 | Dynamic resource allocation | ✅ Done | High | Heartbeat + `remove_worker()` + `dynamically_add_worker()` |
| P3.1 | TLS for worker communication | ✅ Done | Medium | `tls` feature; `Executor::with_tls()`; `rustls`; opt-in |
| P3.2 | PyPI release pipeline | ✅ Done | Low | `.github/workflows/release-py.yml`; maturin; OIDC |
| P3.3 | npm release pipeline | ✅ Done | Low | `.github/workflows/release-js.yml`; napi-rs; 4 targets |
| P3.4 | Integration test suite in CI | ✅ Done | Medium | `.github/workflows/ci.yml`; 3 jobs; distributed tests `#[ignore]` |
| — | `task_fn!` intelligent op_id | ✅ Done | Low | `module::task_fn::Action<types>::8-hex`; stable across reformatting |
| — | Task registry startup validation | ✅ Done | Low | Panics on duplicate `op_id` with different handlers at startup |

---

## Known Remaining Gaps

These are within-scope items where the implementation is partial or has a known limitation:

| Gap | Description |
| --- | --- |
| `MemoryAndDisk` lazy eviction | `persist_with_disk()` writes all partitions eagerly at persist time; true write-on-LRU-eviction requires an eviction hook in `PartitionStore` |
| Shuffle HTTP TLS | Worker TCP task port is TLS-wrapped; `ShuffleManager` HTTP server is still plain HTTP |
| `ShuffleFetcher` transient retry | Network-level retry on temporary fetch failures not implemented (only stage-level retry on full failure) |
| Sort-based shuffle | Only hash partitioning; range-shuffle for globally sorted output not implemented |
| Streaming distributed receivers | `ReceiverTracker` is a local stub; Kafka / Kinesis sources not implemented |
| Python/JS `StreamingContext::start()` threading | Python uses `Python::attach` for GIL in background thread; JS `start()` is a no-op stub — background batch loop for JS deferred to Phase 5 |
| Python/JS accumulator custom merge | `Accumulator` supports int/float add, list append, string concat; user-defined merge closures deferred to Phase 5 |
| Python/JS streaming windowing | Windowed DStream operations (sliding/tumbling windows) deferred to Phase 5 |
| Python/JS streaming windowing | Windowed DStream operations (sliding/tumbling windows) deferred to Phase 5 |
| Python/JS streaming checkpoint | Streaming checkpoint/restore deferred to Phase 5 |
| `task_fn!` in production | The intelligent `task_fn!` op_id scheme is stable but `task_fn!` closures are best-effort for distributed use; `#[task(name = "…")]` is recommended for long-lived production tasks |
| `atomic-nlq` example / real-API test | NLQ pipeline is fully wired; `test_full_nlq_pipeline` test auto-skips if `ANTHROPIC_API_KEY` is absent; run with the key set to test end-to-end |
| `/register` HTTP endpoint | `dynamically_add_worker()` is callable in-process; a full `POST /register` HTTP route on the driver has not been added yet |
| Distributed CI test isolation | Distributed tests run sequentially via `Mutex` and bind fixed ports — flaky if ports are already in use in CI |

---

## Out of Scope (for now)

- Kerberos / SASL authentication
- HDFS connector (S3 covers the primary cloud use case)
- Web UI / dashboard (Prometheus + Grafana is the recommended approach)
