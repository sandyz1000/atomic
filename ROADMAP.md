# Atomic — Production Readiness Roadmap

This document tracks what is needed to take Atomic from experimental research to a system that
can run real workloads reliably. ML/NLQ features (`atomic-nlq`) are excluded from this plan.

**Inspired by Spark's path to production**, the priorities below mirror the same issues Spark's
early adopters had to solve: reliable shuffle, bounded memory, fault recovery, observability,
and secure distribution.

---

## Current Status Summary

| Layer | State |
| ----- | ----- |
| Core RDD engine | Solid — `#[task]` dispatch, local + distributed, lazy pipeline staging |
| Shuffle | In-memory only; distributed worker registration done; no disk spill; incomplete fault recovery |
| SQL (`atomic-sql`) | Working — DataFusion-backed; Parquet/CSV/JSON; RDD-backed tables |
| Streaming (`atomic-streaming`) | Core batch loop works; local mode only; shuffle DStream and checkpointing not wired |
| Graph (`atomic-graph`) | Algorithms implemented; Pregel engine works locally |
| RDD Cache/Persist | MemoryOnly working; no LRU eviction; no disk path |
| Fault recovery | Per-task retry + resubmit; shuffle-map stage recompute incomplete |
| Observability | Structured logs only; no metrics endpoint; no web UI |
| Security | Plain TCP; no TLS; no auth |
| Distribution | `atomic-cli` cross-compile + SSH/SFTP ship works |
| Language bindings | Python (PyO3) + JS (V8/deno_core) UDFs work; `atomic-py` driver API works |

---

## Priority 0 — Correctness (Must Fix Before Any Production Use)

These are bugs or fundamental gaps that make the system unreliable.

### P0.1 — Shuffle Disk Spill

**Problem:** `DashMapShuffleCache` is pure in-memory. Large shuffles will OOM workers.

**What to build:**

- Add a `SpillableShuffleCache` that writes buckets to a temp directory when a configurable
  memory threshold is exceeded (e.g., 80% of `max_shuffle_memory_mb`).
- Use an OS-level atomic rename on spill completion to avoid partial reads.
- `ShuffleManager`'s HTTP handler reads from disk if the bucket is not in memory.
- Pluggable via a `ShuffleStore` trait: `MemoryShuffleStore` (existing), `DiskShuffleStore` (new).

**Files to touch:** `atomic-data/src/shuffle/`, `atomic-compute/src/rdd/shuffled.rs`

**Acceptance:** A word-count job on 10 GB of text completes without OOM.

---

### P0.2 — Complete Shuffle-Map Stage Fault Recovery

**Problem:** When a shuffle-map task fails (worker crash, network drop), the scheduler detects
the failure but does not correctly re-submit the map stage and recompute the lost buckets before
retrying the reduce stage.

**What to build:**

- On `ShuffleFetcher` fetch failure (after retry exhaustion), mark the shuffle stage as lost.
- Remove stale URIs from `MapOutputTracker` for the failed worker.
- Re-submit the full shuffle-map stage; wait for it to complete before re-running reduce.
- Ensure `DashMapShuffleCache` entries from the re-run overwrite stale ones.

**Files to touch:** `atomic-scheduler/src/local.rs`, `atomic-scheduler/src/stage.rs`,
`atomic-data/src/shuffle/`

**Acceptance:** Kill a worker mid-shuffle; the job completes (slowly) after the failed
partitions are recomputed.

---

### P0.3 — LRU Eviction for `PartitionStore`

**Problem:** `PartitionStore` is an unbounded `DashMap`. Caching large RDDs (or many RDDs in
sequence) will exhaust process memory.

**What to build:**
- Replace or wrap the inner `DashMap` with an LRU structure (e.g., `lru` crate or a hand-rolled
  `LinkedHashMap` with a `DashMap` front).
- Eviction policy: evict least-recently-used partitions when total tracked bytes exceed
  `partition_cache_max_mb` from `Config`.
- Add a `partition_store_size_bytes()` metric for observability.

**Files to touch:** `atomic-data/src/cache/mod.rs`

**Acceptance:** A job that caches 10 RDDs does not exhaust memory; evicted partitions
are recomputed on next access.

---

### P0.4 — `unpersist()` API

**Problem:** There is no way to explicitly release cached partitions; once cached, an RDD holds
memory until the process exits.

**What to build:**

- `TypedRdd::unpersist()` — calls `PartitionStore::remove_rdd(rdd_id, num_partitions)`.
- The corresponding `CachedRdd` should mark itself as uncached so subsequent actions recompute.

**Files to touch:** `atomic-compute/src/rdd/cached.rs`, `atomic-compute/src/rdd/typed.rs`

---

### P0.5 — `MemoryAndDisk` Storage Level

**Problem:** `StorageLevel::MemoryAndDisk`, `MemoryOnlySer`, and `DiskOnly` all silently fall
back to `MemoryOnly`. Users who set these expecting spill behavior get silent OOM instead.

**What to build:**

- `MemoryAndDisk`: store in `PartitionStore`; on eviction, serialize to a temp file and read
  back from disk on next access.
- `DiskOnly`: skip memory tier entirely; serialize partition to disk; `CachedRdd::compute`
  reads from disk every time.
- Serialization format for disk tier: `rkyv` (native path) or `bincode` (simpler for generic T).

**Files to touch:** `atomic-data/src/cache/mod.rs`, `atomic-compute/src/rdd/cached.rs`

---

## Priority 1 — Reliability and Completeness

These gaps make Atomic unsuitable for sustained workloads or multi-user clusters.

### P1.1 — Broadcast Variables

**Problem:** Every task payload currently includes a full copy of any driver-side constant
(lookup table, model weights, config). For large constants this wastes network bandwidth
and per-partition memory.

**What to build:**
- `Context::broadcast(value: T) -> Broadcast<T>` — serializes `T` once, assigns a `broadcast_id`.
- Driver sends `BroadcastEnvelope` to each worker on first reference (lazy or eagerly on `broadcast()`).
- Workers cache the value in a `DashMap<broadcast_id, Arc<dyn Any>>`.
- `Broadcast<T>::value()` on the worker returns the cached `Arc<T>`.
- `TaskEnvelope` carries `broadcast_ids: Vec<u64>` for values needed by the pipeline; worker
  fetches missing ones from the driver's broadcast HTTP server.

**Files to touch:** `atomic-data/src/distributed.rs`, `atomic-compute/src/context.rs`,
new `atomic-data/src/broadcast.rs`

---

### P1.2 — Accumulators

**Problem:** There is no mechanism for tasks to report side metrics (record counts, error counts,
custom aggregates) back to the driver.

**What to build:**

- `Context::accumulator(zero: T) -> Accumulator<T>` — driver-side handle with `value()` accessor.
- Workers call `acc.add(delta)` inside tasks; deltas are collected in `TaskResultEnvelope`.
- After each stage, the scheduler merges all deltas into the driver-side accumulator.
- Built-in: `LongAccumulator`, `DoubleAccumulator`, `CollectionAccumulator`.

**Files to touch:** `atomic-data/src/distributed.rs`, `atomic-scheduler/src/local.rs`,
`atomic-compute/src/context.rs`, new `atomic-data/src/accumulator.rs`

---

### P1.3 — Object Store Integration (S3 / GCS / Local)

**Problem:** Atomic has no connectors to cloud storage or HDFS. All input/output is local
filesystem only. This blocks running on ephemeral compute (cloud VMs, containers).

**What to build:**

- Integrate the [`object_store`](https://github.com/apache/arrow-rs/tree/master/object_store)
  crate (already a transitive dependency via DataFusion).
- `AtomicStore` trait: `get(path) -> Bytes`, `put(path, bytes)`, `list(prefix) -> Vec<Path>`.
- Implementations: `LocalStore` (wraps `object_store::local`), `S3Store` (wraps
  `object_store::aws`), `GcsStore` (wraps `object_store::gcp`).
- `Context::text_file(url: &str) -> TypedRdd<String>` — reads lines from any `AtomicStore` URL.
- Shuffle disk spill (P0.1) should use `AtomicStore` for its spill path so shuffle data can
  live in S3 for multi-node jobs.

**Files to touch:** new `crates/atomic-store/`, `atomic-compute/src/io/`

---

### P1.4 — RDD Checkpointing (Lineage Truncation)

**Problem:** Long RDD lineage chains (e.g., iterative algorithms — PageRank, k-means) cause the DAG to grow unboundedly. Any failure requires recomputing from the original source.

**What to build:**

- `TypedRdd::checkpoint(path: &str)` — materializes the RDD to an `AtomicStore` path and
  replaces the RDD's lineage with a `CheckpointRdd` pointing to that path.
- `CheckpointRdd::compute(partition)` reads directly from the checkpoint path; no parent DAG.
- Checkpointing must happen before the action that triggers it (two-pass: compute → checkpoint
  → replace lineage → continue).
- `StreamingContext` should call `checkpoint()` on stateful DStreams each batch.

**Files to touch:** `atomic-compute/src/rdd/`, `atomic-streaming/src/context.rs`,
new `atomic-compute/src/rdd/checkpoint.rs`

---

### P1.5 — Speculative Execution

**Problem:** Slow tasks ("stragglers") block a stage from completing. Spark re-runs slow tasks
speculatively on a second worker; whichever finishes first wins.

**What to build:**

- Track per-task wall-clock time in the scheduler.
- If a task has been running longer than `speculative_multiplier × median_task_time`, submit an
  identical copy to a different worker.
- Accept the first result; cancel (via a cancellation token) the slower copy.
- Speculative tasks are not counted as failures.

**Files to touch:** `atomic-scheduler/src/local.rs`, `atomic-scheduler/src/stage.rs`

---

### P1.6 — Streaming: Shuffle DStream (reduce_by_key over batches)

**Problem:** `ShuffledDStream` is scaffolded but not wired. Stateless `reduceByKey` within a
batch (a core Spark Streaming primitive) does not work.

**What to build:**

- `ShuffledDStream::compute(time_ms)` must get the parent RDD, call `reduce_by_key` on it
  (which triggers `ShuffledRdd` + two-phase shuffle via `LocalScheduler`), and return the result.
- Wire `PairDStream::reduce_by_key(f)` to produce a `ShuffledDStream`.

**Files to touch:** `atomic-streaming/src/dstream/shuffle.rs`,
`atomic-streaming/src/dstream/pair.rs`

---

### P1.7 — Streaming: Checkpointing Wired to Batch Loop

**Problem:** `Checkpoint` type and serialization exist but are never written by the batch loop.
A streaming job that crashes loses all in-memory state.

**What to build:**

- After each batch completes, `JobScheduler` calls `StreamingContext::checkpoint()`.
- Checkpoint captures: `zero_time_ms`, list of registered output operations, per-DStream
  generated-batch metadata.
- `StreamingContext::from_checkpoint(path)` restores the context and resumes the batch loop.

**Files to touch:** `atomic-streaming/src/checkpoint.rs`, `atomic-streaming/src/context.rs`,
`atomic-streaming/src/scheduler/job.rs`

---

### P1.8 — Streaming: `updateStateByKey` / `mapWithState`

**Problem:** Stateful streaming (tracking user sessions, running counts, etc.) requires
maintaining state across batches. This is not implemented.

**What to build:**

- `PairDStream::update_state_by_key(update_fn)` — produces a `StateDStream` that carries a
  state `RDD<(K, S)>` across batches, merging new values with existing state on each tick.
- State RDD is checkpointed (requires P1.7).
- `mapWithState` variant for ergonomic per-record state access.

**Files to touch:** new `atomic-streaming/src/dstream/state.rs`

---

### P1.9 — Basic Observability (Metrics Endpoint)

**Problem:** There is no way for operators to monitor running jobs without attaching a debugger
or grepping logs. Stage timing, task counts, shuffle read/write sizes, and cache hit rates are
invisible.

**What to build:**

- A lightweight HTTP `/metrics` endpoint on the driver (Prometheus text format is simplest).
- Key metrics:
  - `atomic_stage_duration_seconds{stage_id, status}` (histogram)
  - `atomic_task_count{stage_id, status}` (counter: success / retry / failed)
  - `atomic_shuffle_bytes_written{shuffle_id}`, `atomic_shuffle_bytes_read{shuffle_id}`
  - `atomic_cache_hits{rdd_id}`, `atomic_cache_misses{rdd_id}`
  - `atomic_partition_cache_bytes` (gauge)
- `SparkListener`-equivalent event bus: `JobStarted`, `JobEnded`, `StageCompleted`,
  `TaskEnded` — the `LiveListenerBus` stub already exists in `atomic-scheduler`.

**Files to touch:** `atomic-scheduler/src/listener.rs`, new `atomic-compute/src/metrics.rs`

---

## Priority 2 — Performance and Scalability

These are needed before Atomic can handle large-scale production workloads.

### P2.1 — DAG Optimizer (RDD Level)

**Problem:** Every `_task` call creates a separate stage boundary in the RDD DAG. Adjacent
narrow transforms (map → filter → map) could be fused into a single pass without writing
intermediate results to memory.

**What to build:**

- **Pipeline fusion**: merge adjacent narrow-dependency stages into a single `StagedPipeline`
  (already partially done by the `StagedPipeline` mechanism — extend it to cross `map_task` chains).
- **Partition pruning**: if a filter is applied before a wide dependency, push it as early as
  possible in the DAG.
- **Stage coalescing**: after a wide dependency, if the output partition count is much larger
  than the input, automatically coalesce.

**Files to touch:** `atomic-scheduler/src/dag.rs`, `atomic-compute/src/rdd/typed.rs`

---

### P2.2 — Adaptive Partition Coalescing

**Problem:** After a shuffle, the number of reduce partitions is fixed at job submission.
If the shuffle output is small (e.g., heavy filtering before shuffle), the fixed partition
count causes many nearly-empty reduce tasks.

**What to build:**

- After shuffle-map stage completes, inspect bucket sizes from `MapOutputTracker`.
- Automatically merge adjacent small buckets into combined partitions before starting the
  reduce stage (same approach as Spark's Adaptive Query Execution for shuffle).
- Configurable threshold: `min_partition_bytes` and `max_partition_bytes` in `Config`.

**Files to touch:** `atomic-scheduler/src/local.rs`, `atomic-data/src/shuffle/`

---

### P2.3 — Dynamic Resource Allocation

**Problem:** The worker list is fixed at driver startup. There is no mechanism to add or remove workers as load changes.

**What to build:**

- Driver polls registered workers with a heartbeat (e.g., every 5 s).
- Workers that miss N heartbeats are removed from the active pool.
- New workers can register with the driver via a `/register` HTTP endpoint.
- Shuffle-map outputs on a removed worker are marked lost, triggering P0.2 recompute.

**Files to touch:** `atomic-compute/src/hosts.rs`, `atomic-scheduler/src/base.rs`

---

## Priority 3 — Security and Release

These are required before Atomic can be deployed in a multi-tenant or internet-facing environment.

### P3.1 — TLS for Worker Communication

**Problem:** Driver ↔ worker TCP communication is plain text. An attacker on the same network
can inject task results or read shuffle data.

**What to build:**

- Wrap the TCP listener/connector in `rustls` (pure Rust TLS, no OpenSSL dependency).
- Generate or load a self-signed cert per worker; driver verifies against a known-good
  fingerprint list in `Config`.
- Shuffle HTTP (`ShuffleManager`) should also be upgraded to HTTPS.

**Files to touch:** `atomic-compute/src/executor.rs`, `atomic-data/src/shuffle/`

---

### P3.2 — PyPI Release Pipeline

**Problem:** Python users must clone the repo and run `maturin develop` manually.

**What to build:**

- GitHub Actions workflow: on tag push, run `maturin build --release` for
  `linux/amd64`, `linux/arm64`, `macos/arm64`, and upload wheels to PyPI via `maturin publish`.
- Minimum viable `atomic-py` package: `Context`, `RDD`, `map`, `filter`, `reduce`, `collect`.

**Files to touch:** `.github/workflows/release-py.yml`, `crates/atomic-py/`

---

### P3.3 — npm Release Pipeline

**Problem:** JavaScript users have no published package.

**What to build:**

- Build `atomic-js` as a native Node.js addon using `napi-rs` (already implemented).
- GitHub Actions workflow: build for `linux-x64`, `darwin-arm64`; publish to npm as
  `@atomic-compute/js`.

**Files to touch:** `.github/workflows/release-js.yml`, `crates/atomic-js/`

---

### P3.4 — Integration Test Suite

**Problem:** The distributed integration test (`test_distributed`) requires a manually compiled binary and is not run in CI. There are no tests for shuffle correctness at scale, fault recovery, or streaming.

**What to build:**

- CI-runnable distributed test: `cargo test -p atomic` — auto-builds the integration binary
  and runs driver + worker in the same process via threads (or child processes as today, but in CI).
- Shuffle correctness test: large `reduce_by_key` job that verifies key counts exactly.
- Fault recovery test: kill a worker mid-job; verify the job still produces correct results.
- Streaming smoke test: push 100 batches through `QueueInputDStream`; verify all records arrive.
- Graph algorithm tests: PageRank on a known graph; verify scores match a reference implementation.

**Files to touch:** `tests/`, `.github/workflows/ci.yml`

---

## Summary Table

| ID | Feature | Priority | Complexity | Depends On |
| -- | ------- | -------- | ---------- | ---------- |
| P0.1 | Shuffle disk spill | Critical | Medium | — |
| P0.2 | Shuffle-map fault recovery | Critical | High | — |
| P0.3 | LRU eviction for PartitionStore | Critical | Low | — |
| P0.4 | `unpersist()` API | Critical | Low | P0.3 |
| P0.5 | `MemoryAndDisk` storage level | Critical | Medium | P0.3 |
| P1.1 | Broadcast variables | High | Medium | — |
| P1.2 | Accumulators | High | Medium | — |
| P1.3 | Object store (S3/GCS) | High | Medium | — |
| P1.4 | RDD checkpointing | High | High | P1.3 |
| P1.5 | Speculative execution | High | Medium | — |
| P1.6 | Streaming shuffle DStream | High | Medium | — |
| P1.7 | Streaming checkpointing | High | Medium | P1.4 |
| P1.8 | `updateStateByKey` / `mapWithState` | High | High | P1.7 |
| P1.9 | Metrics endpoint | High | Low | — |
| P2.1 | DAG optimizer / pipeline fusion | Medium | High | — |
| P2.2 | Adaptive partition coalescing | Medium | Medium | — |
| P2.3 | Dynamic resource allocation | Medium | High | — |
| P3.1 | TLS for worker communication | Medium | Medium | — |
| P3.2 | PyPI release pipeline | Medium | Low | — |
| P3.3 | npm release pipeline | Medium | Low | — |
| P3.4 | Integration test suite in CI | High | Medium | — |

---

## Out of Scope (for now)

- `atomic-nlq` (NLQ / LLM analytics layer) — deferred; see `notes/nl-to-ir.md`
- Kerberos / SASL authentication
- HDFS connector (S3 via `object_store` covers the primary cloud use case)
- Web UI / dashboard (Prometheus + Grafana is the recommended approach once P1.9 is done)
- Spark SQL compatibility layer (the goal is a clean API, not Spark SQL wire compatibility)
