# Atomic Project Notes

## Project Goal

Atomic is a stable-Rust rewrite and refactor of Vega.

- Preserve the useful high-level execution model from Vega (RDD DAG, lazy transforms, shuffle).
- Remove dependence on unstable Rust features and closure serialization.
- Replace trait-object function shipping with explicit compile-time task registration.
- Use rkyv for distributed wire payloads.

## Repository Structure

- `crates/atomic-data`: shared types — RDD traits, task envelopes, distributed structs, shuffle primitives, dependency DAG.
- `crates/atomic-compute`: execution runtime — context, executor, `NativeBackend`, UDF dispatch, RDD implementations.
- `crates/atomic-scheduler`: DAG building, stage planning, job tracking, `LocalScheduler`, `DistributedScheduler`.
- `crates/atomic-utils`: shared utilities.
- `notes/`: architecture notes and design documents.

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

There is one execution backend: `NativeBackend`. It looks up each `op_id` in the compile-time `TASK_REGISTRY` and calls the registered handler. Docker and WASM backends are not part of this project.

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
- `WorkerCapabilities`: advertised per-worker limits (`max_tasks`); scheduler skips workers at capacity.

## Current Implementation State

### Done

- `#[task]` proc-macro + `TASK_REGISTRY` compile-time dispatch.
- `NativeBackend` — executes task pipelines by op_id lookup.
- `LocalScheduler` — full DAG/stage/shuffle support, thread-pool execution.
- `DistributedScheduler` — TCP dispatch, capacity-aware placement.
- rkyv distributed envelope types (`TaskEnvelope`, `TaskResultEnvelope`, `WorkerCapabilities`).
- Lazy shuffle pipeline (`ShuffleDependency` + `ShuffledRdd` + `Aggregator`).
- `DashMapShuffleCache` + `ShuffleManager` HTTP server.
- `ShuffleFetcher` + `MapOutputTracker`.
- `partition_id` in `TaskResultEnvelope` for correct result ordering after retries.
- Python UDF support (PyO3 / pickle) and JavaScript UDF support (QuickJS).

### Not Done Yet

- Distributed shuffle end-to-end: each worker needs to run its own `ShuffleManager` and register its URI with the driver's `MapOutputTracker`.
- `ShuffleFetcher` retry on transient network errors.
- Failed shuffle-map stage recompute / fault recovery.
- Distributed integration test (driver + real worker over TCP).

## Guardrails For Future Changes

- Reuse Vega's DAG, stage, and shuffle ideas when they fit.
- Do not copy Vega's closure serialization model.
- Do not add Docker or WASM backends — the project uses `NativeBackend` only.
- Keep local and distributed execution paths conceptually aligned.
- `#[task]` functions are the only way to register distributed work.
- Prefer explicit backend routing and compile-time dispatch over dynamic runtime guessing.
