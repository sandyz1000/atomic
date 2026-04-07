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

## RDD API Convention

### The `_task` family — canonical API for local and distributed execution

All narrow transforms and reductions have a `_task` variant. Always use these:

| Method | Trait required | TaskAction |
|---|---|---|
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

// Inline — preferred for one-off lambdas
rdd.map_task(task_fn!(|x: i32| -> i32 { x * 2 }))
```

`task_fn!` generates a zero-sized struct with a stable `file!:line!:col!` op_id registered via `inventory::submit!` — identical to `#[task]` at the dispatch level.

### How the staged pipeline works

```
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
- `task_fn!` macro for inline task lambdas — identical to `#[task]` at dispatch level.
- `NativeBackend` — executes task pipelines by op_id lookup.
- `LocalScheduler` — full DAG/stage/shuffle support, thread-pool execution.
- `DistributedScheduler` — TCP dispatch, capacity-aware placement.
- rkyv distributed envelope types (`TaskEnvelope`, `TaskResultEnvelope`, `WorkerCapabilities`).
- Lazy shuffle pipeline (`ShuffleDependency` + `ShuffledRdd` + `Aggregator`).
- `DashMapShuffleCache` + `ShuffleManager` HTTP server.
- `ShuffleFetcher` + `MapOutputTracker`.
- `partition_id` in `TaskResultEnvelope` for correct result ordering after retries.
- Python UDF support (PyO3 / pickle) and JavaScript UDF support (QuickJS).
- Unified `_task` API: `map_task`, `filter_task`, `flat_map_task`, `fold_task`, `reduce_task` — work identically in local and distributed mode.
- All action methods (`collect`, `count`, `take`, `first`, `reduce`, `fold`, `aggregate`, `for_each`, `for_each_partition`, `count_by_value`, `is_empty`, `top`, `take_ordered`, `max`, `min`) dispatch staged pipelines to workers in distributed mode.
- `AtomicApp::build()` — unified entry point; reads `--worker`/`--workers`/`--local-ip` from CLI.
- Explicit `Config` struct at entry point — replaces global `OnceCell<Configuration>` env-var reading.
- Distributed integration test (driver + real worker over TCP, `cargo test -p atomic-tests test_distributed`).

### Not Done Yet

- Distributed shuffle end-to-end: each worker needs to run its own `ShuffleManager` and register its URI with the driver's `MapOutputTracker`.
- `ShuffleFetcher` retry on transient network errors.
- Failed shuffle-map stage recompute / fault recovery.

## Guardrails For Future Changes

- Reuse Vega's DAG, stage, and shuffle ideas when they fit.
- Do not copy Vega's closure serialization model.
- Do not add Docker or WASM backends — the project uses `NativeBackend` only.
- Keep local and distributed execution paths conceptually aligned.
- `#[task]` functions are the only way to register distributed work.
- Prefer explicit backend routing and compile-time dispatch over dynamic runtime guessing.
