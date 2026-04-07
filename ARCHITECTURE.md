# Atomic — Architecture

## Summary

Atomic is a distributed data-processing framework for stable Rust, written as a redesign of [Vega](https://github.com/rajasekarv/vega). It keeps Vega's core ideas (RDD DAG, lazy transforms, shuffle) but replaces every mechanism that required nightly Rust or fragile runtime tricks.

---

## What Vega Got Right

Vega proved that Rust could express a Spark-like RDD model cleanly:
- Lazy transformation chains building a DAG of narrow and shuffle dependencies.
- Two-phase scheduling — shuffle-map stage, then result stage.
- Partition-level parallelism with a local fallback.

Atomic preserves all of this. The scheduler, DAG walker, stage planner, and shuffle infrastructure follow the same model.

---

## What Vega Got Wrong (and How Atomic Fixes It)

### 1. Nightly-only closure serialization

**Vega** shipped closures to workers by serializing Rust function pointers — only possible via unstable nightly intrinsics. This made Vega dependent on a specific nightly toolchain and unmaintainable once that toolchain moved.

**Atomic** replaces closure serialization entirely. Tasks are plain Rust functions annotated with `#[task]`. At link time, each function submits a `TaskEntry` via `inventory::submit!`. Workers build a `HashMap<op_id, handler>` at startup and dispatch by string lookup. No closures are ever serialized; only the `op_id` string and the rkyv-encoded partition data travel over the wire.

```
Driver:  encode(op_id="myapp::double", data=partition_bytes)  →  TCP  →  Worker
Worker:  TASK_REGISTRY.get("myapp::double")(action, payload, data)
```

Driver and worker run **the same binary**. The dispatch table is built at compile time — it cannot drift.

### 2. Unsafe / ad-hoc wire protocol

**Vega** used `serde` with dynamically selected codecs. Encoding/decoding bugs were only caught at runtime.

**Atomic** uses `rkyv` for all native wire payloads. rkyv's archived types can be validated with `bytecheck` before use, and the serializer is zero-copy on the read side. The same `WireEncode`/`WireDecode` traits are used everywhere; any type that derives the rkyv macros automatically gets a correct wire implementation.

### 3. Global mutable configuration via environment variables

**Vega** stored driver/worker configuration in process-global `OnceCell` variables populated from environment variables at any point during execution.

**Atomic** uses an explicit `Config` struct built at the program entry point and passed to `Context::new_with_config()`. Configuration is immutable after construction. Environment variables are still supported for legacy use, but new code uses the struct directly.

### 4. No stable UDF story

**Vega** had no mechanism for dynamic/non-Rust tasks.

**Atomic** adds first-class Python and JavaScript UDF support:
- Python UDFs are serialized with `pickle` and executed in an embedded PyO3 runtime on each worker.
- JavaScript UDFs are sent as source strings and evaluated by an embedded QuickJS runtime.
- UDFs follow the same `PipelineOp` dispatch path as native tasks — they are a first-class `TaskAction` variant, not a bolt-on.

---

## Core Architecture

```
Driver
  Context::parallelize_typed(data)
    └─ flat_map_task(F)     ┐
    └─ map_task(G)          │  StagedPipeline{source_partitions, ops:[FlatMap, Map]}
    └─ collect()            ┘  → dispatch_pipeline() → TCP → Worker

Worker
  NativeBackend::execute(TaskEnvelope)
    └─ for each op in pipeline:
        TASK_REGISTRY.get(op_id)(action, payload, data) → out_data
    └─ return TaskResultEnvelope{data: out_data}
```

### Key types

| Type | Location | Role |
|------|----------|------|
| `Context` | `atomic-compute` | Driver entry point; builds RDDs, dispatches jobs |
| `TypedRdd<T>` | `atomic-compute` | Type-safe RDD wrapper; accumulates lazy op pipeline |
| `StagedPipeline` | `atomic-compute` | Accumulated ops waiting to be dispatched as one envelope |
| `TaskEnvelope` | `atomic-data` | Wire type: one per partition; carries ops + encoded data |
| `NativeBackend` | `atomic-compute` | Worker-side op executor; looks up and runs handlers |
| `TASK_REGISTRY` | `atomic-compute` | Compile-time `HashMap<op_id, fn>` built via `inventory` |
| `LocalScheduler` | `atomic-scheduler` | In-process thread-pool scheduler |
| `DistributedScheduler` | `atomic-scheduler` | TCP dispatcher with capacity-aware placement |
| `ShuffleDependency` | `atomic-data` | Typed shuffle edge in the RDD DAG |
| `ShuffledRdd` | `atomic-compute` | Reduce-side RDD; calls `ShuffleFetcher::fetch` on compute |
| `ShuffleManager` | `atomic-data` | Per-worker HTTP server; serves `/shuffle/{id}/{map}/{reduce}` |
| `ShuffleFetcher` | `atomic-data` | Driver-side HTTP client; fetches shuffle data from workers |
| `MapOutputTracker` | `atomic-data` | Tracks which worker holds which shuffle bucket |
| `DashMapShuffleCache` | `atomic-data` | In-memory bucket store on each worker |

### Distributed shuffle flow

```
1. Driver detects ShuffleDependency in the RDD DAG
2. Driver encodes parent partitions as rkyv Vec<(K,V)> bytes
3. Driver sends ShuffleMap TaskEnvelope to each worker (one per input partition)
4. Worker: decode Vec<(K,V)> → hash(K) % N → write buckets to DashMapShuffleCache
5. Worker returns shuffle_server_uri (its ShuffleManager address) in TaskResultEnvelope
6. Driver registers all URIs with MapOutputTracker
7. Driver runs reduce stage locally: ShuffledRdd::compute → ShuffleFetcher::fetch (HTTP)
8. ShuffleFetcher reads (K,C) pairs from each worker, merges via Aggregator
```

### `#[task]` dispatch — why it works without nightly

```rust
// At compile time:
#[task]
fn double(x: i32) -> i32 { x * 2 }
// expands to:
inventory::submit!(TaskEntry { op_id: "myapp::double", handler: __double_dispatch });

// At startup:
TASK_REGISTRY = inventory::iter::<TaskEntry>.map(|e| (e.op_id, e.handler)).collect()

// At dispatch:
TASK_REGISTRY.get("myapp::double")(action, payload, data)
```

No nightly features. No unsafe transmutes. The function pointer stored in the registry is a plain `fn` item — stable and auditable.

---

## What Atomic Does NOT Do

- **No closure serialization.** Both the driver and the worker must run the same binary, and container-based task isolation is not a goal. For ad hoc logic in distributed tasks, use Python or JavaScript UDFs while experimenting. Once the implementation is stable, rewrite it in Rust and ship the Rust binary to workers with `atomic-cli`.
- **No Spark SQL / structured API.** Atomic works at the RDD level. There is no `DataFrame`, no query optimizer, and no SQL parser.
- **No streaming.** Micro-batch or continuous streaming is not implemented.
- **No adaptive query execution.** Partition sizes and parallelism are fixed at job submission.
- **No persistent shuffle storage.** Shuffle data lives in memory (`DashMapShuffleCache`). Worker restarts lose all shuffle state.
