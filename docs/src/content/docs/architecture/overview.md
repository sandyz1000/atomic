---
title: Architecture Overview
description: How the Atomic engine is structured and how a job flows from driver to workers.
---

Atomic runs partition-based jobs over a directed acyclic graph (DAG) of lazy
transformations. This page describes the parts of the engine and how a job
moves through them.

## Crates

| Crate | Responsibility |
|---|---|
| `atomic-data` | RDD traits, wire envelopes, dependency DAG, partitioners, shuffle primitives |
| `atomic-compute` | Context, executor, RDD implementations, task registry, persist/cache |
| `atomic-scheduler` | DAG building, stage planning, `LocalScheduler`, `DistributedScheduler` |
| `atomic-sql` | SQL and DataFrame layer on Apache DataFusion |
| `atomic-streaming` | Micro-batch streaming |
| `atomic-structured` | Continuous SQL/DataFrame queries with windows, joins, watermarks |
| `atomic-graph` | Graph processing: `Graph<VD, ED>`, Pregel, built-in algorithms |
| `atomic-nlq` | Natural-language query layer |
| `atomic-py` / `atomic-js` | Python and Node.js bindings |
| `atomic-cli` | Cross-compilation and binary distribution |
| `atomic-k8s` | Kubernetes per-job worker allocation |

## Execution flow

A driver builds an RDD, chains transformations, and calls an action. In local
mode the action runs on a thread pool. In distributed mode the chain is
accumulated into a staged pipeline and dispatched to workers, one task per
partition.

```text
Driver
  Context::parallelize_typed(data)
    └─ flat_map_task(F)     ┐
    └─ map_task(G)          │  StagedPipeline { source_partitions, ops: [FlatMap, Map] }
    └─ collect()            ┘  → dispatch over TCP → Worker

Worker
  NativeBackend::execute(TaskEnvelope)
    └─ for each op in pipeline:
        TASK_REGISTRY.get(op_id)(action, payload, data) → out_data
    └─ return TaskResultEnvelope { data: out_data, partition_id }
```

The driver and workers run the same binary. The task dispatch table is built at
compile time, so both sides agree on which identifier maps to which function.

## Key types

| Type | Crate | Role |
|---|---|---|
| `Context` | `atomic-compute` | Driver entry point; builds RDDs and dispatches jobs |
| `TypedRdd<T>` | `atomic-compute` | Type-safe RDD wrapper; accumulates the lazy op pipeline |
| `StagedPipeline` | `atomic-compute` | Accumulated ops waiting to be dispatched |
| `TaskEnvelope` | `atomic-data` | One per partition; carries ops plus encoded partition data |
| `TaskResultEnvelope` | `atomic-data` | Result or failure; carries `partition_id` for ordering |
| `NativeBackend` | `atomic-compute` | Worker-side op executor; routes each runtime to its dispatcher |
| `TASK_REGISTRY` | `atomic-compute` | Compile-time `HashMap<op_id, fn>` built with `inventory` |
| `LocalScheduler` | `atomic-scheduler` | Thread-pool scheduler with full DAG and shuffle support |
| `DistributedScheduler` | `atomic-scheduler` | TCP dispatcher with capacity-aware placement |

## Execution backend

There is one execution backend, `NativeBackend`, holding a registry of
dispatchers keyed by runtime:

| Runtime | File | Responsibility |
|---|---|---|
| `Native` | `runtimes/native.rs` | Compile-time `TASK_REGISTRY` lookup plus shuffle-map writes |
| `Python` | `runtimes/py.rs` | Embedded PyO3 worker pool |
| `JavaScript` | `runtimes/js.rs` | Embedded V8 (deno_core) runtime |

`NativeBackend::execute` orchestrates; each runtime's dispatch logic lives in
its own file. Adding a runtime means adding one dispatcher implementation and
one registry entry.

## Modes

- **Local** (`Config::local()`) — every partition runs on a thread pool in the
  current process.
- **Distributed** (`Config::builder().workers(...)`) — partitions are dispatched
  as `TaskEnvelope`s over TCP to remote workers. Each worker runs the same
  binary with `--worker --port N`.

## Related pages

- [Execution Model](/architecture/execution-model/) — tasks, staged pipelines, version safety
- [Shuffle](/architecture/shuffle/) — how data moves between stages
- [The RDD API](/concepts/rdd-api/) — the transformation and action surface
