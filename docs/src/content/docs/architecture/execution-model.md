---
title: Execution Model
description: How tasks are registered, dispatched, and protected against binary mismatch.
---

Distributed work in Atomic is a set of compiled functions registered at compile
time and dispatched by string identifier. This page explains how that works and
how the engine prevents a stale worker from producing wrong results.

## Tasks

A task is a plain Rust function annotated with `#[task]`:

```rust
use atomic_compute::task;

#[task]
fn double(x: i32) -> i32 { x * 2 }

rdd.map_task(Double).collect()?;
```

The macro generates a zero-sized struct (`Double`) and submits a `TaskEntry`
through `inventory::submit!`. At startup the engine collects every entry into a
`HashMap<op_id, handler>` named `TASK_REGISTRY`. At dispatch time the worker
looks up the op identifier and calls the handler:

```rust
TASK_REGISTRY.get("myapp::double::a1b2c3d4")(action, payload, data)
```

No closures are serialized. Only the op identifier and the `rkyv`-encoded
partition data travel over the wire.

### Inline tasks

For one-off functions, `task_fn!` produces the same dispatch entry with a
content-hash identifier:

```rust
rdd.map_task(task_fn!(|x: i32| -> i32 { x * 2 }));
```

The identifier is derived from the closure's normalized token text, so it stays
stable across line-number shifts and reformatting, and changes only when the
body changes.

### Captured state

A task can carry data by being a struct with fields:

```rust
#[task]
struct FilterAbove { threshold: f64 }

impl UnaryTask<f64, bool> for FilterAbove {
    fn call(&self, x: f64) -> bool { x > self.threshold }
}

let threshold = compute_on_driver(&data);
rdd.filter_task(FilterAbove { threshold });   // fields encoded in the op payload
```

## Staged pipelines

In distributed mode, narrow transformations do not execute immediately. Each
`_task` call appends an op to a `StagedPipeline`:

```text
parallelize_typed(data, n)
  └─ flat_map_task(Tokenize)  → StagedPipeline { source, ops: [FlatMap] }
       └─ map_task(PairOne)   → StagedPipeline { source, ops: [FlatMap, Map] }
            └─ collect()      → dispatch full chain to workers, collect on driver
```

When an action fires, the whole op chain is sent as one `TaskEnvelope` per
partition. The action methods (`collect`, `count`, `take`, `reduce_task`,
`fold_task`, and the rest) check for a staged pipeline and dispatch it.

## Binary version safety

The driver and workers run the same binary, but a worker can be left running an
older build. Two mechanisms catch that.

**Op identifier body hash.** The last segment of every op identifier is a short
FNV-1a hash of the function body tokens, for example `myapp::double::a1b2c3d4`.
If the body changes, the identifier changes, and a worker running the old build
logs a "task not found" error instead of running stale code.

**Registry fingerprint.** `REGISTRY_FINGERPRINT` is a single `u64` computed at
startup by folding every sorted `(op_id, body_hash)` pair with FNV-1a. Workers
advertise their fingerprint in `WorkerCapabilities` during the TCP handshake.
The scheduler compares it against the driver's fingerprint when a worker
registers:

```text
driver fingerprint = 0xdeadbeef...
worker advertises  = 0xdeadbeef...  → proceed
worker advertises  = 0xbadcafe0...  → log error: fingerprint mismatch, redeploy workers
```

The check runs at registration, before any task is dispatched, so a mismatched
worker fails early rather than silently diverging.

## Cross-language tasks

Python and JavaScript tasks travel with the job and need no recompilation:

- Python functions are serialized with `cloudpickle` and run on the worker's
  embedded PyO3 runtime.
- JavaScript functions are captured as source text and evaluated by the
  embedded V8 runtime.

Both follow the same per-op dispatch path as native Rust tasks. Use a Rust
`#[task]` when you want native speed and the compile-time dispatch guarantee;
use the Python or JavaScript path for dynamic logic that changes often.

| Change | Requires worker redeploy? |
|---|---|
| Modify a Python task | No — sent in the task envelope |
| Modify a JavaScript task | No — sent in the task envelope |
| Modify a Rust `#[task]` | Yes — compiled into the binary |
| Add scheduler or shuffle code | Yes — compiled into the binary |

## Persist and cache

`cache()` and `persist(level)` wrap an RDD in a `CachedRdd<T>`. The first action
computes each partition and stores it; later actions read from the store instead
of recomputing the DAG.

```text
rdd.map_task(Double).cache()

action 1: collect()
  CachedRdd::compute(p0) → miss → compute → store → return
action 2: count()
  CachedRdd::compute(p0) → hit  → return from store, no recompute
```

The cached type needs only `Data + Clone + 'static` — no serialization bounds.
The partition store is a process-wide singleton keyed by `(rdd_id, partition)`;
RDD identifiers are globally unique, so multiple contexts in one process share
the store safely.
