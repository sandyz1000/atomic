# Scheduler Unification — Design Proposal

**Status:** Proposal, under review. Not yet implemented.
**Scope:** `atomic-scheduler`, `atomic-runtime-macros`, `atomic-data` (`distributed` module), `atomic-compute` (`context`, `runtimes`).

## Summary

Collapse the local and distributed schedulers onto one contract. Every unit of work
is a registered task op — there is no separate closure execution path. The task
definition (`#[task]` / `task_fn!`) owns the task's logic, its combinator shape, and
its payload. The scheduler plans a stage DAG and dispatches through a transport that
is either in-process (local) or over the wire (distributed).

## Current state

Three scheduler trait layers exist. Two are dead:

- `Scheduler` (`lib.rs`) — zero implementations. Its `run_job` signature does not match
  the code that runs jobs.
- `DAGScheduler` (`dag.rs`) — zero implementations. Only its data types
  (`CompletionEvent`, `TaskEndReason`, `FetchFailedVals`) are used.
- `NativeScheduler` (`base.rs`) — the one real trait. `LocalScheduler` implements it
  fully; `DistributedScheduler` stubs the parts that do not fit its model.

Runtime dispatch happens through the `Schedulers` enum, which bypasses all three traits
with an inherent `run_job`.

The two schedulers do not share an intermediate representation:

- `LocalScheduler` consumes the `Rdd`/`Stage` DAG and runs Rust closures in-process
  (`ResultTask { func: Arc<F> }`), driven by a `CompletionEvent` queue.
- `DistributedScheduler` consumes `Vec<PipelineOp>` plus serialized partition bytes and
  dispatches `TaskEnvelope`s. Stages are not the unit of work.

The difference is a design constraint, not drift: distributed dispatch ships registered
op IDs over rkyv and never serializes a closure. A closure that captures its environment
cannot become an op, so the closure API runs only on the driver.

Stage orchestration is duplicated. `atomic-scheduler/base.rs` splits stages for the
closure path; `atomic-compute/context/job_runner.rs` (`run_pending_shuffle_stages`) splits
stages for the distributed op path. `DistributedScheduler::submit_task` is a no-op, its
`run_job` returns `UnsupportedOperation`, and the `id_in_job` parameter threaded through
`submit_task` is unused by every implementation.

## Core idea

Two axes were tangled in one type hierarchy. Separate them:

- **Representation** is always an op. `#[task]` / `task_fn!` is the only way to define
  work. A capturing closure becomes a task plus rkyv-encoded arguments.
- **Transport** is in-process or over the wire. Both run the same
  `ComputeEngine::execute(&TaskEnvelope)`; the only difference is whether the envelope
  crosses a socket first. Speculation, fallback, and cancellation are wire concerns.

Everything else follows from making the task definition the single source of truth for a
task's shape and payload.

## Target architecture

```
DagScheduler        Rdd/Stage DAG, shuffle split, map-output tracking, retry
    |               (backend-agnostic)
    |  lower Stage -> TaskEnvelope (op_id + macro-derived shape + typed payload)
    v
Transport           the only backend seam
    |-- LocalTransport : ComputeEngine::execute in-process
    +-- WireTransport  : ship envelope, worker runs the same ComputeEngine::execute
                         (owns speculation, fallback, cancellation, heartbeat)
```

`DagScheduler` builds stages once for both transports. `LocalTransport` runs a stage's
tasks in-process. `WireTransport` lowers each task to a `TaskEnvelope` and dispatches it;
a task that cannot be lowered (a capturing closure) is rejected at plan time rather than
silently skipped.

Both transports return a `TaskResultEnvelope` into one `CompletionEvent` stream, which
the `DagScheduler` consumes to advance the DAG.

## Task definition as the source of truth

### Shape (replaces `TaskAction`)

`TaskAction` currently mixes two unrelated concerns in one enum:

- **Combinator shape** — `Map`, `Filter`, `FlatMap`, `Fold`, `Reduce`, `Aggregate`,
  `Collect`. These are derivable from the task function signature, which the macro already
  inspects (argument count and return type). Today they are hand-selected at more than 60
  construction sites.
- **Engine primitives** — `ShuffleMap`, `Cache`, `ReadFileSplit`, `MergeState`,
  `AgentStep`, `KafkaConsume`. These carry no user task function; they run built-in logic.

Split the enum:

- The combinator shape becomes metadata the macro derives from the signature and attaches
  to the registry entry. A registered task knows whether it is a map, a filter, or a
  combiner. Adding a combinator means teaching the macro one signature pattern, in one
  place, instead of adding an enum variant plus construction sites.
- The engine primitives move to a separate `StepKind` enum, owned by the engine. New
  engine steps (a write/sink op, for example) are added here.

One function can serve more than one shape: a combiner `fn(T, T) -> T` is used by both
`Fold` (with a zero) and `Reduce` (without one), and the choice is the call site
(`fold_task` vs `reduce_task`). Where the shape genuinely varies by call site, the op
carries a small selector (for example `has_zero`, or `with_index` for partition ops)
rather than a hand-written action.

### Payload (typed, task-owned)

`PipelineOp.payload` is a `Vec<u8>` whose meaning depends on the action: an rkyv-encoded
zero for `Fold` and `Aggregate`, a JSON `AgentStepPayload` for `AgentStep`, a bincode
`KafkaConsumePayload` for `KafkaConsume`, empty for `Map`. The encoding is written at each
call site and decoded per-action at the dispatcher.

Make the payload a typed value the task declares, with a macro-generated codec. A task
with bound parameters declares a parameters struct; the macro encodes it at the call site
and decodes it on the worker. The fold zero is one such parameter. Engine primitives
declare their own payload type the same way.

## Capturing closures in distributed mode

The invariant: ship data, never code. A capturing closure is split into registered logic
plus its captured values as serialized arguments.

The transport slot already exists. `PipelineOp.payload` is present, and the worker
dispatch function `__atomic_dispatch_<fn>` already receives a payload byte slice — today
only fold zeros and Python/JS configs use it.

Two mechanisms, layered:

1. **Task with parameters.** `#[task]` emits a struct with fields for the bound
   parameters; the fields are rkyv-encoded into the payload at the call site and decoded on
   the worker.

   ```rust
   #[task]
   fn scale(x: i32, factor: i32) -> i32 { x * factor }
   rdd.map_task(Scale::new(10)); // 10 is rkyv-encoded into the payload
   ```

2. **Capture-lifting.** `task_fn!` takes an explicit capture list, lifts those variables
   into the parameters struct, and encodes them into the payload. The capturing closure
   then works in distributed mode because it desugars to mechanism 1.

   ```rust
   let factor = 10;
   rdd.map_task(task_fn!([factor] |x: i32| -> i32 { x * factor }));
   ```

   Bound parameters require the rkyv traits. The capture list is explicit so the check is
   local and honest about what crosses the wire.

Values that cannot be serialized (a database handle, a socket) do not travel as
parameters. A large read-only value uses the broadcast-variable path (encode once, cache
per worker, read by id). A non-serializable resource uses worker-side lazy initialization
keyed by a config value that is shipped as a parameter.

This is the model rusty-celery uses for task arguments (a serializable parameters struct
generated by the task macro), extended with capture-lifting, which rusty-celery does not
provide.

## Missing actions

Transforms that need a task function but have no combinator shape today, so they cannot
dispatch in distributed mode:

| Missing | Note |
| --- | --- |
| `MapPartitions`, `MapPartitionsWithIndex` | whole-partition function; today a driver-only closure RDD |
| `MapValues`, `FlatMapValues` | pair value-only, preserves the partitioner |
| `MapPartitionsToPair` | partition to pairs |
| `Foreach`, `ForeachPartition` | side-effecting terminal, returns no data |
| `Glom` | partition to `Vec<T>`; a special case of `MapPartitions` |
| `Sample`, `Distinct` | sample is a seeded filter; distinct is shuffle plus dedup |
| Write/Sink (file, object store) | symmetric to `ReadFileSplit`; writes are driver-only today |

Once the macro derives shape from the signature, the combinator entries are added as
signature patterns in the macro. Write/Sink is added to `StepKind`. `zip`, `union`, and
`cartesian` are dependency-structural, not per-element task functions, and stay out of the
shape set.

## Phased plan

Each phase compiles and passes the test suite on its own.

| Phase | What | Risk | Depends on |
| --- | --- | --- | --- |
| 1. Dead-code and rename | Remove the `Scheduler` trait, the `DAGScheduler` trait skeleton (keep its data types), the unreachable `Schedulers::run_job` distributed arm and `UnsupportedOperation` stub, the `id_in_job` parameter and its double `enumerate`. Rename `Mutators` to `SchedulerState`. | None; no behavior change | — |
| 2. Task as source of truth | Macro derives combinator shape from the signature. Split `TaskAction` into macro-derived shape plus a `StepKind` engine enum. Make the payload a typed, task-owned value with a generated codec. | Medium; touches the macro and the wire schema | 1 |
| 3. Parameters and capture | `#[task]` structs with fields encode into the payload. `task_fn!` gains capture-lifting. Capturing closures gain an op equivalent. | Medium | 2 |
| 4. Fill combinator gaps | Add `MapPartitions` (and with-index), `MapValues`, `FlatMapValues`, `Foreach` as macro signature patterns. Add Write/Sink to `StepKind`. | Low once phase 2 lands | 2 |
| 5. One DagScheduler | Extract DAG planning into `planner.rs`. `DagScheduler` owns stage lifecycle, map-output tracking, and retry. Move distributed shuffle-stage orchestration out of `atomic-compute` into the scheduler. | High; cross-crate, on the hot path | 1 |
| 6. Transport seam | Add the `Transport` trait. `LocalTransport` runs in-process; `WireTransport` wraps the current distributed dispatch (speculation, fallback, cancellation). Add `lowering.rs` for `Stage` to `TaskEnvelope`. Unify completion into one async `CompletionEvent` channel and remove the polling loop. | High | 3, 5 |
| 7. Retire the closure IR | `.map` / `.filter` / `.reduce` closure syntax becomes sugar over `task_fn!`. Remove the `ResultTask.func` path and the `NativeScheduler` remnants. `LocalScheduler` becomes `DagScheduler` plus `LocalTransport`. | Medium | 3, 6 |

Order: 1, 2, 3, 4, 5, 6, 7. Phase 1 first, to shrink the surface at zero risk. Phase 2 is
the keystone: once the task owns its shape and payload, the downstream lowering is
mechanical.

## Decisions

Settled:

- Op-only representation. Closure syntax survives as sugar; the closure execution path is
  removed.
- Transport is the only backend seam. Speculation, fallback, and cancellation live in
  `WireTransport`.
- The task definition owns shape and payload. `StepKind` holds the engine primitives.

Open:

1. **Non-serializable captures.** Either forbid them (require worker-side initialization
   plus a shipped config parameter), or keep a `LocalClosure` task variant that only
   `LocalTransport` accepts and `WireTransport` rejects at plan time. Recommendation: keep
   the `LocalClosure` variant — it is one task arm, not a second scheduler, and it
   preserves local prototyping.
2. **Payload layout.** Either extend a per-`StepKind` payload schema, or add a distinct
   `args` field on `PipelineOp` alongside config. Recommendation: a single typed
   parameters value that owns the whole payload, config included.

## Risks

- Phases 5 and 6 move orchestration across crate boundaries and touch the dispatch hot
  path. They must preserve the map-output recovery hook, job-abort handling, adaptive
  shuffle coalescing, cancellation, and speculation.
- Phase 2 changes the wire schema (`TaskAction` split, typed payload). Worker and driver
  binaries must be rebuilt together; the registry fingerprint already guards mismatched
  binaries.
- Capture-lifting requires the rkyv traits on captured values. The macro must produce a
  clear compile error when a captured value lacks them.
