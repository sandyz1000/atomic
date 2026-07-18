# Scheduler Unification — Design Proposal

**Status:** Proposal, under review. Not yet implemented.
**Scope:** `atomic-scheduler`, `atomic-runtime-macros`, `atomic-data` (`distributed` module), `atomic-compute` (`context`, `runtimes`).

> NOTE: This supersedes `DESIGN.md`. `DESIGN.md` was locked (unwritable) during the
> session that produced this revision; rename this file over it once accessible.

## Summary

Collapse the local and distributed schedulers onto one contract. Every unit of work
is a registered task op — there is no separate closure execution path. The task
definition (`#[task]` / `task_fn!`) owns the task's logic, its combinator shape, and
its payload. The scheduler plans a stage DAG and dispatches through one backend hook
(`submit_task`) that runs the task in-process (local) or over the wire (distributed).

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
    |               (backend-agnostic; drives one shared CompletionEvent loop)
    |  submit_task(op-task, executor)      the one runtime-specific hook
    v
backend
    |-- LocalScheduler       : ComputeEngine::execute in-process  -> CompletionEvent
    +-- DistributedScheduler : ship TaskEnvelope to a worker      -> CompletionEvent
                          (owns speculation, fallback, cancellation per task-set)
```

`DagScheduler` builds stages once for both backends and runs one `CompletionEvent` loop
(`submit_stage` -> `submit_missing_tasks` -> `submit_task` -> `on_event_success`). The
backend hook is `submit_task`: local runs the op-task in-process, distributed ships it.
Both report results back as `CompletionEvent`s.

## The backend seam is `submit_task` at task granularity

This is the load-bearing decision. `submit_task` — not a pipeline-level `run_pipeline`
— is the uniform backend hook, and it works only because every unit of work is now a
task op.

Why the current `DistributedScheduler::submit_task` is an empty stub: the trait method
takes `TaskOption` (`ResultTask`/`ShuffleMapTask`), and each `TaskOption` carries
`func: Arc<F>` — a live closure. A worker cannot run a closure, so distributed cannot
fill that signature. The stub is a symptom that `submit_task(TaskOption-with-closure)`
is a local-only seam.

The fix is not to fill the stub against the closure signature. It is to change the task
representation: `ResultTask` / `ShuffleMapTask` carry their **op** (`op_id` +
`Vec<PipelineOp>` + partition bytes — enough to build a `TaskEnvelope`), not a closure.
Once the task is an op, `submit_task` is uniform:

```rust
// shared trait — the ONLY runtime-specific hook
fn submit_task(&self, task: OpTask, executor: SocketAddrV4);
//   Local:       ComputeEngine::execute(task.envelope())      -> push CompletionEvent
//   Distributed: submit_task_to_worker(task.envelope(), exec) -> push CompletionEvent
```

The `#[task]` / `task_fn!` macros are what make this possible: they make an op the unit
of work for the local scheduler too, so there is no closure left to block distributed.
`map(closure)` becomes sugar over `task_fn!`, and `ResultTask.func` is removed.

Consequences:

- The DAG machinery, `CompletionEvent` loop, map-output tracking, and retry become
  fully shared and runtime-agnostic. Distributed stops needing its separate
  `run_native_job` orchestrator; it plugs into the shared loop through `submit_task`.
- Speculation, fallback, and cancellation move into a per-task-set layer inside the
  distributed `submit_task` backend (the analog of a task-set manager), which is where
  cross-partition visibility belongs — not in a separate pipeline orchestrator.
- A pipeline-granularity seam (`run_pipeline`) was rejected: it abstracts only the op
  path and leaves the closure path in place, so it is a half-measure. Task-granularity
  `submit_task` is the full unification.

## Task definition as the source of truth

### Shape (replaces the mixed `TaskAction`)

`TaskAction` conflated two concerns in one enum. Split it (done in the enum-split phase):

- **`TaskAction`** — combinator shape only: `Map`, `Filter`, `FlatMap`, `Fold`,
  `Reduce`, `Aggregate`, `Collect`. Derivable from the task function signature, which
  the `#[task]` / `task_fn!` macros already inspect.
- **`StepKind`** — built-in engine steps that carry no `op_id`: `ShuffleMap`, `Cache`,
  `ReadFileSplit`, `MergeState`, `AgentStep`, `KafkaConsume`.
- **`OpKind`** — `Task(TaskAction)` or `Engine(StepKind)`; `PipelineOp.kind` holds it.

Adding a `#[task]` never touches these enums (shape comes from the signature). New engine
steps go in `StepKind` only. The dispatcher matches `OpKind` exhaustively, so a new
`StepKind` variant forces a handling decision at compile time.

### Payload (typed, task-owned)

`PipelineOp.payload` is a `Vec<u8>` whose meaning depends on the op: an rkyv-encoded
zero for `Fold`/`Aggregate`, a JSON `AgentStepPayload` for `AgentStep`, a bincode
`KafkaConsumePayload` for `KafkaConsume`, empty for `Map`. The encoding is written at
each call site and decoded per-op at the dispatcher.

Make the payload a typed value the task declares, with a macro-generated codec. A task
with bound parameters declares a parameters struct; the macro encodes it at the call site
and decodes it on the worker. The fold zero is one such parameter. This is also the
channel that carries captured values for a lifted closure.

## Capturing closures in distributed mode

The invariant: ship data, never code. A capturing closure is split into registered logic
plus its captured values as serialized arguments.

The transport slot already exists. `PipelineOp.payload` is present, and the worker
dispatch function `__atomic_dispatch_<fn>` already receives a payload byte slice — today
only fold zeros and Python/JS configs use it.

Two mechanisms, layered:

1. **Task with parameters.** `#[task]` emits a struct with fields for the bound
   parameters; the fields are rkyv-encoded into the payload at the call site and decoded
   on the worker. `rdd.map_task(Scale::new(10))`.

2. **Capture-lifting.** `task_fn!` takes an explicit capture list, lifts those variables
   into the parameters struct, and encodes them into the payload. The capturing closure
   then works in distributed mode because it desugars to mechanism 1.
   `rdd.map_task(task_fn!([factor] |x: i32| -> i32 { x * factor }))`.

Values that cannot be serialized (a database handle, a socket) do not travel as
parameters. A large read-only value uses the broadcast-variable path. A non-serializable
resource uses worker-side lazy initialization keyed by a config value shipped as a
parameter.

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
| 2. Task as source of truth | Macro derives combinator shape from the signature. Split `TaskAction` into combinator `TaskAction` plus a `StepKind` engine enum under `OpKind`; `PipelineOp.action` becomes `PipelineOp.kind`. (Typed task-owned payload deferred pending the payload-layout decision.) | Medium; touches the macro and the wire schema | 1 |
| 3. Parameters and capture | `#[task]` structs with fields encode into the payload. `task_fn!` gains capture-lifting. Capturing closures gain an op equivalent. | Medium | 2 |
| 4. Fill combinator gaps | Add `MapPartitions` (and with-index), `MapValues`, `FlatMapValues`, `Foreach` as macro signature patterns. Add Write/Sink to `StepKind`. | Low once phase 2 lands | 2 |
| 5. One DagScheduler | Extract DAG planning into `planner.rs`. `DagScheduler` owns stage lifecycle, map-output tracking, and retry. Move distributed shuffle-stage orchestration out of `atomic-compute` into the scheduler. | High; cross-crate, on the hot path | 1 |
| 6. Uniform `submit_task` backend | Change `ResultTask`/`ShuffleMapTask` to carry an op (`OpTask`) instead of `func: Arc<F>`. Make `submit_task` the one shared backend hook: local runs the op in-process via `ComputeEngine`, distributed ships the `TaskEnvelope`; both report a `CompletionEvent`. Distributed plugs into the shared event loop and retires `run_native_job` as the top-level orchestrator; speculation/fallback/cancellation move to a per-task-set layer inside the distributed backend. | High | 3, 5 |
| 7. Retire the closure IR | `.map` / `.filter` / `.reduce` closure syntax becomes sugar over `task_fn!`. Remove the `ResultTask.func` path and the `NativeScheduler` remnants. `LocalScheduler` becomes a `submit_task` backend for the shared `DagScheduler`. | Medium | 3, 6 |

Order: 1, 2, 3, 4, 5, 6, 7. Phase 1 first, to shrink the surface at zero risk. Phase 2 is
the keystone: once the task owns its shape and payload, the downstream lowering is
mechanical. Phases 6 and 7 deliver the uniform `submit_task`.

## Decisions

Settled:

- Op-only representation. Closure syntax survives as sugar; the closure execution path is
  removed.
- The backend seam is `submit_task` at task granularity, not a pipeline-level
  `run_pipeline`. It is uniform because every unit of work is a task op; the task carries
  an op, not a closure. Speculation/fallback/cancellation live inside the distributed
  `submit_task` backend.
- The task definition owns shape and payload. `StepKind` holds the engine primitives.

Open:

1. **Non-serializable captures.** Either forbid them (require worker-side initialization
   plus a shipped config parameter), or keep a `LocalClosure` task variant that only the
   local backend accepts and the distributed backend rejects at plan time. Recommendation:
   keep the `LocalClosure` variant — it is one task arm, not a second scheduler, and it
   preserves local prototyping.
2. **Payload layout.** Either extend a per-`StepKind` payload schema, or add a distinct
   `args` field on `PipelineOp` alongside config. Recommendation: a single typed
   parameters value that owns the whole payload, config included.

## Risks

- Phases 5 and 6 move orchestration across crate boundaries and touch the dispatch hot
  path. They must preserve the map-output recovery hook, job-abort handling, adaptive
  shuffle coalescing, cancellation, and speculation.
- Phase 2 changes the wire schema (`TaskAction` split, `PipelineOp.kind`). Worker and
  driver binaries must be rebuilt together; the registry fingerprint already guards
  mismatched binaries.
- Capture-lifting requires the rkyv traits on captured values. The macro must produce a
  clear compile error when a captured value lacks them.
