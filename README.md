# Atomic

A distributed data processing framework written in stable Rust.

---

## What is Atomic?

Atomic is a rewrite and redesign of [vega](https://github.com/rajasekarv/vega), a Spark-inspired distributed compute engine for Rust. Vega proved that Rust could support a Spark-like RDD model, but it required **nightly Rust** to serialize closures across the network — a fragile foundation that made the project difficult to maintain and eventually unmaintained.

Atomic solves this by replacing closure serialization entirely. Tasks are registered at compile time via a `#[task]` macro and dispatched to workers by a stable string ID. There are no nightly features, no unsafe closure transmutes, and no runtime reflection. The result is a framework that compiles on stable Rust and has a clear, auditable boundary between driver code and worker execution.

---

## How Atomic improves on Vega

**Vega's core limitation** was that sending a closure from a driver to a worker required serializing a Rust function pointer — something only possible on nightly Rust via unstable intrinsics. This made Vega dependent on a specific nightly toolchain and blocked it from ever stabilizing.

**Atomic's approach:**

- Tasks are plain Rust functions annotated with `#[task]`. The macro registers them into a compile-time dispatch table (via `inventory`). Workers look up tasks by ID — no closure, no unsafe transmute.
- Partition data is encoded with `rkyv` for zero-copy deserialization on the worker side, replacing Vega's `serde`-based wire format.
- The driver API (`ctx.parallelize(...).map(...).filter(...).collect()`) looks identical to Vega and Spark, but under the hood it builds a `PipelineOp` chain that is executed locally or dispatched to workers over TCP without any function serialization.
- Python and JavaScript UDFs are supported as first-class distributed operations. Python functions are serialized via `pickle`, JS functions via `fn.toString()`. Workers execute them through embedded PyO3 and QuickJS runtimes. This gives Python and JS developers a Spark-like experience without requiring Rust.
- Local and distributed execution share the same `dispatch_pipeline` contract. Switching between modes is an environment variable, not a code change.

---

## Quick example

**Rust native task:**

```rust
#[task]
fn double(x: i32) -> i32 { x * 2 }

let ctx = Context::new()?;
let result = ctx.parallelize_typed(vec![1, 2, 3, 4], 2)
    .map_task("double")
    .collect()?;
// [2, 4, 6, 8]
```

**Python UDF (local or distributed):**

```python
import atomic

ctx = atomic.Context()
result = ctx.parallelize([1, 2, 3, 4], num_partitions=2) \
            .map(lambda x: x * 2) \
            .filter(lambda x: x > 4) \
            .collect()
# [6, 8]
```

---

## Crate layout

| Crate | Purpose |
| --- | --- |
| `atomic-data` | Shared types: RDD traits, task envelopes, wire protocol, distributed structs |
| `atomic-compute` | Execution runtime: context, executor, backends, UDF dispatch |
| `atomic-scheduler` | Local thread-pool and distributed TCP schedulers |
| `atomic-utils` | Shared utilities |
| `atomic-runtime-macros` | `#[task]` proc-macro for compile-time task registration |
| `atomic-py` | Python extension module (maturin/PyO3) — Spark-like Python driver API |
| `atomic-js` | JavaScript library (rquickjs) — Spark-like JS driver API |
| `atomic-worker` | Polyglot worker binary with embedded Python + QuickJS runtimes |

---

## Roadmap

### Completed

- [x] Stable-Rust task dispatch via `#[task]` macro — no nightly required
- [x] rkyv zero-copy wire protocol for partition data
- [x] Local thread-pool execution via `LocalScheduler`
- [x] Distributed TCP execution via `DistributedScheduler`
- [x] `dispatch_pipeline` — unified local/distributed execution contract
- [x] Multi-op lazy pipeline staging (`PipelineOp` chains)
- [x] Python UDF support — `pickle`-serialized lambdas dispatched to workers via PyO3
- [x] JavaScript UDF support — `fn.toString()` source dispatched to workers via QuickJS
- [x] `atomic-py` — Spark-like Python driver API (`parallelize`, `map`, `filter`, `fold`, `collect`, etc.)
- [x] `atomic-js` — Spark-like JavaScript driver API
- [x] `atomic-worker` — polyglot worker binary (native + Python + JS tasks)
- [x] RDD transformations: `map`, `filter`, `flat_map`, `map_values`, `flat_map_values`, `key_by`, `reduce_by_key`, `group_by_key`, `count_by_value`, `count_by_key`, `group_by`, `union`, `zip`, `cartesian`, `coalesce`
- [x] RDD actions: `collect`, `count`, `fold`, `reduce`, `take`, `first`
- [x] Narrow and shuffle dependency tracking with two-phase (map stage → reduce stage) execution
- [x] Worker capability handshake and capacity-aware task placement (`max_tasks` respected)

### In Progress / Recently Completed

- [x] **Lazy shuffle pipeline** — `reduce_by_key` and `group_by_key` now build a `ShuffleDependency + ShuffledRdd` DAG lazily (Spark-style). Execution is deferred to actions (`collect`, `count`, etc.). The scheduler detects shuffle stage boundaries, runs a two-phase map → reduce, stores buckets in `DashMapShuffleCache`, and reads them back via HTTP through the embedded `ShuffleManager`.
- [x] **`DashMapShuffleCache`** — concrete in-memory shuffle cache implementation; replaces the stub trait-only definition.
- [x] **Shuffle infrastructure wired end-to-end** — `SHUFFLE_CACHE`, `MAP_OUTPUT_TRACKER`, and `SHUFFLE_SERVER_URI` globals initialized on context start; `do_shuffle_task_typed()` stubs replaced with real cache writes and URI returns.
- [x] **`partition_id` in `TaskResultEnvelope`** — result envelopes now carry `partition_id` so the distributed scheduler can reconstruct partition order correctly after retries.
- [x] **Capacity-aware worker placement** — `next_executor_with_capacity()` skips workers whose `max_tasks` is 0; distributed scheduler routes tasks only to available workers.
- [x] **Scheduler infinite-loop fix** — two bugs in `LocalScheduler` / `Stage`: (1) `add_output_loc` had an inverted condition that never incremented `num_available_outputs`; (2) `get_shuffle_map_stage` returned a stale clone instead of the live `stage_cache` entry, so shuffle stages always appeared unavailable.

### Pending — Essential Before Production / Distribution

- [x] **Distributed shuffle correctness** — workers each run their own `ShuffleManager` HTTP server; URIs are registered with the driver's `MapOutputTracker` via `shuffle_server_uri` and `run_shuffle_map_stage`. Verified end-to-end.
- [x] **`ShuffleFetcher` HTTP retry and error recovery** — `fetch_chunk_with_retry` implements exponential backoff; transient network errors during shuffle reads are retried automatically.
- [x] **Fault recovery** — per-task failure counter, resubmit queue, and `MaxTaskFailures` error implemented; failed stages are detected and requeued.
- [ ] **Distributed integration test** — automated test that spawns a real worker over TCP and runs a full `#[task]` job end-to-end; not yet written.
- [ ] **PyPI release** — `maturin publish` workflow for `atomic-py` not yet set up.
- [ ] **npm release** — `npm publish @atomic-compute/js` workflow not yet set up.
- [ ] **DAG optimizer** — no predicate pushdown, pipeline fusion, or partition pruning; every transformation becomes a separate stage boundary.
- [ ] **Checkpointing** — no RDD lineage truncation or checkpoint storage; a long lineage chain will recompute from the source on any failure.
- [ ] **Web dashboard** — job progress, stage timing, and partition metrics are logged but not exposed via a UI or metrics endpoint.

### Planned (Post-Stabilization)

- [ ] Adaptive query execution — dynamic partition coalescing based on shuffle output sizes
- [ ] Broadcast variables — driver-side values replicated to all workers without serializing into every task envelope
- [ ] Accumulator API — distributed counters and aggregators updated from task closures
- [ ] Structured streaming / micro-batch mode

---

## Installation

### Rust (native tasks)

```bash
# Add atomic-compute to your Cargo.toml
# [dependencies]
# atomic-compute = { path = "path/to/atomic/crates/atomic-compute" }

cargo build --release
```

### Python

```bash
cd crates/atomic-py
pip install maturin
maturin develop --release        # dev install (editable)
# or: maturin build --release && pip install ../../target/wheels/atomic-*.whl
```

### JavaScript / Node.js

```bash
# Build the native Node.js module
cargo build --release -p atomic-js

# Load in your script
const { Context } = require('./crates/atomic-js');
```

### Worker binary

```bash
# Build the polyglot worker (supports native, Python, and JS tasks)
cargo build --release -p atomic-worker

# Start a worker on port 10001
RUST_LOG=info ./target/release/atomic-worker --worker --port 10001
```

---

## Design notes

- Distributed tasks reference pre-registered functions by ID via the `#[task]` macro and `TASK_REGISTRY`. Workers cannot receive arbitrary Rust code at runtime — only data payloads and op IDs. This is intentional: it makes the execution model auditable and keeps the worker surface area small.
- Python/JS UDFs are the explicit escape hatch for dynamic code. They go through a clearly bounded path (`PythonUdf` / `JavaScriptUdf` actions) with their own serialization format (JSON data, pickle/source function encoding).
- rkyv is used for the Rust native path; JSON is used for the Python/JS path. Both sides can encode and decode their own format without sharing a schema.
- `atomic-py` is a `cdylib` — it must be built with `maturin`, not `cargo build`. `atomic-worker` must be excluded from `cargo test --workspace` because it activates PyO3's `auto-initialize` feature which requires linking against libpython.

---

## License

Licensed under the [Apache 2.0 License](LICENSE).
