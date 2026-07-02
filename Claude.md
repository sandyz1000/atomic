# CLAUDE.md

Guidance for Claude Code when working in this repository.

For prose conventions (docs, READMEs, commits), the `atomic-prose` skill is authoritative.
For Rust style and structure, the `atomic-rust-standards` skill is authoritative.

## Development Pattern

Act as a senior Rust engineer. Keep code idiomatic: rustfmt, clippy clean, clear error
handling, minimal `unwrap`/`expect` outside tests. Group code by responsibility (domain,
adapters, config); avoid generic `utils`. Names reflect responsibility
(`circuit_breaker.rs`, not `helpers.rs`). When refactoring, keep behavior the same and focus
on structure, naming, and readability.

## Critical Rules

- `README.md` in the root may be edited only when explicitly asked.
- For serialization on Rust-native paths, use **rkyv** before considering alternatives.
- `atomic-py` is a `cdylib` — build with `maturin`, never `cargo build` alone. `cargo test`
  for it also requires `maturin develop`.
- `atomic-worker` must be **excluded** from `cargo test --workspace` — it enables PyO3's
  `auto-initialize`, which breaks the link step in non-Python test binaries.
- Do not reference Spark, PySpark, or GraphX in code comments, docstrings, or docs. Atomic is
  its own project; it took inspiration, it does not reimplement them.

## Development Commands

```bash
# Build
cargo build
cargo build --release
cargo build --release --features tls      # TLS for worker comms
cargo build --release --features s3       # S3 object store

# Test (excludes atomic-py and atomic-worker by design)
cargo test --workspace --exclude atomic-py --exclude atomic-worker
cargo test -p atomic -- test_reduce_by_key_basic     # one test by name
cargo test -p atomic -- test_distributed             # distributed integration test

# Distributed tests requiring a pre-built binary
cargo build --release -p atomic
cargo test -p atomic -- --test-threads=1 --ignored

# Examples
cargo run --example task_wordcount
cargo run --example pi

# Python bindings (requires maturin)
cd crates/atomic-py && pip install maturin && maturin develop --release && pytest tests/

# Worker binary
cargo build --release -p atomic-worker
RUST_LOG=info ./target/release/atomic-worker --worker --port 10001

# Cross-compile and ship to workers
cargo install --path crates/atomic-cli
atomic build --target x86_64-unknown-linux-musl
atomic ship --workers user@host1,user@host2

# CI locally (mirrors GitHub Actions)
cargo test --workspace --exclude atomic-py --exclude atomic-worker -- --test-threads=4
cargo fmt --all -- --check
cargo clippy --workspace --exclude atomic-py --exclude atomic-worker -- -D warnings
```

## Project Goal

Atomic is a distributed compute engine for stable Rust. It keeps a high-level RDD execution
model (lazy transform DAG, shuffle) but removes any dependence on unstable Rust features and
closure serialization. Distributed work is registered at compile time and dispatched by string
ID instead of shipping serialized closures. Distributed wire payloads use rkyv.

## Repository Structure

- `atomic-data`: shared types — RDD traits, task envelopes, distributed structs, shuffle
  primitives, dependency DAG, partition cache.
- `atomic-compute`: execution runtime — context, executor, `NativeBackend`, task dispatch, RDD
  implementations, persist/cache. Backend runtimes in `runtimes/`: `native.rs`, `py.rs`, `js.rs`.
- `atomic-scheduler`: DAG building, stage planning, `LocalScheduler`, `DistributedScheduler`.
- `atomic-sql`: SQL and DataFrame layer on DataFusion.
- `atomic-streaming`: micro-batch streaming on `atomic-compute`; `DistributedSource` + Kafka source.
- `atomic-structured`: continuous SQL/DataFrame queries — windows, stream-stream joins,
  watermarks, mergeable-aggregate state store, sinks. Built on `atomic-sql`.
- `atomic-graph`: graph processing — `Graph<VD,ED>`, Pregel engine, built-in algorithms.
- `atomic-nlq`: natural-language query layer — agentic `WorkflowPlan` loop + LLM-native
  DataFusion nodes + vector index. Uses OpenAI.
- `atomic-py` / `atomic-js`: Python (PyO3/maturin) and Node.js (NAPI) bindings; mirror `TypedRdd`.
- `atomic-worker`: standalone worker binary with embedded PyO3 + V8.
- `atomic-cli`: cross-compilation, secure binary distribution to workers, and ad-hoc
  Kubernetes job submission (`atomic submit-k8s`); the latter behind the `k8s` feature.
- `atomic-k8s`: Kubernetes per-job worker allocator (`KubeWorkerAllocator`) and the
  driver `Job` spec builder for `atomic submit-k8s`; behind the `k8s` feature.
- `atomic-bootstrap`: tiny fetch-and-exec binary, published as a generic container
  image, that lets `atomic submit-k8s --binary` stage a driver binary to S3 and run it
  with no per-job image build.
- `atomic-utils`, `atomic-tests`: shared utilities and the integration suite.

## RDD API Convention

### The `_task` family — canonical API for local and distributed execution

All narrow transforms and reductions have a `_task` variant. Always use these:

| Method | Trait required | TaskAction |
| --- | --- | --- |
| `rdd.map_task(F)` | `UnaryTask<T, U>` | `Map` |
| `rdd.filter_task(F)` | `UnaryTask<T, bool>` | `Filter` |
| `rdd.flat_map_task(F)` | `UnaryTask<T, Vec<U>>` | `FlatMap` |
| `rdd.fold_task(zero, F)` | `BinaryTask<T>` | `Fold` |
| `rdd.reduce_task(F)` | `BinaryTask<T>` | `Reduce` |

Local mode runs them in-process; distributed mode accumulates them into a `StagedPipeline` and
dispatches the whole chain as one `TaskEnvelope` per partition when an action fires. The
closure variants (`map`, `filter`, …) run only on the driver and cannot dispatch to workers.

### Registering a task

```rust
#[task]
fn double(x: i32) -> i32 { x * 2 }
rdd.map_task(Double);                                 // #[task] generates a PascalCase struct

rdd.map_task(task_fn!(|x: i32| -> i32 { x * 2 }));    // inline; content-hash op_id
```

Both submit a `TaskEntry` via `inventory::submit!` and emit a `body_hash`. `REGISTRY_FINGERPRINT`
folds all `(op_id, body_hash)` pairs into one `u64`; workers advertise it on the TCP handshake and
the driver rejects mismatched workers.

### Entry point

Build a `Config` and pass it to `Context::new_with_config()`. For driver+worker programs use
`AtomicApp::build()`, which reads `--worker` / `--workers` / `--local-ip`. Workers run the same
binary: `./my_app --worker --port 10001`.

## Architecture Rules

- **Serialization** — rkyv for distributed wire payloads. No generic closure serialization, no
  Cap'n Proto.
- **Execution model** — local mode runs on threads via `LocalScheduler`; distributed mode
  dispatches task envelopes via `DistributedScheduler` + `NativeBackend`. Driver and workers run
  the same binary; the dispatch table is linked at compile time. Tasks are registered with
  `#[task]` and dispatched by string ID.
- **One backend** — `NativeBackend` only. It holds a `HashMap<TaskRuntime, Box<dyn OpDispatcher>>`:
  `Native` (`runtimes/native.rs`), `Python` (`runtimes/py.rs`), `JavaScript` (`runtimes/js.rs`).
  Adding a runtime = one `impl OpDispatcher` + one `HashMap::insert`. No Docker or WASM backends.
- **Shuffle** — `reduce_by_key` / `group_by_key` are lazy (`ShuffleDependency` + `ShuffledRdd`).
  The scheduler splits the DAG at shuffle boundaries. Map output lives in `DashMapShuffleCache`,
  served over HTTP by `ShuffleManager`; `ShuffleFetcher` pulls on the reduce side.

## Distributed Contract

Distributed tasks use `atomic_data::distributed`:

- `TaskEnvelope` — rkyv metadata + `Vec<PipelineOp>` + input partition bytes.
- `TaskResultEnvelope` — rkyv result or failure, with `partition_id` for ordering.
- `WorkerCapabilities` — advertised limits (`max_tasks`, `registry_fingerprint`); the scheduler
  skips workers at capacity and logs a clear error on fingerprint mismatch.

The op_id format is `crate::module::fn::body_hash_short`. Changing a function body changes the
op_id, so a stale worker fails loudly at dispatch instead of running old code.

## Guardrails for Future Changes

- Stable Rust only — no nightly features, no `fn`-pointer serialization. (This is unrelated to
  the Python/JS closure-shipping path, which is a supported execution model.)
- No Docker or WASM backends.
- `#[task]` / `task_fn!` is the only way to register distributed work.
- Prefer explicit backend routing and compile-time dispatch over runtime guessing.
- Keep the Rust, Python, and JS RDD APIs in parity — they are co-equal surfaces over one engine.

## Two Execution Models

Both are production paths, neither is second-class:

- **Compiled task dispatch (Rust).** `#[task]` functions compiled into the binary, dispatched by
  string ID. Native speed plus the compile-time registry/fingerprint guarantee. Changing a Rust
  task requires `cargo build` + `atomic ship`.
- **Dynamic closure shipping (Python / JS).** The function travels with the job — Python via
  `cloudpickle`, JS via captured source — and runs on the worker's embedded PyO3 / V8 runtime. No
  rebuild, no redeploy.

All three languages share one job API, so a job can be prototyped dynamically and have a hot path
rewritten as a Rust `#[task]` without changing the driver script.

## Where to Find More

- **Architecture** — `docs/src/content/docs/architecture/` (overview, execution model, shuffle).
- **Per-feature guides** — `docs/src/content/docs/guides/` (SQL, streaming, graph, NLQ,
  configuration, deployment) and `concepts/rdd-api.md`.
- **Implementation history and per-change detail** — `CHANGELOG.md`.
- **SQL** — `atomic-sql` wraps DataFusion (`AtomicSqlContext`, `RddTableProvider`); Arrow
  `RecordBatch` is the row format. No custom optimizer rules or physical operators are added.
- **Graph** — built-in algorithms: PageRank, ShortestPath (Pregel), StronglyConnectedComponent
  (Tarjan), LabelPropagation, TriangleCount, ConnectedComponent.
- **Docs site** — `cd docs && npm install && npm run dev`. JS API reference is generated by
  TypeDoc from `crates/atomic-js/index.d.ts`.
