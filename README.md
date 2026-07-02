# Atomic

**A distributed compute engine in stable Rust — process datasets across many
machines, deploy as a single self-contained binary, and write your jobs in Rust,
Python, or TypeScript.**

## What it is

Some data jobs are too big for one machine, so the work is split across a
cluster. Doing that usually means a heavy stack: a language runtime on every
node, a cluster manager, containers. Atomic removes that. You ship **one small
executable** to each machine and you are running in under a minute. Write your
logic in **Python or TypeScript** to move fast, or in **Rust** for maximum speed
— the same job API in every language. SQL and LLM-powered natural-language
queries are built in.

## What makes it different

Atomic combines three ideas:

1. **Two ways to run your code — chosen per language.**
   - **Python and TypeScript: dynamic closure shipping.** Functions travel with
     the job — Python via `cloudpickle`, JavaScript as captured source — and run
     on the worker's embedded PyO3 / V8 runtime. Change a function, re-run. No
     rebuild, no redeploy.
   - **Rust: compiled task dispatch.** `#[task]` functions are registered at
     compile time and dispatched by string ID. The worker runs the same binary
     as the driver, so the function is already there — you ship the binary, not
     the code.

2. **One binary, anywhere.** Driver and worker are the same executable. No
   language runtime to install, no daemon, no cluster manager to start.
   Cross-compile with `atomic build`, ship with `atomic ship` over SSH.

3. **Wrong code can't run by accident.** Rust tasks are dispatched by ID against
   a compile-time registry, so a worker cannot execute code it wasn't built
   with. A missing or mismatched task fails immediately at dispatch with a clear
   error, not hours later in a worker log:

   ```text
   Task 'my_crate::transform::normalize_v2' not registered in TASK_REGISTRY.
   Registered ops (12 total): [my_crate::transform::normalize, ...]
   ```

   A second layer catches the case where the name stays the same but the body
   changes. Every `#[task]` embeds an FNV-1a hash of its body in its op ID, and
   the whole registry folds into one `REGISTRY_FINGERPRINT`. Workers advertise
   their fingerprint at handshake; the driver rejects a mismatch before
   dispatching anything.

## Quick start

### Rust

```rust
use atomic_compute::{context::Context, env::Config, task};

#[task]
fn square(x: i32) -> i32 { x * x }

fn main() -> anyhow::Result<()> {
    let ctx = Context::new_with_config(Config::local())?;
    let result = ctx
        .parallelize_typed(vec![1, 2, 3, 4, 5], 2)
        .filter(|x| x % 2 != 0)
        .map_task(Square)          // dispatched to workers by ID in distributed mode
        .collect()?;
    println!("{result:?}");        // [1, 9, 25]
    Ok(())
}
```

Switch to distributed mode by changing one `Config` line — the job code is
unchanged:

```rust
let config = Config::builder()
    .local_ip("10.0.0.100".parse()?)
    .workers(vec!["10.0.0.101:10001".parse()?, "10.0.0.102:10001".parse()?])
    .build();
```

### Python

```python
import atomic_compute

ctx = atomic_compute.Context()
result = (
    ctx.parallelize([1, 2, 3, 4, 5], num_partitions=2)
       .filter(lambda x: x % 2 != 0)
       .map(lambda x: x * x)
       .collect()
)
print(result)  # [1, 9, 25]
```

### TypeScript

```typescript
import { Context } from "@atomic-compute/js";

const ctx = new Context();
const result = ctx
  .parallelize([1, 2, 3, 4, 5], 2)
  .filter((x: number) => x % 2 !== 0)
  .map((x: number) => x * x)
  .collect();
console.log(result);  // [1, 9, 25]
```

## Two execution models

Both are production paths, neither is second-class. Because all three languages
share one job API, a job can start dynamic in Python and have a hot path
rewritten as a Rust `#[task]` later without changing the program's shape.

| Capability | Rust | Python | TypeScript |
| --- | --- | --- | --- |
| `#[task]` compile-time dispatch | yes | — | — |
| Closure / lambda tasks | local | pickled | V8 source |
| SQL (`SqlContext`) | yes | yes | yes |
| Streaming (`StreamingContext`) | yes | yes | yes |
| Graph (`Graph`, built-in algorithms) | yes | yes | yes |
| Broadcast variables / accumulators | yes | yes | yes |
| S3 `text_file` / `save_as_text_file` | yes | `s3` feature | `s3` feature |
| Pregel custom vertex programs | yes | `run_pregel` | `runPregelF64` |

## Capabilities

- **RDD API** — `map`, `filter`, `flat_map`, shuffle-based `reduce_by_key`,
  `group_by_key`, joins, `cogroup`, distributed `sort_by_key`, custom
  partitioners, `cache` / `persist` / `checkpoint`.
- **SQL** — `atomic-sql` on Apache DataFusion: SQL parser, optimizer, Arrow
  columnar execution, Parquet/CSV/JSON, RDD-backed table providers.
- **Streaming** — micro-batch `StreamingContext`, and `atomic-structured` for
  continuous SQL with tumbling/sliding/session windows, stream-stream joins,
  watermarks, and exactly-once Kafka.
- **Graph** — `Graph<VD,ED>` with a Pregel engine: PageRank, shortest path,
  strongly connected components (Tarjan), label propagation, triangle count,
  connected components.
- **Natural language queries** — `atomic-nlq` plans a workflow of tool calls
  from a plain-language question and runs it on the SQL and compute layers.
- **Deployment** — static musl binary, SSH/SFTP distribution, Kubernetes Helm
  chart with per-job worker allocation, mutual TLS, Prometheus metrics.

See the [documentation](docs/src/content/docs/) for full guides.

## Architecture

```text
Driver (Python / TypeScript / Rust)
  Context → TypedRdd → StagedPipeline → TaskEnvelope
       AtomicSqlContext → DataFusion LogicalPlan
       NlqContext → LlmPlanner → WorkflowPlan
                         │  TCP (optional mTLS)
        ┌────────────────┼────────────────┐
   ┌────▼────┐      ┌────▼────┐       ┌────▼────┐
   │ Worker  │      │ Worker  │       │ Worker  │
   │ TASK_   │      │ TASK_   │       │ TASK_   │
   │REGISTRY │      │REGISTRY │       │REGISTRY │
   │ (same   │      │ (same   │       │ (same   │
   │ binary) │      │ binary) │       │ binary) │
   └─────────┘      └─────────┘       └─────────┘
```

- `TASK_REGISTRY` is linked at compile time via `inventory`. Workers cannot
  execute tasks they were not compiled with.
- Distributed wire types use `rkyv` for zero-copy deserialization.
- `LocalScheduler` and `DistributedScheduler` share the same `NativeBackend`
  dispatch, so local-mode tests cover the same code path as distributed jobs.

See [docs/.../architecture/](docs/src/content/docs/architecture/) for detail.

## Crate layout

| Crate | Purpose |
| --- | --- |
| `atomic-data` | Shared types — RDD traits, task envelopes, wire protocol, shuffle, cache |
| `atomic-compute` | Execution runtime — context, executor, `NativeBackend`, RDD impls |
| `atomic-scheduler` | `LocalScheduler` + `DistributedScheduler` |
| `atomic-sql` | SQL layer — `AtomicSqlContext`, DataFusion table providers |
| `atomic-streaming` | Micro-batch streaming + Kafka source |
| `atomic-structured` | Continuous SQL queries — windows, joins, watermark, state store |
| `atomic-graph` | Graph processing — `Graph<VD,ED>`, Pregel, algorithms |
| `atomic-nlq` | Natural-language query — workflow planner, LLM DataFusion nodes |
| `atomic-py` / `atomic-js` | Python and Node.js bindings |
| `atomic-worker` | Worker binary with embedded PyO3 + V8 |
| `atomic-cli` | Cross-compilation + SSH/SFTP binary distribution |
| `atomic-k8s` | Kubernetes per-job worker allocation |
| `atomic-runtime-macros` | `#[task]` and `task_fn!` proc-macros |

## Deployment

No cluster manager, no container daemon, no language runtime to install on
workers. One binary, shipped over SSH, running in under a minute.

### Ship to bare-metal or VMs

```bash
# 1. Install the CLI (once)
cargo install --path crates/atomic-cli

# 2. Cross-compile a static Linux binary from your dev machine
atomic build --target x86_64-unknown-linux-musl

# 3. Upload to workers and verify checksum
atomic ship --workers user@10.0.0.101,user@10.0.0.102
```

`ship` connects over SSH, uploads via SFTP, verifies the SHA-256 checksum, and
renames atomically — the old binary is never replaced until the new one lands
intact.

Then start workers (same binary, `--worker` flag):

```bash
# On each worker host
./my_app --worker --port 10001
```

And run the driver from your machine:

```bash
./my_app --driver --workers 10.0.0.101:10001,10.0.0.102:10001
```

That's the full deployment. No Dockerfile, no registry, no YAML until you need
Kubernetes.

### Kubernetes

For managed clusters, the Helm chart in [`deploy/`](deploy/) provisions workers
as a StatefulSet and wires up the headless Service for discovery. For per-job
elasticity — different resource profiles per job, pods created on demand and
deleted after — set `allocator: kube` and the driver allocates pods through the
Kubernetes API directly:

```bash
helm install atomic deploy/helm/atomic \
  --set allocator=kube \
  --set k8s.workerImage=myregistry/my-app:latest
```

Workers for each job are created with exactly the resources that job requested
(`ResourceProfile`) and deleted when it finishes.

### Submit a one-off job

`helm install`/`helm upgrade` is for standing deployments. For a single ad-hoc run —
the `spark-submit` equivalent — `atomic submit-k8s` (requires `atomic-cli --features
k8s`) creates one `Job`, runs it, and exits. No Helm release needed for the run itself,
but the target namespace needs the RBAC from `helm install ... --set allocator=kube`
(or `--set driver.enabled=false` to install just the RBAC/ServiceAccount).

Two ways to get the driver binary into the pod:

```bash
# Already have an image (built via deploy/Dockerfile, pushed to a registry):
atomic submit-k8s --image myregistry/my-app:0.1 \
  --dynamic-workers --worker-image myregistry/my-app:0.1 \
  -- --my-job-flag foo

# No image at all — stage the binary to S3, run it through the project's generic
# fetch-and-exec bootstrap image:
atomic build --target x86_64-unknown-linux-musl
atomic submit-k8s --binary target/x86_64-unknown-linux-musl/release/my_app \
  --s3-bucket my-staging-bucket \
  --dynamic-workers --worker-image myregistry/my-app:0.1 \
  -- --my-job-flag foo
```

`--dynamic-workers` makes the Kubernetes allocator available to the submitted driver
(`ctx.with_workers(...)` inside the job's own code decides how many workers and what
size). Without it, point the driver at an existing worker StatefulSet instead by
passing `--workers dns:<svc>:<port>` as one of the trailing job args:

```bash
atomic submit-k8s --image myregistry/my-app:0.1 \
  -- --workers dns:my-atomic-worker:10001 --my-job-flag foo
```

Follow the run with `kubectl logs -f job/<name> -n <namespace> -c driver` — printed
after submission.

### Advanced: mutual TLS, S3

```bash
# TLS between driver and workers
atomic build --target x86_64-unknown-linux-musl --features tls

# S3 as checkpoint / text_file source
atomic build --target x86_64-unknown-linux-musl --features s3
```

See the [deployment guide](docs/src/content/docs/guides/deployment.md) for
TLS certificate setup, S3 credentials, and Helm values reference.

## Documentation

The documentation site is built with Astro Starlight from `docs/`:

```bash
cd docs && npm install && npm run dev
```

- [Getting Started](docs/src/content/docs/guides/getting-started.md)
- [The RDD API](docs/src/content/docs/concepts/rdd-api.md)
- [Architecture](docs/src/content/docs/architecture/overview.md)
- [Configuration](docs/src/content/docs/guides/configuration.md)
- [Roadmap](docs/src/content/docs/roadmap.md)

## Status

Beta. Core features are implemented and tested: local execution, distributed TCP
dispatch, shuffle, streaming, structured streaming, graph, and SQL.

- **Ready** — local-mode jobs, SQL analytics, graph algorithms, Python/JS
  prototyping, static binary and Kubernetes deployment.
- **Early adopter** — distributed mode on real workloads. The core is solid
  (shuffle joins, fault recovery, distributed cache with locality, speculation,
  sharded structured-streaming state, exactly-once Kafka). Kafka, structured
  streaming, and K8s integration tests run behind the `--ignored` CI job.
- **Not yet** — Kubernetes CRD operator, Delta/Iceberg table formats,
  broadcast/sort-merge join and skew handling for very large shuffles.

## License

[Apache 2.0](LICENSE)
