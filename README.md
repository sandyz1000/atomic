# Atomic

**A distributed compute engine in stable Rust — process datasets across many machines, deploy as a
single self-contained binary, and write your jobs in Rust, Python, or TypeScript.**

## In plain terms

Some data jobs are too big for one computer, so the work is split across a cluster of machines.
Doing that normally means a heavy stack — a Java Virtual Machine on every node, a cluster manager,
containers. Atomic removes all of that: you ship **one small executable** to each machine and you are
running in under a minute. You write your logic in **Python or TypeScript** when you want to move
fast, or in **Rust** when you want maximum speed — the same job API in every language. The result is
a faster, lighter, safer way to run distributed data and analytics workloads, including SQL and
LLM-powered natural-language queries.

## What makes it different (for engineers)

Atomic builds on the RDD model that Apache Spark popularized, then combines three ideas no other
engine has put together:

1. **Two first-class ways to run your code — pick per language, not per project.**
   - **Python & TypeScript — dynamic closure shipping.** Your functions travel *with the job*:
     Python lambdas are serialized with `cloudpickle`, JS functions are captured as source (and can
     capture driver-side values as context). They run on the worker's embedded PyO3 / V8 runtime.
     Change a lambda, re-run — no rebuild, no redeploy. This is a real execution path, not a toy.
   - **Rust — compiled task dispatch.** Rust has no closure serialization, so Atomic does the
     principled thing instead: `#[task]` functions are registered at compile time and dispatched by
     string ID. The worker runs the *same binary* as the driver, so the function is already there —
     you ship the binary, not the code. The payoff is the safety guarantee in idea 3.
2. **One binary, anywhere.** Driver and worker are the same executable. No JVM, no daemon, no cluster
   manager required to start. Cross-compile with `atomic build`, ship with `atomic ship` over SSH —
   workers are running in under a minute.
3. **Wrong code can't run by accident.** Because Rust tasks are dispatched by ID against a
   compile-time registry, a worker can never execute code it wasn't built with. A missing or
   mismatched task fails *immediately, at dispatch*, with a clear error — not three hours later in a
   worker log. This is a structural guarantee, not a coding convention (details below).

---

## Why This Matters

Distributed engines have to get your code onto the workers. There are two honest ways to do it, and
Atomic deliberately uses **both** — choosing the right one per language instead of forcing one model
on everyone:

- **Serialize the code and ship it** — flexible and dynamic, the right call for scripting languages.
  This is Atomic's **Python/TypeScript** path (`cloudpickle` / JS source), and it's how PySpark, Ray,
  and Flink work too. The trade-off is that serialization can fail at runtime for awkward object
  graphs — a familiar tax in those ecosystems.
- **Compile the code in and dispatch by name** — fast and verifiable, the right call for a systems
  language. This is Atomic's **Rust** path, and it's the one most engines *can't* offer.

For Rust, the `#[task]` approach means the worker's dispatch table is linked at compile time via
`inventory`. The driver sends a string ID and a data payload — not code. If a task ID doesn't exist
on the worker, you get a clear error listing what *is* registered, at dispatch time, not buried in a
worker log three hours later:

```text
Task 'my_crate::transform::normalize_v2' not registered in TASK_REGISTRY.
Registered ops (12 total): [my_crate::transform::normalize, my_crate::transform::filter_nulls, ...]
```

This is a structural guarantee, not a coding convention.

A second safety layer catches the subtler case — where the task name stays the same but the **body changes**. Every `#[task]` function embeds a short FNV-1a hash of its body tokens in its op_id (`"my_crate::transform::normalize::a1b2c3d4"`). The entire registry is hashed into a single `REGISTRY_FINGERPRINT`. Workers advertise their fingerprint on the TCP handshake; the driver compares it before accepting the worker. A mismatched fingerprint means the worker was compiled from different code and is logged as an error before any task is dispatched:

```text
worker 10.0.0.5:10001 registry fingerprint mismatch:
  driver=0xdeadbeef12345678, worker=0xbadcafe087654321.
Task implementations diverged — redeploy workers with the same binary.
```

---

## Quick Start

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

Switch to distributed mode by changing one `Config` line — the job code is unchanged:

```rust
let config = Config::builder()
    .local_ip("10.0.0.100".parse()?)
    .workers(vec!["10.0.0.101:10001".parse()?, "10.0.0.102:10001".parse()?])
    .build();
```

### Python (dynamic)

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

---

## Two Execution Models — and an Optional Path Between Them

Atomic gives you two production-grade ways to run a job, and you can use either on its own:

- **Dynamic (Python / TypeScript).** Closures ship with the job. Ideal for fast iteration,
  exploratory analytics, and teams that live in Python or TS. Nothing about this is "temporary" —
  many jobs run this way in production forever.
- **Compiled (Rust).** `#[task]` functions are built into the binary. Ideal for CPU-bound hot paths
  where you want native speed and the compile-time dispatch guarantee.

Because **all three languages share the same job API**, there is also an *optional* progression: when
a dynamic job is proven and you want to squeeze out more throughput, you can re-express the hot path
as a Rust `#[task]` without changing the shape of the program. The steps below show that path — but
treat it as one option, not a mandate.

### Step 1 — Start dynamic, in Python

```python
import atomic_compute

ctx = atomic_compute.Context()
result = (
    ctx.text_file("data/events.txt")       # Python reads local or remote paths
       .flat_map(lambda line: line.split())
       .map(lambda w: (w.lower(), 1))
       .reduce_by_key(lambda a, b: a + b)
       .collect()
)
```

Python lambdas are pickled with `cloudpickle` and executed by the embedded PyO3 runtime on workers. This works immediately — no compilation step.

### Step 2 — Optionally re-express the hot path in Rust

If (and only if) you need more throughput, re-express the job in Rust using the `#[task]` API. This is a separate program, not a patch to the Python driver — and the Python version keeps working:

```rust
use atomic_compute::{context::Context, env::Config, task};

#[task]
fn tokenize(line: String) -> Vec<(String, u64)> {
    line.split_whitespace()
        .map(|w| (w.to_lowercase(), 1u64))
        .collect()
}

fn main() -> anyhow::Result<()> {
    let ctx = Context::new_with_config(Config::distributed(workers)?)?;
    ctx.text_file("s3://my-bucket/events/")?
       .flat_map_task(Tokenize)
       .reduce_by_key(Add)
       .collect()
}
```

### Step 3 — Redeploy the same binary

```bash
atomic build && atomic ship --workers user@host1,user@host2
```

Workers now execute the compiled Rust `#[task]` function instead of deserializing a Python lambda. The Python prototype still runs unchanged — it uses the PyO3 runtime path and produces the same results. Both versions are independently deployable from the same cluster.

**Language capability summary:**

| Feature | Rust | Python | TypeScript |
| --- | --- | --- | --- |
| `#[task]` compile-time dispatch | yes | no | no |
| Closure / lambda UDFs | yes (local) | yes (pickled) | yes (V8 source string) |
| SQL (`SqlContext`) | yes | yes | yes |
| Streaming (`StreamingContext`) | yes | yes | yes |
| Graph (`Graph`, 6 algorithms) | yes | yes | yes |
| Broadcast variables / accumulators | yes | yes | yes |
| `join`, `cogroup`, `sort_by`, `glom`, `cache`, `checkpoint` on RDD | yes | yes | yes |
| S3 `text_file` / `save_as_text_file` | yes | yes (`s3` feature) | yes (`s3` feature) |
| Pregel custom vertex programs | yes | yes (`run_pregel`) | yes (`runPregelF64`) |

---

## SQL Queries

`atomic-sql` wraps [Apache DataFusion](https://github.com/apache/datafusion) — a full query optimizer with 30+ rewrite rules, Arrow columnar execution, and Parquet/CSV/JSON readers.

**Python:**

```python
from atomic_compute import SqlContext

ctx = SqlContext()
ctx.register_parquet("orders", "data/orders.parquet")   # local path

df = ctx.sql("""
    SELECT customer_id,
           COUNT(*)      AS order_count,
           SUM(amount)   AS total_spent
    FROM orders
    WHERE status = 'completed'
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 100
""")

df.show()
df.write_parquet("/tmp/top_customers/")

# Python-only: export to PyArrow Table → Pandas
table = df.to_arrow()
pandas_df = table.to_pandas()
```

**Python — register an RDD as a SQL table** (Python only; schema required):

```python
from atomic_compute import Context, SqlContext

rdd_ctx = Context()
sql_ctx = SqlContext()

rdd = rdd_ctx.parallelize([{"id": 1, "val": 2.5}, {"id": 2, "val": 3.0}])
sql_ctx.register_rdd("data", rdd, {"id": "int64", "val": "float64"})
df = sql_ctx.sql("SELECT * FROM data WHERE val > 2.0")
```

**TypeScript:**

```typescript
import { SqlContext } from "@atomic-compute/js";

const ctx = new SqlContext();
ctx.registerParquet("orders", "data/orders.parquet");

const df = ctx.sql(`
    SELECT customer_id, COUNT(*) AS order_count
    FROM orders
    GROUP BY customer_id
`);
df.show();
df.writeParquet("/tmp/output/");
```

---

## Natural Language Queries (`atomic-nlq`)

Ask a question in plain English; Atomic plans the analysis, runs it, and streams its progress back
live. This is built into the engine, not a chatbot bolted on top.

**How it works (for engineers).** An LLM (OpenAI) turns the question into a **`WorkflowPlan`** — a
small dependency graph of *tool calls* — and an executor runs the steps in parallel on Atomic,
looping until the question is answered. Tools are built-in SQL plus any Python/JS tools you register.
SQL is one tool — and *inside* a SQL step, LLM operations (`llm_filter`, `llm_map`, `embed`,
`vector_search`) are real DataFusion query-plan operators, so the optimizer can batch per-row LLM
calls into grouped API requests instead of one call per row.

```text
User: "find customers who bought luxury items and estimate lifetime value"
         │
         ▼  LlmPlanner (OpenAI) → WorkflowPlan: a graph of tool calls
    a: sql_query   { "SELECT … FROM orders" }
    b: llm_filter  { "is this a luxury item?" }   depends_on: [a]
         │
         ▼  WorkflowExecutor → parallel dependency waves on Atomic
    sql_query → DataFusion (LlmBatchingRule batches per-row LLM ops) → RddScanExec / LlmFilterExec
    python / js tools → atomic-worker PyO3 / V8 runtimes
         │
         ▼  AgentLoop evaluates results → answer + VisualizationSpec; repeat until done
         ▼  every step streamed back to the caller as AgentEvents
```

Wiring LLM calls into a distributed query engine as first-class, batchable plan operators — wrapped
in a streaming agent loop — is not something other distributed compute frameworks offer.

---

## Distributed Subagents (`agent_step`)

`agent_step` puts a multi-round LLM agent loop **inside the map-reduce engine itself**: instead of
writing a one-shot prompt per row, you get one *subagent* per partition that can reason over several
rounds, then the driver reduces all subagents' findings into one result set. The agent loop is owned
by the framework (atomic-nlq's `AgentLoop`) — callers never write the loop, retry logic, or an HTTP
client. You supply a **config** (model, system prompt, round budget, token budget); the same config
shape works identically from Rust, Python, or JavaScript, and dispatches through the *same* op
pipeline as `map_task`/`filter_task` — no separate execution path, no closures shipped over the wire.

### What actually happens per partition

For each partition, the worker runs one subagent that processes its input elements **one at a time,
in order**. For each input it loops up to `max_rounds` times, sending the running conversation back to
the LLM each round, until either the model's response contains a `FINAL ANSWER:` marker (early exit)
or `max_rounds` is reached. The loop also stops early if `max_tokens_total` (an approximate,
char-based token budget shared across *all* inputs in that partition) would be exceeded — remaining
inputs in the partition are returned with `budget_exceeded: true` and an empty answer rather than
silently dropped. One `AgentFindings` record comes back per input element, in input order:

| Field | Meaning |
| --- | --- |
| `input_id` | Index of the input within its partition (0-based) |
| `answer` | Final response text (the `FINAL ANSWER:` prefix is stripped if present) |
| `rounds` | How many LLM rounds this input actually took |
| `confidence` | Currently always `1.0` unless `budget_exceeded` zeroed it out |
| `budget_exceeded` | `true` if the partition's token budget ran out before this input got a real answer |

### Config fields (`AgentStepPayload`)

| Field | Type | Meaning |
| --- | --- | --- |
| `model` | string | e.g. `"gpt-4o-mini"`, `"claude-haiku-4-5-20251001"` |
| `system_prompt` | string | Task instructions sent as the system message every round |
| `max_rounds` | int | Per-input round cap |
| `provider` | string | `"openai"` (default) or `"anthropic"` — selects the API client and reads the matching env var (`OPENAI_API_KEY` / `ANTHROPIC_API_KEY`) |
| `tool_refs` | string[] | Names of tools the model is told it may reference (see limitation below) |
| `output_schema` | string \| null | Optional JSON Schema; if set, each answer is checked to be valid JSON and wrapped in an `{"error": ...}` envelope if it isn't (best-effort, not full schema validation) |
| `max_tokens_total` | int \| null | Approximate (`chars / 4`) token budget shared across the whole partition |

### Rust example

```rust
atomic_nlq::agent_runner::register();   // wires the agent loop into this binary — call once at startup

let config = AgentStepPayload {
    model: "gpt-4o-mini".to_string(),
    system_prompt: "Extract the key obligation (who must do what by when) as one sentence. \
                     End with FINAL ANSWER: <text> once confident.".to_string(),
    max_rounds: 2,
    tool_refs: vec![],
    provider: "openai".to_string(),
    output_schema: None,
    max_tokens_total: Some(20_000),
};

let docs = ctx.parallelize_typed(legal_clauses, 4);   // 4 partitions = 4 subagents in parallel
let findings = docs.agent_step(config).collect()?;

for f in &findings {
    println!("[{}] rounds={} -> {}", f.input_id, f.rounds, f.answer);
}
```

### Python example

```python
findings = ctx.parallelize(legal_clauses, num_partitions=4).agent_step({
    "model": "gpt-4o-mini",
    "system_prompt": "Extract the key obligation as one sentence.",
    "max_rounds": 2,
    "provider": "openai",
    "max_tokens_total": 20_000,
})
for f in findings:
    print(f["input_id"], f["rounds"], f["answer"])
```

### TypeScript example

```typescript
const findings = rdd.agentStep({
  model: "gpt-4o-mini",
  systemPrompt: "Extract the key obligation as one sentence.",
  maxRounds: 2,
  provider: "openai",
  maxTokensTotal: 20_000,
});
```

Full runnable versions: [`examples/agent_step`](examples/agent_step) (Rust), `examples/py-demo/src/agent_step.py`,
and the heavier [`examples/agent_code_audit`](examples/agent_code_audit) (multi-round severity review)
and [`examples/agent_news_triage`](examples/agent_news_triage) (urgency ranking) walkthroughs.

### Current limitation: `tool_refs` is a prompt hint, not tool execution

`tool_refs` is forwarded into the system prompt as a list of names the model is told it "may
reference" — the `AgentStep` runner does **not** parse tool-call responses or invoke
`ToolRegistry` tools on the model's behalf today. This is different from the NLQ `WorkflowExecutor`
path above, which *does* execute `sql_query` and registered Python/JS tools as real steps. Real
in-loop tool execution for `agent_step` is open work, tracked in
`notes/agentic-udf-future-design.md`.

### Production-safety knobs

A long-running, rate-limited multi-round LLM call doesn't fit the retry/timeout assumptions tuned
for cheap CPU tasks, so `AgentStep` pipelines get separate handling in `DistributedScheduler`:

- **Skipped from speculation** — no duplicate (billed) LLM calls racing each other for the same
  partition.
- **A longer, separate timeout** — `agent_step_timeout` (default 30 min vs. the 5-min cheap-task
  default), configurable via `Config::builder().agent_step_timeout_secs(n)` or the
  `ATOMIC_AGENT_STEP_TIMEOUT_SECS` env var.
- **Cost-aware retry logging** — there is no per-input checkpointing within a partition, so a retried
  `AgentStep` partition re-runs its *entire* agent loop from scratch. The scheduler logs a clear
  warning on every such retry, naming the run/task IDs, so a re-incurred LLM bill for already-answered
  inputs shows up in logs rather than being silent.

---

## Deployment — Ship a Static Binary in 60 Seconds

```bash
# Install the CLI
cargo install --path crates/atomic-cli

# Cross-compile a fully static Linux binary (no deps, no JVM, no Python)
atomic build --target x86_64-unknown-linux-musl

# Upload to workers via SSH with host-key verification + SHA-256 integrity check
atomic ship --workers user@10.0.0.101,user@10.0.0.102

# Start workers (same binary, different flag)
./my_app --worker --port 10001
```

The `ship` command verifies the remote host against `~/.ssh/known_hosts`, uploads via SFTP, verifies the SHA-256 checksum on the remote, and renames atomically. No Docker, no registry, no Kubernetes required.

### Or deploy on Kubernetes (Helm)

The same-binary model maps cleanly onto K8s: **one image** runs both roles, and the
`REGISTRY_FINGERPRINT` handshake matches the immutable-image rolling-update invariant. Workers are a
horizontally-scalable `StatefulSet` behind a headless `Service`; the driver discovers them by DNS.

```bash
docker build -f deploy/Dockerfile --build-arg BIN=my_app -t myrepo/atomic:0.1 .
helm install demo deploy/helm/atomic \
  --set image.repository=myrepo/atomic --set image.tag=0.1 --set worker.replicas=3
# optional: worker autoscaling (DNS re-resolution picks up new pods automatically)
helm upgrade demo deploy/helm/atomic --set autoscaling.enabled=true
```

The chart includes the worker StatefulSet, headless Service, driver Job/Deployment, ConfigMap,
optional `HorizontalPodAutoscaler`, optional mTLS, and `/health` + `/metrics` probes. See
[deploy/README.md](deploy/README.md). Kubernetes is an *option*, not a requirement.

---

## Feature Matrix

| Category | Feature | Status |
| --- | --- | --- |
| **Core** | `#[task]` compile-time dispatch | yes |
| | `task_fn!` inline anonymous tasks | yes |
| | Local thread-pool execution | yes |
| | Distributed TCP execution | yes |
| | Lazy pipeline staging (multi-op `TaskEnvelope`) | yes |
| | Speculative execution | yes |
| | Job cancellation | yes |
| **RDD API** | `map`, `filter`, `flat_map`, `reduce_by_key`, `group_by_key` | yes |
| | `join`, `left_outer_join`, `right_outer_join`, `full_outer_join` (shuffle-based) | yes |
| | `cogroup`, `cogroup_shuffle` — distributed, no driver collect | yes |
| | `flat_map_values`, `map_partitions_to_pair` | yes |
| | `repartition_shuffle(n)` — element-level redistribution | yes |
| | `sort_by_key` — distributed (sample → range partition → local sort) | yes |
| | `sort_by_task` — distributed non-pair sort via a registered key task | yes |
| | `fold_by_key`, `aggregate_by_key`, `subtract_by_key` | yes |
| | `tree_reduce`, `tree_aggregate` | yes |
| | `to_local_iterator`, `collect_as_map`, `count_approx` | yes |
| | `to_debug_string` (DAG lineage printer) | yes |
| | Custom partitioner — local (`partition_by`) + distributed (`partition_by_named` / `register_partitioner!` / `TypedPartitioner`) | yes |
| | `cache`, `persist`, `unpersist`, `checkpoint` | yes |
| | `MemoryAndDisk` / `DiskOnly` storage levels | yes |
| | Distributed RDD cache + locality scheduling (serve cached partitions from holding worker) | yes |
| **Shuffle** | Hash shuffle + disk spill | yes |
| | Sort-based shuffle — consolidated file + **lazy k-way sort-merge** reduce; distributed via `register_sort_shuffle_map!` | yes |
| | Adaptive partition coalescing | yes |
| | Shuffle-map stage fault recovery | yes |
| | Lineage recompute on executor loss (lost map output → `FetchFailed` → stage resubmit) | yes |
| **SQL** | DataFusion query engine (30+ optimizer rules) | yes |
| | Parquet, CSV, JSON readers | yes |
| | RDD-backed table provider | yes |
| | DataFrame write (Parquet, CSV) | yes |
| | SQL UDF registration (Python callable) | yes |
| **Streaming** | Micro-batch DStream (`StreamingContext`) — Rust + Python + JS | yes |
| | `map`, `filter`, `flat_map`, `reduce_by_key`, `join`, `update_state_by_key` | yes |
| | Checkpoint (bincode, atomic write) | yes |
| | Structured Streaming (`atomic-structured`) — continuous SQL/DataFrame queries | yes |
| | Tumbling / sliding / session event-time windows + watermark + late-data drop | yes |
| | Stream-stream joins (inner / left / right outer, time-bounded) | yes |
| | Output modes: Append / Update / Complete | yes |
| | State store with checkpoint + recovery | yes |
| | Kafka source/sink | yes (`kafka` feature; DStream + Structured) |
| | Sinks: memory / console / file (parquet) / Kafka | yes |
| | Distributed streaming sources (`DistributedSource` — file splits + Kafka Direct to workers) | yes |
| | Distributed structured-streaming state — sharded window/session/join state (`.distributed(n)`, `MergeState`, per-shard checkpoint, report-back affinity) | yes |
| **Graph** | `Graph<VD,ED>` + Pregel engine — Rust | yes |
| | `Graph(vertices, edges)` + 6 algorithms — Python + JS | yes |
| | PageRank, SSSP, SCC, LabelPropagation, TriangleCount, CC | yes |
| **Language Bindings** | Python (`atomic-compute` on PyPI) — RDD + SQL + Graph + Streaming | yes |
| | TypeScript/JavaScript (`@atomic-compute/js` on npm) — RDD + SQL + Graph + Streaming | yes |
| | `BroadcastVar`, `Accumulator` — Python + JS | yes |
| | `join`, `sort_by`, `glom`, `cache`, `checkpoint` on RDD — Python + JS | yes |
| | Python → Arrow (`df.to_arrow()`) | yes |
| | Python RDD → SQL bridge (`register_rdd`) | yes |
| | Polyglot UDF preflight — JS native-fn rejection + `*WithContext` capture; Python `cloudpickle` load round-trip | yes |
| **Infrastructure** | S3 object store — Rust only (`s3` feature) | yes |
| | Mutual TLS (`tls` feature, rustls) | yes |
| | Prometheus `/metrics` endpoint | yes |
| | Dynamic worker heartbeat + removal | yes |
| | Kubernetes — Dockerfile + Helm chart (worker StatefulSet, headless Service, HPA, mTLS) | yes |
| | DNS worker discovery (`--workers dns:<svc>:<port>`) + live re-resolution for autoscaling | yes |
| | `atomic build` (musl static binary) | yes |
| | `atomic ship` (SSH/SFTP, host-key verified) | yes |
| | S3 I/O for Python + JS bindings (`s3` feature) | yes |
| | Pregel custom vertex programs — Python + JS | yes |
| **NLQ** | LLM-native DataFusion plan nodes | yes |
| | `LlmBatchingRule` optimizer | yes |
| | `InMemoryVectorIndex` | yes |
| **Agentic** | `agent_step` distributed subagents — Rust + Python + JS | yes |
| | Speculation-skip for `AgentStep` partitions | yes |
| | Agent-aware per-task timeout (separate from cheap-CPU-task default) | yes |
| | Cost-aware retry warning logging for `AgentStep` partitions | yes |

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│  Driver (Python / TypeScript / Rust)                        │
│                                                             │
│  Context → TypedRdd → StagedPipeline → TaskEnvelope         │
│                                    │                        │
│              AtomicSqlContext → DataFusion LogicalPlan       │
│                                    │                        │
│              NlqContext → LlmPlanner → WorkflowPlan         │
└────────────────────────┬────────────────────────────────────┘
                         │  TCP (optional mTLS)
            ┌────────────┼────────────┐
            │            │            │
      ┌─────▼──┐   ┌─────▼──┐  ┌─────▼──┐
      │ Worker │   │ Worker │  │ Worker │
      │        │   │        │  │        │
      │ TASK_  │   │ TASK_  │  │ TASK_  │
      │REGISTRY│   │REGISTRY│  │REGISTRY│
      │(same   │   │(same   │  │(same   │
      │binary) │   │binary) │  │binary) │
      └────────┘   └────────┘  └────────┘
```

**Key architectural properties:**

- `TASK_REGISTRY` is linked at compile time via `inventory::submit!`. Workers cannot execute tasks they weren't compiled with — there is no remote code execution surface.
- All distributed wire types use `rkyv` for zero-copy deserialization. No reflection, no dynamic dispatch on the hot path.
- `LocalScheduler` and `DistributedScheduler` share the same `NativeBackend` dispatch. Local-mode tests cover exactly the same codepath as distributed-mode jobs.
- Python UDFs are `cloudpickle`-serialized and executed by the embedded PyO3 runtime in `atomic-worker`. JavaScript UDFs are shipped as source strings and evaluated by the embedded V8 runtime. Both go through the same `TaskEnvelope` wire format as Rust `#[task]` functions.

---

## Crate Layout

| Crate | Purpose |
| --- | --- |
| `atomic-data` | Shared types — RDD traits, task envelopes, wire protocol, shuffle primitives, cache |
| `atomic-compute` | Execution runtime — context, executor, `NativeBackend`, RDD implementations |
| `atomic-scheduler` | `LocalScheduler` (thread-pool) + `DistributedScheduler` (TCP, speculative, heartbeat) |
| `atomic-sql` | SQL layer — `AtomicSqlContext`, `DataFrame`, RDD-backed DataFusion table providers |
| `atomic-streaming` | Micro-batch streaming — `StreamingContext`, `DStream`, `JobScheduler`, Kafka source (`kafka` feature) |
| `atomic-structured` | Structured Streaming — continuous SQL/DataFrame queries, event-time windows, watermark, state store, sinks (on DataFusion + the streaming batch loop) |
| `atomic-graph` | Graph processing — `Graph<VD,ED>`, Pregel engine, built-in algorithms |
| `atomic-nlq` | Natural language query — LLM-native DataFusion plan nodes, `LlmBatchingRule` |
| `atomic-py` | Python bindings (maturin/PyO3) — full RDD + SQL API, Arrow integration |
| `atomic-js` | JavaScript/TypeScript bindings (napi-rs) — full RDD + SQL API |
| `atomic-worker` | Polyglot worker binary — embedded PyO3 + V8 runtimes |
| `atomic-cli` | Cross-compilation (`cargo-zigbuild`) + secure SSH/SFTP binary distribution |
| `atomic-runtime-macros` | `#[task]` and `task_fn!` proc-macros |

---

## Honest Comparison

Atomic stands on its own, but the fair reference point is the incumbent JVM-based engine most teams
already know. Here is the unvarnished trade-off — including where the incumbent still wins:

| Dimension | JVM engine (e.g. Spark) | Atomic |
| --- | --- | --- |
| Task dispatch | Runtime pickle / bytecode shipping | Compile-time `#[task]` dispatch table |
| Worker startup | JVM cold start (seconds) | Native binary (milliseconds) |
| Memory model | GC + off-heap tricks | Rust ownership + rkyv zero-copy |
| Closure safety | Runtime serialization failures | Compile-time — "does it build?" = "does it dispatch?" |
| Deployment | JVM on every node + cluster manager | Static musl binary, SSH upload |
| SQL optimizer | Catalyst (10yr+, highly mature) | DataFusion (excellent, newer) |
| Streaming | Structured Streaming + Kafka, exactly-once, distributed | Micro-batch + Structured Streaming (tumbling/sliding/session windows, stream-stream joins, watermark, output modes); Kafka source/sink (feature-gated); source reads distribute to workers (`DistributedSource`); distributed sharded query state (`.distributed(n)`); exactly-once Kafka→Kafka (transactional sink + `send_offsets_to_transaction`) |
| Kubernetes | Full operator | Dockerfile + Helm chart + DNS discovery (no CRD operator) |
| Ecosystem | Delta Lake, MLflow, hundreds of connectors | Early |
| NLQ / LLM | Plugin / prompt wrapper | First-class plan nodes |
| Stability | Exabyte-tested | Early-stage; strong test suite |

Atomic is likely **faster** for small-to-medium CPU-bound jobs where JVM overhead and GC dominate.
The remaining gap versus mature JVM engines is **feature coverage, not language efficiency**: very
large shuffles and complex joins need sort-merge join, broadcast join, adaptive join-strategy
selection, and skew handling — all of which are tractable in Rust and planned (see
[ROADMAP.md](ROADMAP.md)). Atomic already has shuffle hash joins, disk-spilling shuffle, adaptive
partition coalescing, range-partitioned distributed sort, distributed RDD caching with locality
scheduling, a Kafka source/sink, Structured Streaming (tumbling/sliding/session windows,
stream-stream joins, watermark, output modes) with distributed sharded state and report-back
affinity, and exactly-once Kafka→Kafka delivery. Choose Atomic if you want to avoid the JVM stack
entirely, run the same job in three languages, and accept being an early adopter.

---

## Getting Started

### Rust (examples)

```bash
cargo build --release
cargo run --example task_wordcount
cargo run --example pi
cargo run --example sort
```

### Python (bindings)

```bash
cd crates/atomic-py
pip install maturin
maturin develop --release
pytest tests/
```

### JavaScript / TypeScript (bindings)

```bash
cd crates/atomic-js
npm install
npm run build
npm test
```

### Distributed mode — local loopback (same machine)

Driver and worker run as two separate processes on the same machine. Useful for testing distributed code locally — no shipping needed.

```bash
# 1. Build
cargo build --release

# 2. Start a worker in one terminal
RUST_LOG=info ./target/release/my_app --worker --port 10001

# 3. Run the driver in another terminal
ATOMIC_DEPLOYMENT_MODE=distributed \
ATOMIC_LOCAL_IP=127.0.0.1 \
ATOMIC_WORKERS=127.0.0.1:10001 \
./target/release/my_app
```

### Distributed mode — real cluster (separate machines)

Workers run on remote hosts. You must cross-compile, ship the binary, and start workers before running the driver.

```bash
# 1. Cross-compile a static Linux binary
cargo install --path crates/atomic-cli   # install once
atomic build --target x86_64-unknown-linux-musl

# 2. Ship to worker hosts (SSH key from agent; host-key verified)
atomic ship --workers user@10.0.0.101,user@10.0.0.102

# 3. Start workers on each remote host (SSH in, or via systemd)
ssh user@10.0.0.101 "RUST_LOG=info ./my_app --worker --port 10001 &"
ssh user@10.0.0.102 "RUST_LOG=info ./my_app --worker --port 10001 &"

# 4. Run the driver locally
ATOMIC_DEPLOYMENT_MODE=distributed \
ATOMIC_LOCAL_IP=10.0.0.100 \
ATOMIC_WORKERS=10.0.0.101:10001,10.0.0.102:10001 \
./target/release/my_app
```

See [docs/getting-started.md](docs/getting-started.md), [docs/configuration.md](docs/configuration.md), and [docs/deployment.md](docs/deployment.md) for full documentation.

---

## Status

**Beta** — all core features are implemented and tested. The test suite covers local execution, distributed TCP dispatch, shuffle, streaming, structured streaming, graph, and SQL. Production readiness depends on your risk tolerance and workload:

- **Ready**: Local-mode jobs, SQL analytics (DataFusion), graph algorithms, Python/JS prototyping, musl static binary deployment, Kubernetes deployment (Helm)
- **Early adopter**: Distributed mode on real workloads — core is solid (shuffle joins, fault recovery, distributed cache + locality, speculation, distributed structured-streaming state with report-back affinity, exactly-once Kafka→Kafka); Kafka + Structured Streaming + K8s are newer and the distributed/broker integration tests run behind the `--ignored` CI job
- **Not yet**: Kubernetes CRD operator; Delta/Iceberg table formats; broadcast join / sort-merge join / skew handling for very large shuffles

---

## License

[Apache 2.0](LICENSE)
