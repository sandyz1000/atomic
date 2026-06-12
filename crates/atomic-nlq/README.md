# atomic-nlq

Natural-language analytics for the Atomic engine. A user asks a question in plain English; an LLM
(OpenAI) plans a **`WorkflowPlan`** — a dependency graph of tool calls — and an executor runs it on
Atomic, looping until the question is answered and streaming progress along the way.

## Overview

`atomic-nlq` is an **agent**, not an NL→SQL translator. The LLM never emits raw SQL or a DataFusion
plan directly; it chooses *tools*. SQL is one built-in tool. Inside a SQL step, LLM operations
(`llm_filter`, `llm_map`, `embed`, `vector_search`) are real DataFusion plan operators, so the
optimizer can batch per-row LLM calls.

```text
User NL query
    │
    ▼  LlmPlanner (OpenAI: schema + tool list + query)
WorkflowPlan — a dependency graph of tool calls (JSON)
    │
    ▼  WorkflowExecutor — runs steps in parallel dependency waves (tokio JoinSet)
    │     ├── Builtin(SqlQuery) → AtomicSqlContext.sql()
    │     │     └─ in-SQL LLM ops: LlmBatchingRule → LlmFilterExec / LlmMapExec / EmbedExec
    │     ├── Python(code)      → atomic-worker PyO3 runtime
    │     └── JavaScript(code)  → atomic-worker V8 runtime
    │
    ▼  AgentLoop.evaluate (OpenAI) → { done, answer, visualization? } — repeat until done
AgentResult  (+ a live stream of AgentEvents)
```

> An earlier design had the LLM emit JSON parsed by an `IrParser` into a DataFusion `LogicalPlan`.
> That `ir/` module is gone; the planner now produces a `WorkflowPlan`. The LLM operators survive as
> DataFusion extension nodes used inside SQL steps.

## Features

| Feature | What it does |
| --- | --- |
| **Agentic loop** | `plan → execute → evaluate → repeat` until the question is answered or `max_rounds` is hit |
| **Streaming** | `query_streaming` emits `AgentEvent`s (plan, per-step start/finish, done) for live UIs |
| **SQL tool** | `sql_query` — full DataFusion SQL over registered tables, without the user writing SQL |
| **Python / JS tools** | Register user code as tools the planner can call (PyO3 / V8 on `atomic-worker`) |
| **LLM filter / map** | Per-row predicate or transform evaluated by the LLM, batched by `LlmBatchingRule` |
| **Embed** | Adds a `FixedSizeList<Float32>` embedding column using an OpenAI embedding model |
| **Vector search** | ANN similarity search against a registered `VectorIndexProvider` |
| **RDD-backed tables** | Register `TypedRdd<RecordBatch>` from `atomic-compute` via `build_with_compute` |
| **Visualization hint** | The evaluation step returns a `VisualizationSpec` for dashboards |
| **Retry + jitter** | Exponential back-off with jitter on 429 / 503 / 529 |

## Quick start

```toml
atomic-nlq = { path = "../atomic-nlq" }
```

```text
OPENAI_API_KEY=sk-...        # required (planning, evaluation, embeddings)
```

### Basic usage (RDD-backed table)

```rust
use std::sync::Arc;
use atomic_compute::context::Context;
use atomic_nlq::{NlqConfig, NlqContext};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // SQL-backed analytics need the compute backend.
    let sc = Arc::new(Context::new_with_config(config)?);
    let rdd = sc.parallelize_typed(batches, num_partitions);

    let ctx = NlqContext::build_with_compute(NlqConfig::default(), sc); // reads OPENAI_API_KEY
    ctx.register_rdd("orders", rdd)?;

    // query() runs the agent loop and returns an AgentResult (not a DataFrame).
    let result = ctx.query("show the top 5 customers by total spend").await?;
    println!("{}", result.answer);
    if let Some(viz) = result.visualization {
        println!("suggested chart: {}", viz.chart_type);
    }
    Ok(())
}
```

### Streaming (drive a live UI)

```rust
let (tx, mut rx) = tokio::sync::mpsc::channel(64);
tokio::spawn(async move {
    while let Some(event) = rx.recv().await {
        // AgentEvent::PlanCreated | StepStarted | StepCompleted | RoundEvaluating | Done | …
        println!("{event:?}");
    }
});
let result = ctx.query_streaming("count events grouped by user_id", tx).await?;
```

### Dry-run the plan

```rust
let plan = ctx.plan("find customers who bought luxury items").await?; // WorkflowPlan, not executed
for step in &plan.steps {
    println!("{}: {} (depends_on {:?})", step.id, step.tool, step.depends_on);
}
```

### Register a Python/JS tool

```rust
use atomic_nlq::{ToolDefinition, ToolRuntime};

ctx.register_tool(ToolDefinition {
    name: "zscore".into(),
    description: "Standard-score a numeric column the LLM passes in.".into(),
    input_schema: serde_json::json!({ "type": "object", "properties": { "col": { "type": "string" } } }),
    runtime: ToolRuntime::Python("def run(args, inputs): ...".into()),
});
```

The tool's name and description are included in the planner prompt, so the LLM knows when to call it.

### Register a DataFusion UDF (used inside SQL steps)

```rust
ctx.sql_ctx(); // the AtomicSqlContext
// Scalar/aggregate UDFs are registered via the ToolRegistry:
// registry.register_scalar(my_scalar_udf, "Returns true for luxury categories");
```

### Vector search

```rust
use std::sync::Arc;
use atomic_nlq::InMemoryVectorIndex;

let index = Arc::new(InMemoryVectorIndex::new("products", 1024));
// index.upsert(id, embedding).await?;
ctx.register_vector_index(index);

let result = ctx.query("find products similar to 'luxury handbag'").await?;
```

## Configuration

`NlqConfig::default()` reads env vars; fields can also be set manually.

| Field | Default | Description |
| --- | --- | --- |
| `openai_api_key` | `$OPENAI_API_KEY` | OpenAI API key (planning, evaluation, embeddings) |
| `openai_base_url` | `https://api.openai.com/v1` | Override for proxies / Azure OpenAI |
| `model` | `gpt-4o` | Model for planning and agent evaluation |
| `embed_model` | `text-embedding-3-small` | Embedding model for `embed` |
| `max_rounds` | `5` | Max agent rounds before returning the best result so far |
| `llm_batch_size` | `50` | Rows per LLM API call in `llm_filter` / `llm_map` |
| `max_chunk_bytes` | `200_000` | Max serialized bytes per chunk (~50k tokens) |
| `max_retries` | `3` | Retries on 429 / 503 / 529 |
| `timeout_secs` | `60` | HTTP request timeout |

`NlqContext::build` validates config and panics immediately on an empty key or zero batch size /
chunk size / round count.

## Tools & operators

The planner picks from these **tools**:

```text
sql_query     — run a SQL SELECT over registered tables (returns a DataFrame)
llm_filter    — per-row boolean predicate via the LLM
llm_map       — per-row transform via the LLM (Utf8 / Int64 / Float64 / Boolean output)
embed         — add a FixedSizeList<Float32> embedding column (OpenAI)
vector_search — ANN top-k against a registered vector index
<your tools>  — any Python/JS tool you registered
```

`llm_filter` / `llm_map` / `embed` / `vector_search` are also DataFusion **extension operators**
(`LlmFilterNode` + `LlmFilterExec`, etc.) so they can appear inside a `sql_query` plan and be batched
by `LlmBatchingRule`.

## Architecture

```text
crates/atomic-nlq/src/
├── context.rs               — NlqContext: public entry point (build / query / query_streaming / plan)
├── config.rs                — NlqConfig
├── errors.rs                — NlqError
├── registry.rs              — ToolRegistry: builtin + Python/JS tools; DataFusion UDF registration
├── openai/                  — OpenAiClient (chat + embeddings, retry/back-off)
├── planner/
│   ├── llm_planner.rs       — LlmPlanner: NL query → WorkflowPlan
│   └── prompt.rs            — system-prompt builder
├── workflow/
│   ├── mod.rs               — WorkflowPlan / WorkflowStep / StepOutput
│   ├── agent_loop.rs        — AgentLoop: plan → execute → evaluate → repeat; AgentResult
│   ├── executor.rs          — WorkflowExecutor: parallel dependency-wave execution
│   └── streaming.rs         — AgentEvent / VisualizationSpec
├── nodes/                   — LlmFilterNode/Exec, LlmMapNode/Exec, EmbedNode/Exec, VectorSearchNode/Exec
├── optimizer/
│   └── llm_batching_rule.rs — OptimizerRule: batch per-row LLM calls in a SQL plan
├── physical/
│   └── extension_planner.rs — ExtensionPlanner + QueryPlanner wiring
└── vector/
    ├── provider.rs          — VectorIndexProvider trait
    └── in_memory.rs         — InMemoryVectorIndex (cosine similarity)
```

## Running the demo

```bash
# With a key: runs the full agentic NLQ pipeline
OPENAI_API_KEY=sk-... cargo run --example nlq

# Without a key: falls back to direct SQL, demonstrating DataFusion integration
cargo run --example nlq
```

## Testing

```bash
cargo test -p atomic-nlq
```

Context-level tests (`tests/test_context.rs`) run without an API key. The end-to-end LLM test
(`test_full_nlq_pipeline`) runs only when `OPENAI_API_KEY` is set, and is skipped silently otherwise:

```bash
OPENAI_API_KEY=sk-... cargo test -p atomic-nlq test_full_nlq_pipeline
```
