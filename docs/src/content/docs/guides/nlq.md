---
title: Natural Language Queries
description: Ask questions in plain language; an LLM plans and runs a tool workflow on the coordinator.
---

`atomic-nlq` answers analytics questions stated in plain language. An LLM runs
**once on the coordinator** (never on workers) and produces a `WorkflowPlan` — a
JSON dependency graph of tool calls. `WorkflowExecutor` dispatches each tool
directly through the Atomic engine (`#[task]` functions, SQL, or Python/JS
runtimes), and an `AgentLoop` iterates until the answer is complete.

The LLM emits a tool graph, not SQL directly; `sql_query` is one tool among
several. LLM operations (`LlmFilter`, `LlmMap`, `Embed`, `VectorSearch`) run as
DataFusion extension operators inside SQL steps — they are real distributed
operators, not per-partition agent loops.

This layer uses OpenAI and requires `OPENAI_API_KEY` in the environment. Tests
that need the API skip when the key is absent.

## Entry point

```rust
use atomic_nlq::{NlqContext, NlqConfig};

let ctx = NlqContext::build_with_compute(NlqConfig::default(), compute_ctx);
ctx.register_rdd("orders", orders_rdd)?;
ctx.register_tool(my_python_tool);          // optional user tools

let result = ctx.query("find customers who bought luxury items").await?;
println!("{}", result.answer);
```

`query` returns an `AgentResult` with the answer, the executed steps, the number
of rounds, and an optional visualization. `query_streaming` emits progress
events through a channel for a live UI. `plan` is a dry run that returns the
workflow plan without executing it.

## How it works

```text
User question (coordinator side)
  └─ LlmPlanner (OpenAI: schema + tool list + question) → WorkflowPlan (JSON DSL)
       └─ WorkflowExecutor dispatches steps through the Atomic engine
            ├─ Builtin(SqlQuery)  → AtomicSqlContext.sql()
            │     (LLM ops run as DataFusion extension nodes inside SQL)
            ├─ Builtin(Filter) → TypedRdd::filter_task
            ├─ Builtin(Aggregate) → TypedRdd::combine_by_key
            ├─ Python(code) → TaskRuntime::Native dispatch via Context::dispatch_pipeline
            └─ JavaScript(code) → TaskRuntime::Native dispatch via Context::dispatch_pipeline
       └─ AgentLoop evaluates (coordinator LLM call) → { done, answer, visualization? }
            └─ repeat until done or max rounds
```

**Key design points:**
- The LLM runs only on the coordinator (driver), never on workers.
- `WorkflowExecutor` resolves tool names through `ToolRegistry` and dispatches
  concrete engine tasks — not per-partition agent loops.
- Python/JS tools are dispatched as `TaskAction::MapPartitions` ops through
  `Context::dispatch_pipeline`, inheriting retry, locality, and fault tolerance.
- LLM operations (`LlmFilter`, `LlmMap`, `Embed`, `VectorSearch`) are DataFusion
  extension operators batched by `LlmBatchingRule` inside SQL queries.

## Types

| Type | Role |
|---|---|
| `NlqContext` | Entry point; wraps the SQL context, OpenAI client, and agent loop |
| `LlmPlanner` | Calls OpenAI and produces a `WorkflowPlan` |
| `WorkflowExecutor` | Runs steps in parallel dependency waves |
| `AgentLoop` | plan → execute → evaluate → repeat |
| `ToolRegistry` | Built-in `sql_query` plus user Python/JS tools |
| `InMemoryVectorIndex` | In-memory index for vector search |
