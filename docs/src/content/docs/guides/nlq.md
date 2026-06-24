---
title: Natural Language Queries
description: Ask questions in plain language; an LLM plans and runs a tool workflow.
---

`atomic-nlq` answers analytics questions stated in plain language. An LLM plans
a workflow — a dependency graph of tool calls — and an executor runs it on the
SQL and compute layers, repeating until the question is answered. The LLM emits
a tool graph, not SQL directly; `sql_query` is one tool among several.

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
User question
  └─ LlmPlanner (OpenAI: schema + tool list + question) → WorkflowPlan
       └─ WorkflowExecutor runs steps in parallel dependency waves
            ├─ sql_query     → AtomicSqlContext.sql()
            ├─ python(code)  → worker PyO3 runtime
            └─ javascript(code) → worker V8 runtime
       └─ AgentLoop evaluates → { done, answer, visualization? }
            └─ repeat until done or max rounds
```

## Two layers

1. **Agentic workflow.** The planner produces a `WorkflowPlan`; the executor
   runs tool calls in dependency waves; the agent loop evaluates results and
   decides whether to run another round.

2. **LLM operators inside SQL.** A SQL step can carry DataFusion extension nodes
   — `LlmFilter`, `LlmMap`, `Embed`, `VectorSearch` — so per-row LLM work runs
   inside a query. A batching rule groups per-row calls into batched API
   requests.

## Types

| Type | Role |
|---|---|
| `NlqContext` | Entry point; wraps the SQL context, OpenAI client, and agent loop |
| `LlmPlanner` | Calls OpenAI and produces a `WorkflowPlan` |
| `WorkflowExecutor` | Runs steps in parallel dependency waves |
| `AgentLoop` | plan → execute → evaluate → repeat |
| `ToolRegistry` | Built-in `sql_query` plus user Python/JS tools |
| `InMemoryVectorIndex` | In-memory index for vector search |
