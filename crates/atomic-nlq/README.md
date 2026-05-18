# atomic-nlq

Natural language query layer for the Atomic analytics platform. Translates plain-English questions into executable [DataFusion](https://github.com/apache/datafusion) plans using Anthropic's Claude API.

## Overview

`atomic-nlq` sits between the user and `atomic-sql`. A user writes a natural language query; the crate calls Claude to produce a structured JSON plan, parses it into a DataFusion `LogicalPlan`, and hands it off to the execution engine.

```text
User NL query
    │
    ▼  LlmPlanner (Anthropic API: schema + UDF list + query)
Structured JSON plan
    │
    ▼  IrParser
DataFusion LogicalPlan
    │  (relational ops: TableScan / Filter / Aggregate / Join / Sort / Limit)
    │  (extension ops:  LlmFilter / LlmMap / Embed / VectorSearch)
    │
    ▼  DataFusion optimizer  (predicate push-down, projection pruning, …)
    ▼  LlmBatchingRule       (groups per-row LLM calls into batched requests)
    │
    ▼  Physical plan + execution
Arrow RecordBatch stream
```

## Features

| Feature | What it does |
|---------|-------------|
| **Relational queries** | Full SQL-style plans without writing SQL — scan, filter, aggregate, sort, join, limit |
| **LLM filter** | `LlmFilter` — per-row boolean predicate evaluated by the LLM (e.g. "is this a luxury item?") |
| **LLM map** | `LlmMap` — per-row text/numeric transformation (e.g. "summarise this review in one sentence") |
| **Embed** | `Embed` — adds a `FixedSizeList<Float32>` embedding column using Voyage AI |
| **Vector search** | `VectorSearch` — ANN similarity search against a registered `VectorIndexProvider` |
| **UDF integration** | Register Rust / Python / JS scalar UDFs; the LLM planner sees their names and descriptions |
| **RDD-backed tables** | Register `TypedRdd<RecordBatch>` tables from `atomic-compute` via `build_with_compute` |
| **Retry + jitter** | Automatic exponential back-off with jitter on 429 / 503 / 529 responses |
| **Safety guards** | Payload size cap (200 KB), response length validation, query sanitisation, config validation |

## Quick start

Add to `Cargo.toml`:

```toml
atomic-nlq = { path = "../atomic-nlq" }
```

Set environment variables:

```
ANTHROPIC_API_KEY=sk-ant-...
VOYAGE_API_KEY=pa-...        # only needed for Embed nodes
```

### Basic usage

```rust
use atomic_nlq::{NlqConfig, NlqContext};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ctx = NlqContext::build(NlqConfig::default());

    // Register an Arrow in-memory table
    ctx.sql_ctx()
        .register_record_batches("orders", schema, batches)
        .await?;

    // Execute a natural language query
    let df = ctx.query("show the top 5 customers by total spend").await?;
    df.show().await?;
    Ok(())
}
```

### Registering custom UDFs

```rust
use atomic_nlq::registry::NlqRegistry;

ctx.registry.register_scalar(
    "is_luxury",
    "Returns true if the product category is a luxury item",
    "(category: Utf8) -> Boolean",
    |args| { /* your implementation */ },
);
```

The UDF name and description are included in the LLM planner prompt automatically.

### Vector search (semantic similarity)

```rust
use std::sync::Arc;
use atomic_nlq::vector::in_memory::InMemoryVectorIndex;

// Register a 1024-dimensional index
let index = Arc::new(InMemoryVectorIndex::new("products", 1024));
index.upsert(1, embedding_vector).await?;
ctx.register_vector_index(index);

// Now the LLM can emit Embed + VectorSearch nodes for semantic queries:
let df = ctx
    .query("find products similar to 'luxury handbag'")
    .await?;
```

### RDD-backed tables (with atomic-compute)

```rust
use atomic_nlq::{NlqConfig, NlqContext};
use atomic_compute::context::Context;
use std::sync::Arc;

let sc = Arc::new(Context::new_with_config(config)?);
let rdd = sc.parallelize_typed(batches, num_partitions);

let ctx = NlqContext::build_with_compute(NlqConfig::default(), sc);
ctx.register_rdd("events", rdd)?;

let df = ctx.query("count events grouped by user_id").await?;
```

## Configuration

`NlqConfig` can be constructed manually or via `Default` (reads env vars):

| Field | Default | Description |
|-------|---------|-------------|
| `anthropic_api_key` | `$ANTHROPIC_API_KEY` | Anthropic API key |
| `embed_api_key` | `$VOYAGE_API_KEY` | Voyage AI API key (for `Embed` nodes) |
| `default_model` | `claude-sonnet-4-6` | Claude model for NL-to-plan translation |
| `embed_model` | `voyage-3` | Embedding model |
| `embed_model_dim` | `None` (auto) | Override embedding dimension |
| `embed_base_url` | Voyage AI v1 endpoint | Embed API base URL |
| `llm_batch_size` | `50` | Rows per LLM API call in LlmFilter/LlmMap |
| `max_chunk_bytes` | `200_000` | Max serialised bytes per chunk (≈50 k tokens) |
| `max_retries` | `3` | Retries on 429 / 503 / 529 |
| `timeout_secs` | `60` | HTTP request timeout |
| `anthropic_base_url` | Anthropic API endpoint | Anthropic API base URL |

`NlqContext::build` panics immediately on invalid config (empty API key, zero batch size).

## Plan node reference

The LLM produces JSON nodes from this grammar. All node types:

```
TableScan   — scan a registered table
Filter      — WHERE predicate (SQL expression)
Projection  — SELECT expressions
Aggregate   — GROUP BY + aggregation functions (sum / count / avg / min / max)
Sort        — ORDER BY
Limit       — SKIP + FETCH
Join        — Inner / Left / Right / Full / LeftSemi / LeftAnti
LlmFilter   — per-row boolean filter via LLM
LlmMap      — per-row value transform via LLM (Utf8 / Int64 / Float64 / Boolean output)
Embed       — add a FixedSizeList<Float32> embedding column (Voyage AI)
VectorSearch — ANN top-k search against a registered vector index
```

## Architecture

```
crates/atomic-nlq/src/
├── context.rs               — NlqContext: public entry point
├── config.rs                — NlqConfig
├── errors.rs                — NlqError
├── registry.rs              — NlqRegistry: UDF registration + description
├── anthropic/
│   ├── client.rs            — AnthropicClient (messages API)
│   ├── embed_client.rs      — EmbedClient (Voyage AI /embeddings)
│   ├── retry.rs             — exponential back-off with jitter
│   └── types.rs             — request / response types
├── planner/
│   ├── llm_planner.rs       — LlmPlanner: NL query → JSON plan
│   └── prompt.rs            — system prompt builder
├── ir/
│   ├── json_plan.rs         — JsonPlanNode / JsonExpr serde types
│   └── parser.rs            — IrParser: JSON → DataFusion LogicalPlan
├── nodes/
│   ├── llm_filter.rs        — LlmFilterNode + LlmFilterExec
│   ├── llm_map.rs           — LlmMapNode + LlmMapExec
│   ├── embed.rs             — EmbedNode + EmbedExec
│   └── vector_search.rs     — VectorSearchNode + VectorSearchExec
├── optimizer/
│   └── llm_batching_rule.rs — OptimizerRule: groups per-row LLM calls into batches
├── physical/
│   └── extension_planner.rs — ExtensionPlanner + QueryPlanner wiring
└── vector/
    ├── provider.rs          — VectorIndexProvider trait
    └── in_memory.rs         — InMemoryVectorIndex (cosine similarity, for testing)
```

## Running the demo

```
ANTHROPIC_API_KEY=... cargo run -p atomic-nlq --example nlq_demo
```

## Testing

```
cargo test -p atomic-nlq
```

27 unit tests covering: relational plan parsing, LlmFilter node + batching rule, UDF registry, schema validation.

Integration tests (require a live API key) are gated behind the `integration` feature flag:

```
ANTHROPIC_API_KEY=... cargo test -p atomic-nlq --features integration
```
