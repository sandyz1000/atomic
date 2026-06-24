---
title: Distributed Subagents (agent_step)
description: Run a multi-round LLM agent loop per partition inside the compute engine.
---

`agent_step` runs a multi-round LLM agent loop inside the map-reduce engine. Each
partition gets one subagent that reasons over several rounds; the driver reduces
all subagents' findings into one result set. The agent loop is owned by the
framework (`atomic-nlq`'s `AgentLoop`) — you supply a config, not the loop, retry
logic, or an HTTP client.

The same config shape works from Rust, Python, and JavaScript, and dispatches
through the same op pipeline as `map_task` and `filter_task` — no separate
execution path, no closures shipped over the wire.

This feature uses an LLM provider (OpenAI or Anthropic) and reads the matching
API key from the environment.

## What happens per partition

For each partition, the worker runs one subagent that processes its input
elements one at a time, in order. For each input it loops up to `max_rounds`
times, sending the running conversation back to the LLM each round, until either
the response contains a `FINAL ANSWER:` marker (early exit) or `max_rounds` is
reached.

The loop stops early if `max_tokens_total` — an approximate, character-based
token budget shared across all inputs in the partition — would be exceeded.
Remaining inputs are returned with `budget_exceeded: true` and an empty answer
rather than silently dropped.

One `AgentFindings` record comes back per input, in input order:

| Field | Meaning |
| --- | --- |
| `input_id` | Index of the input within its partition (0-based) |
| `answer` | Final response text (the `FINAL ANSWER:` prefix is stripped if present) |
| `rounds` | How many LLM rounds this input took |
| `confidence` | Set by the runner; `1.0` unless `budget_exceeded` zeroed it |
| `budget_exceeded` | `true` if the partition's token budget ran out before this input got a real answer |

## Config fields (`AgentStepPayload`)

| Field | Type | Meaning |
| --- | --- | --- |
| `model` | string | e.g. `"gpt-4o-mini"`, `"claude-haiku-4-5-20251001"` |
| `system_prompt` | string | Task instructions sent as the system message each round |
| `max_rounds` | int | Per-input round cap |
| `provider` | string | `"openai"` (default) or `"anthropic"`; selects the API client and reads `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` |
| `tool_refs` | string[] | Tools the model may call mid-loop via `TOOL_CALL: <name> <json_args>` |
| `resolved_tools` | `ResolvedTool[]` | Python/JS tool source, filled in driver-side by `resolve_agent_step` — leave empty; do not construct by hand |
| `output_schema` | string \| null | Optional JSON schema; each answer is checked to be valid JSON and wrapped in an `{"error": ...}` envelope if not (best-effort) |
| `max_tokens_total` | int \| null | Approximate (`chars / 4`) token budget shared across the whole partition |

## Rust

```rust
atomic_nlq::agent_runner::register();   // wire the agent loop into this binary — call once at startup

let config = AgentStepPayload {
    model: "gpt-4o-mini".to_string(),
    system_prompt: "Extract the key obligation (who must do what by when) as one sentence. \
                     End with FINAL ANSWER: <text> once confident.".to_string(),
    max_rounds: 2,
    tool_refs: vec![],
    resolved_tools: vec![],
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

## Python

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

## TypeScript

```typescript
const findings = rdd.agentStep({
  model: "gpt-4o-mini",
  systemPrompt: "Extract the key obligation as one sentence.",
  maxRounds: 2,
  provider: "openai",
  maxTokensTotal: 20_000,
});
```

Runnable examples: `examples/agent_step` (Rust),
`examples/py-demo/src/agent_step.py`, and the longer `examples/agent_code_audit`
(multi-round severity review) and `examples/agent_news_triage` (urgency ranking).

## Tool calling

A subagent calls a tool mid-loop by responding with exactly one line:

```text
TOOL_CALL: <tool_name> <json_args>
```

The runner parses the marker, dispatches the call, appends
`Tool <name> result: <output>` to the conversation, and starts the next round.
The model sees the result and can continue reasoning, call another tool, or emit
`FINAL ANSWER:`. Tool dispatch follows the same two-lane split as the rest of
the engine:

- **Rust tools** are `#[task]` functions of shape `fn(String) -> String` (JSON
  text in, JSON text out — `WireEncode`/`WireDecode` cover only rkyv types, so a
  tool cannot take `serde_json::Value` directly). Put the function's generated
  op ID (`SomeTask::NAME`) straight into `tool_refs`; workers look it up in the
  same `TASK_REGISTRY` used by `map_task`.
- **Python/JS tools** are registered in `atomic-nlq`'s `ToolRegistry` (raw source
  defining a top-level `run(args)` function) and must be resolved once,
  driver-side, before staging the config:

  ```rust
  let config = ctx.resolve_agent_step(config)?;   // fills resolved_tools
  let findings = rdd.agent_step(config).collect()?;
  ```

  `resolve_agent_step` leaves `#[task]` op IDs untouched and copies matching
  `ToolRegistry` Python/JS source into `resolved_tools`, so workers never need a
  registry of their own. It rejects builtin tools (`sql_query`, `llm_filter`, …)
  and unknown names with a clear error. Running Python/JS tools requires building
  with the matching feature (`atomic-nlq/python` or `atomic-nlq/js`); without it,
  a `TOOL_CALL:` to a named-but-unresolved tool returns a clear error string fed
  back into the conversation instead of crashing the worker.

A tool call naming something in neither `TASK_REGISTRY` nor `resolved_tools` also
produces an error string fed back into the conversation, not a panic — the model
can retry with a different tool or answer anyway.

## Production-safety handling

A long-running, rate-limited multi-round LLM call does not fit the retry and
timeout assumptions tuned for cheap CPU tasks, so `AgentStep` pipelines get
separate handling in `DistributedScheduler`:

- **Skipped from speculation** — no duplicate, billed LLM calls racing for the
  same partition.
- **A longer, separate timeout** — `agent_step_timeout` (default 30 minutes vs.
  the 5-minute cheap-task default), set with
  `Config::builder().agent_step_timeout_secs(n)` or `ATOMIC_AGENT_STEP_TIMEOUT_SECS`.
- **Cost-aware retry logging** — there is no per-input checkpointing within a
  partition, so a retried `AgentStep` partition re-runs its entire agent loop
  from scratch. The scheduler logs a warning on every such retry, naming the run
  and task IDs, so a re-incurred LLM bill shows up in logs rather than silently.
