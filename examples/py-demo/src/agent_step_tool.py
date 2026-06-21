"""
Distributed subagent with an inline **Python tool** using Rdd.agent_step.

This is the *scripted* tool lane: the tool is an ordinary Python function shipped
with the job as source — no binary rebuild, no redeploy. The worker's embedded
PyO3 runtime executes it when the subagent emits `TOOL_CALL: <name> <json_args>`.
(Contrast the Rust `examples/agent_code_audit`, where tools are compiled `#[task]`
functions baked into the binary.)

A tool is just an ordinary task in an LLM-callable role: define `run(args)`, give
it a name, and pass it under `tools`. The binding ships its source in
`AgentStepPayload.resolved_tools`; changing the tool never requires a recompile.

Prerequisites:
  cd crates/atomic-py && maturin develop --release && cd ../..
  export OPENAI_API_KEY=sk-...     # OpenAI (default provider)
  export ANTHROPIC_API_KEY=sk-...  # or Anthropic Claude

Run:
  python3 src/agent_step_tool.py
"""

import os

import atomic_compute

provider = "openai" if os.environ.get("OPENAI_API_KEY") else "anthropic" if os.environ.get("ANTHROPIC_API_KEY") else None
if provider is None:
    print("agent_step_tool example: skipped -- set OPENAI_API_KEY or ANTHROPIC_API_KEY to run.")
    raise SystemExit(0)

model = "gpt-4o-mini" if provider == "openai" else "claude-haiku-4-5-20251001"

ctx = atomic_compute.Context()

# A Python tool: maps a severity label to a numeric weight. Shipped as source —
# the worker runs `run(args)` with the JSON args the model supplies.
severity_weight_tool = {
    "name": "severity_weight",
    "source": """
def run(args):
    weights = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
    return {"weight": weights.get(args.get("label", ""), 0)}
""",
}

issues = [
    "Unbounded user input is concatenated directly into a SQL query string.",
    "A config value is read once at startup and cached for the process lifetime.",
    "Two threads write to a shared file without a lock.",
    "A debug log line prints a full request body including an auth header.",
]

print(f"Triaging {len(issues)} issues with an inline Python tool ({provider}/{model})...")

findings = ctx.parallelize(issues, num_partitions=2).agent_step({
    "model": model,
    "system_prompt": (
        "You are a security triage agent. Classify the issue's severity as one of "
        "CRITICAL/HIGH/MEDIUM/LOW. Call the `severity_weight` tool with "
        '{"label": "<your label>"} to fetch its numeric weight, then respond with:\n'
        "FINAL ANSWER: <LABEL> (weight <N>) — <one-sentence justification>"
    ),
    "max_rounds": 4,
    "provider": provider,
    "tools": [severity_weight_tool],
    "max_tokens_total": 20_000,
})

for f in findings:
    print(f"[input {f['input_id']}] rounds={f['rounds']} budget_exceeded={f['budget_exceeded']}")
    print(f"  -> {f['answer']}\n")

assert len(findings) == len(issues), "expected one finding per input issue"
print(f"agent_step_tool example completed successfully ({len(findings)} findings).")
