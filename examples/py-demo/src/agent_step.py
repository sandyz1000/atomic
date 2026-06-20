"""
Distributed subagent example using Rdd.agent_step (Python binding).

Runs a framework-native LLM agent loop over each partition of a document set.
Each partition is processed by a subagent that extracts key obligations from
a legal clause, returning one finding dict per input string.

Prerequisites:
  cd crates/atomic-py && maturin develop --release && cd ../..
  export OPENAI_API_KEY=sk-...     # OpenAI (default provider)
  export ANTHROPIC_API_KEY=sk-...  # or Anthropic Claude

If neither key is set, the example exits with a clear message.

Run:
  python3 src/agent_step.py
"""

import os

import atomic_compute

provider = "openai" if os.environ.get("OPENAI_API_KEY") else "anthropic" if os.environ.get("ANTHROPIC_API_KEY") else None
if provider is None:
    print("agent_step example: skipped -- set OPENAI_API_KEY or ANTHROPIC_API_KEY to run.")
    raise SystemExit(0)

model = "gpt-4o-mini" if provider == "openai" else "claude-haiku-4-5-20251001"

ctx = atomic_compute.Context()

docs = [
    "The Supplier shall deliver all goods within 30 days of purchase order receipt.",
    "Late deliveries incur a penalty of 2% per week, capped at 10% of contract value.",
    "Either party may terminate this agreement with 90 days written notice.",
    "The Buyer warrants payment within 45 days of invoice date.",
]

print(f"Running agent_step over {len(docs)} documents ({provider}/{model})...")

findings = ctx.parallelize(docs, num_partitions=2).agent_step({
    "model": model,
    "system_prompt": "Extract the key obligation (who must do what by when) as one sentence.",
    "max_rounds": 2,
    "provider": provider,
    "max_tokens_total": 20_000,
})

for f in findings:
    print(f"[input {f['input_id']}] rounds={f['rounds']} budget_exceeded={f['budget_exceeded']}")
    print(f"  -> {f['answer']}\n")

assert len(findings) == len(docs), "expected one finding per input document"
print(f"agent_step example completed successfully ({len(findings)} findings).")
