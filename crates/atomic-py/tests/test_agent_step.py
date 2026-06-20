"""Tests for Rdd.agent_step — the Python binding over TaskAction::AgentStep.

Requires OPENAI_API_KEY or ANTHROPIC_API_KEY; auto-skips otherwise (same
convention as crates/atomic-nlq/tests/test_agent_step.rs).
"""
import os

import pytest

requires_api_key = pytest.mark.skipif(
    not (os.environ.get("OPENAI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")),
    reason="requires OPENAI_API_KEY or ANTHROPIC_API_KEY",
)


def _provider_and_model():
    if os.environ.get("OPENAI_API_KEY"):
        return "openai", "gpt-4o-mini"
    return "anthropic", "claude-haiku-4-5-20251001"


@requires_api_key
def test_agent_step_local_mode(ctx):
    provider, model = _provider_and_model()
    docs = [
        "Party A shall pay $100 within 30 days.",
        "Party B shall deliver goods by December 31.",
    ]
    findings = ctx.parallelize(docs, 2).agent_step({
        "model": model,
        "system_prompt": "Extract the obligation as one sentence.",
        "max_rounds": 2,
        "provider": provider,
        "max_tokens_total": 10_000,
    })

    assert len(findings) == 2
    for f in findings:
        assert f["answer"]
        assert f["rounds"] >= 1
        assert isinstance(f["budget_exceeded"], bool)


@requires_api_key
def test_agent_step_defaults(ctx):
    provider, model = _provider_and_model()
    findings = ctx.parallelize(["hello", "world"], 1).agent_step({
        "model": model,
        "system_prompt": "Echo the input back in one short sentence.",
        "provider": provider,
    })

    assert len(findings) == 2
    assert {f["input_id"] for f in findings} == {0, 1}


def test_agent_step_missing_required_key(ctx):
    with pytest.raises(KeyError):
        ctx.parallelize(["x"], 1).agent_step({"system_prompt": "no model given"})
