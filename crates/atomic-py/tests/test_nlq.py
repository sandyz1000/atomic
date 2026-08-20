"""NlqContext tests for atomic-py.

Run with:  maturin develop && pytest tests/test_nlq.py -v

No real LLM calls are made — a dummy API key exercises construction, the
SqlContext wiring, and error propagation (an auth failure from the provider)
without needing OPENAI_API_KEY / network access in CI.
"""

import pytest

from atomic_compute import NlqContext


def test_context_creates():
    ctx = NlqContext(api_key="dummy-key")
    assert ctx is not None


def test_rejects_unknown_provider():
    with pytest.raises(ValueError):
        NlqContext(api_key="dummy-key", provider="not-a-provider")


def test_exposes_working_sql_ctx():
    ctx = NlqContext(api_key="dummy-key")
    rows = ctx.sql_ctx().sql("SELECT 42 AS n").collect()
    assert rows == [{"n": 42}]


def test_plan_propagates_provider_auth_error():
    ctx = NlqContext(api_key="dummy-key")
    with pytest.raises(RuntimeError, match="API"):
        ctx.plan("how many rows")
