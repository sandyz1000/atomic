"""Shared fixtures for atomic-py tests.

Run with:  maturin develop && pytest
"""
import pytest
import atomic


@pytest.fixture
def ctx():
    """A local (non-distributed) Context with 2 partitions."""
    return atomic.Context(default_parallelism=2)
