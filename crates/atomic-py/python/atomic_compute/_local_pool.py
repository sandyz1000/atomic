"""Local parallel execution pool for atomic-py.

Uses `concurrent.futures.ProcessPoolExecutor` to dispatch partition-level
closures across OS processes — the same model as PySpark's `local[N]`, which
bypasses the GIL and achieves genuine multi-core parallelism.

`run_partition` is defined at module level so it is importable by name,
which is required for `ProcessPoolExecutor` to serialize it to worker processes.
"""

from __future__ import annotations

import concurrent.futures
from typing import Any

import cloudpickle

_pool: concurrent.futures.ProcessPoolExecutor | None = None
_pool_size: int = 0


def _get_pool(n: int) -> concurrent.futures.ProcessPoolExecutor:
    """Return (or lazily create) a pool sized to `n` workers."""
    global _pool, _pool_size
    if _pool is None or _pool_size != n:
        if _pool is not None:
            _pool.shutdown(wait=False, cancel_futures=True)
        _pool = concurrent.futures.ProcessPoolExecutor(max_workers=n)
        _pool_size = n
    return _pool


def shutdown_pool() -> None:
    """Gracefully shut down the pool. Called by `Context.stop()`."""
    global _pool
    if _pool is not None:
        _pool.shutdown(wait=False, cancel_futures=True)
        _pool = None


def run_partition(fn_bytes: bytes, items: list[Any]) -> list[Any]:
    """Worker entry-point — executes inside a subprocess.

    Must be importable by name (module-level function) for ProcessPoolExecutor
    to pickle it across the process boundary.
    """
    fn = cloudpickle.loads(fn_bytes)
    return list(fn(items))
