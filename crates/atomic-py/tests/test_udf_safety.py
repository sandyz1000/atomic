"""Coverage for the pickle round-trip check UDFs go through before being staged
for distributed dispatch (A3).

`_verify_picklable` calls the same `pickle_fn` helper that every distributed
`map`/`filter`/`flat_map`/... call site uses internally. It is exposed only so
this round-trip check is testable without a live distributed `Context` —
`Context` construction in distributed mode performs real TCP handshakes against
configured workers at construction time, which is impractical to set up here.
"""

import threading

import pytest
import atomic_compute


def _double(x):
    return x * 2


def test_unpicklable_rejected():
    lock = threading.Lock()
    captures_lock = lambda: lock  # noqa: E731

    with pytest.raises(Exception):
        atomic_compute._verify_picklable(captures_lock)


def test_picklable_accepted():
    atomic_compute._verify_picklable(_double)
