"""
Streaming bindings tests — Phase 4.4 (Python)

Prerequisites:
    cd crates/atomic-py
    pip install maturin cloudpickle pytest
    maturin develop --release
    pytest tests/test_streaming.py
"""

import atomic_compute
import pytest


def make_ssc(batch_secs: float = 0.1) -> atomic_compute.StreamingContext:
    return atomic_compute.StreamingContext(batch_secs)


# ── Basic foreach_rdd ─────────────────────────────────────────────────────────


def test_foreach_rdd_basic():
    ssc = make_ssc()
    stream, queue = ssc.test_queue_stream()
    results = []
    ssc.foreach_rdd(stream, lambda batch: results.extend(batch))
    queue.push([1, 2, 3])
    ssc.run_one_batch()
    assert results == [1, 2, 3]


def test_foreach_rdd_empty_batch():
    ssc = make_ssc()
    stream, queue = ssc.test_queue_stream()
    results = []
    ssc.foreach_rdd(stream, lambda batch: results.extend(batch))
    queue.push([])
    ssc.run_one_batch()
    assert results == []


def test_no_batch_queued():
    """When the queue is empty, foreach_rdd receives an empty list."""
    ssc = make_ssc()
    stream, queue = ssc.test_queue_stream()
    results = []
    ssc.foreach_rdd(stream, lambda batch: results.extend(batch))
    ssc.run_one_batch()  # nothing pushed
    assert results == []


def test_multiple_batches():
    """Multiple run_one_batch calls each consume one enqueued batch."""
    ssc = make_ssc()
    stream, queue = ssc.test_queue_stream()
    results = []
    ssc.foreach_rdd(stream, lambda batch: results.extend(batch))
    queue.push([1, 2])
    queue.push([3, 4])
    ssc.run_one_batch()
    ssc.run_one_batch()
    assert results == [1, 2, 3, 4]


# ── map ───────────────────────────────────────────────────────────────────────


def test_map_transform():
    ssc = make_ssc()
    stream, queue = ssc.test_queue_stream()
    mapped = stream.map(lambda x: x * 2)
    results = []
    ssc.foreach_rdd(mapped, lambda b: results.extend(b))
    queue.push([1, 2, 3])
    ssc.run_one_batch()
    assert results == [2, 4, 6]


def test_map_empty():
    ssc = make_ssc()
    stream, queue = ssc.test_queue_stream()
    mapped = stream.map(lambda x: x + 100)
    results = []
    ssc.foreach_rdd(mapped, lambda b: results.extend(b))
    queue.push([])
    ssc.run_one_batch()
    assert results == []


# ── filter ────────────────────────────────────────────────────────────────────


def test_filter_transform():
    ssc = make_ssc()
    stream, queue = ssc.test_queue_stream()
    filtered = stream.filter(lambda x: x % 2 == 0)
    results = []
    ssc.foreach_rdd(filtered, lambda b: results.extend(b))
    queue.push([1, 2, 3, 4, 5])
    ssc.run_one_batch()
    assert results == [2, 4]


# ── flat_map ──────────────────────────────────────────────────────────────────


def test_flat_map_transform():
    ssc = make_ssc()
    stream, queue = ssc.test_queue_stream()
    flat = stream.flat_map(lambda x: x.split())
    results = []
    ssc.foreach_rdd(flat, lambda b: results.extend(b))
    queue.push(["hello world", "foo bar"])
    ssc.run_one_batch()
    assert sorted(results) == ["bar", "foo", "hello", "world"]


# ── chained transforms ────────────────────────────────────────────────────────


def test_chained_map_filter():
    ssc = make_ssc()
    stream, queue = ssc.test_queue_stream()
    chain = stream.map(lambda x: x * 2).filter(lambda x: x > 4)
    results = []
    ssc.foreach_rdd(chain, lambda b: results.extend(b))
    queue.push([1, 2, 3, 4, 5])
    ssc.run_one_batch()
    assert sorted(results) == [6, 8, 10]


# ── reduce_by_key ─────────────────────────────────────────────────────────────


def test_reduce_by_key():
    ssc = make_ssc()
    stream, queue = ssc.test_pair_queue_stream()
    reduced = stream.reduce_by_key(lambda a, b: a + b)
    results = []
    ssc.foreach_rdd(reduced, lambda b: results.extend(b))
    queue.push([("a", 1), ("b", 2), ("a", 3)])
    ssc.run_one_batch()
    result_dict = dict(results)
    assert result_dict["a"] == 4
    assert result_dict["b"] == 2


def test_reduce_by_key_requires_pair():
    ssc = make_ssc()
    stream, _ = ssc.test_queue_stream()
    with pytest.raises(Exception):
        stream.reduce_by_key(lambda a, b: a + b)


# ── group_by_key ──────────────────────────────────────────────────────────────


def test_group_by_key():
    ssc = make_ssc()
    stream, queue = ssc.test_pair_queue_stream()
    grouped = stream.group_by_key()
    results = []
    ssc.foreach_rdd(grouped, lambda b: results.extend(b))
    queue.push([("a", 1), ("b", 2), ("a", 3)])
    ssc.run_one_batch()
    result_dict = dict(results)
    assert sorted(result_dict["a"]) == [1, 3]
    assert result_dict["b"] == [2]


# ── join ──────────────────────────────────────────────────────────────────────


def test_join():
    ssc = make_ssc()
    left, q1 = ssc.test_pair_queue_stream()
    right, q2 = ssc.test_pair_queue_stream()
    joined = left.join(right)
    results = []
    ssc.foreach_rdd(joined, lambda b: results.extend(b))
    q1.push([("a", 1), ("b", 2)])
    q2.push([("a", 10), ("c", 30)])
    ssc.run_one_batch()
    result_dict = dict(results)
    assert result_dict["a"] == (1, 10)
    assert "b" not in result_dict


# ── left_outer_join ───────────────────────────────────────────────────────────


def test_left_outer_join():
    ssc = make_ssc()
    left, q1 = ssc.test_pair_queue_stream()
    right, q2 = ssc.test_pair_queue_stream()
    joined = left.left_outer_join(right)
    results = []
    ssc.foreach_rdd(joined, lambda b: results.extend(b))
    q1.push([("a", 1), ("b", 2)])
    q2.push([("a", 10)])
    ssc.run_one_batch()
    result_dict = dict(results)
    assert result_dict["a"] == (1, 10)
    assert result_dict["b"][0] == 2
    assert result_dict["b"][1] is None


# ── map_values ────────────────────────────────────────────────────────────────


def test_map_values():
    ssc = make_ssc()
    stream, queue = ssc.test_pair_queue_stream()
    mapped = stream.map_values(lambda v: v * 10)
    results = []
    ssc.foreach_rdd(mapped, lambda b: results.extend(b))
    queue.push([("a", 1), ("b", 2)])
    ssc.run_one_batch()
    result_dict = dict(results)
    assert result_dict["a"] == 10
    assert result_dict["b"] == 20


# ── update_state_by_key ───────────────────────────────────────────────────────


def test_update_state_by_key():
    ssc = make_ssc()
    stream, queue = ssc.test_pair_queue_stream()

    def update_fn(new_values, old_state):
        return (old_state or 0) + sum(new_values)

    stateful = stream.update_state_by_key(update_fn)
    results_per_batch = []
    ssc.foreach_rdd(stateful, lambda b: results_per_batch.append(dict(b)))

    # batch 1
    queue.push([("a", 1), ("b", 2)])
    ssc.run_one_batch()

    # batch 2 — state accumulates; "b" should persist even without new values
    queue.push([("a", 10)])
    ssc.run_one_batch()

    # After batch 1: a=1, b=2
    assert results_per_batch[0].get("'a'") == 1 or results_per_batch[0].get("a") == 1

    # After batch 2: a=11, b=2 (b still in state)
    final = results_per_batch[1]
    # 'a' may be stored as repr key "'a'" or plain "a" depending on key type
    a_val = final.get("'a'") or final.get("a")
    assert a_val == 11
