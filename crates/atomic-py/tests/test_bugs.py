"""Regression tests for confirmed atomic-py bugs (PY-B1 through PY-B6).

Each test is named after the bug ID it guards.  A failure here means a
previously-fixed bug has regressed.
"""
import pytest
import atomic_compute


@pytest.fixture
def ctx():
    return atomic_compute.Context(default_parallelism=2)


# ── PY-B1: group_by_key used repr() for key equality ─────────────────────────

def test_pyb1_group_by_key_tuple_keys(ctx):
    """Tuple keys with identical values must group together."""
    data = [((1, 2), "a"), ((1, 2), "b"), ((2, 3), "c")]
    result = ctx.parallelize(data).group_by_key().collect()
    groups = {k: sorted(v) for k, v in result}
    assert sorted(groups[(1, 2)]) == ["a", "b"]
    assert groups[(2, 3)] == ["c"]

def test_pyb1_group_by_key_int_keys(ctx):
    data = [(1, "x"), (2, "y"), (1, "z")]
    result = dict(
        (k, sorted(v))
        for k, v in ctx.parallelize(data).group_by_key().collect()
    )
    assert result[1] == ["x", "z"]
    assert result[2] == ["y"]

def test_pyb1_group_by_key_preserves_insertion_order(ctx):
    """First-seen key must appear first in output."""
    data = [("b", 2), ("a", 1), ("b", 3)]
    result = ctx.parallelize(data).group_by_key().collect()
    keys = [k for k, _ in result]
    assert keys[0] == "b"
    assert keys[1] == "a"


# ── PY-B2: reduce_by_key used repr() for key equality ────────────────────────

def test_pyb2_reduce_by_key_string_keys(ctx):
    data = [("hello", 1), ("hello", 1), ("world", 1)]
    result = dict(ctx.parallelize(data).reduce_by_key(lambda a, b: a + b).collect())
    assert result["hello"] == 2
    assert result["world"] == 1

def test_pyb2_reduce_by_key_tuple_keys(ctx):
    data = [((1, 2), 10), ((1, 2), 5), ((3, 4), 1)]
    result = dict(ctx.parallelize(data).reduce_by_key(lambda a, b: a + b).collect())
    assert result[(1, 2)] == 15
    assert result[(3, 4)] == 1

def test_pyb2_reduce_by_key_int_keys(ctx):
    data = [(1, 100), (2, 200), (1, 50)]
    result = dict(ctx.parallelize(data).reduce_by_key(lambda a, b: a + b).collect())
    assert result[1] == 150
    assert result[2] == 200


# ── PY-B3: count_by_value returned repr-string keys, not item keys ────────────

def test_pyb3_count_by_value_int_keys(ctx):
    result = ctx.parallelize([1, 2, 1, 3, 2, 1]).count_by_value()
    assert result[1] == 3
    assert result[2] == 2
    assert result[3] == 1

def test_pyb3_count_by_value_string_keys(ctx):
    result = ctx.parallelize(["a", "b", "a"]).count_by_value()
    assert result["a"] == 2
    assert result["b"] == 1

def test_pyb3_count_by_value_tuple_keys(ctx):
    """Tuple keys must be dict keys, not repr strings like \"('a', 1)\"."""
    data = [("a", 1), ("b", 2), ("a", 1)]
    result = ctx.parallelize(data).count_by_value()
    assert result[("a", 1)] == 2
    assert result[("b", 2)] == 1

def test_pyb3_count_by_key_uses_object_keys(ctx):
    data = [("x", 10), ("y", 20), ("x", 30)]
    result = ctx.parallelize(data).count_by_key()
    assert result["x"] == 2
    assert result["y"] == 1


# ── PY-B4: map_values in distributed mode (documented, deferred) ──────────────

def test_pyb4_map_values_local_preserves_keys(ctx):
    """Local mode must preserve keys (the bug only surfaces in distributed mode)."""
    data = [("a", 1), ("b", 2), ("a", 3)]
    result = ctx.parallelize(data).map_values(lambda v: v * 10).collect()
    assert set(result) == {("a", 10), ("b", 20), ("a", 30)}

def test_pyb4_map_values_key_unchanged(ctx):
    result = ctx.parallelize([("key", 42)]).map_values(lambda v: v + 1).collect()
    assert result == [("key", 43)]


# ── PY-B5: __iter__ was declared in stub but not implemented ──────────────────

def test_pyb5_rdd_is_iterable(ctx):
    rdd = ctx.parallelize([1, 2, 3])
    result = list(rdd)
    assert sorted(result) == [1, 2, 3]

def test_pyb5_for_loop_works(ctx):
    seen = []
    for x in ctx.parallelize(["a", "b", "c"]):
        seen.append(x)
    assert sorted(seen) == ["a", "b", "c"]

def test_pyb5_iter_empty(ctx):
    assert list(ctx.parallelize([])) == []


# ── PY-B6: max/min/top/take_ordered propagate sort errors ────────────────────

def test_pyb6_max_raises_on_uncomparable_types(ctx):
    with pytest.raises(Exception):
        ctx.parallelize([1, "a", 2]).max()

def test_pyb6_min_raises_on_uncomparable_types(ctx):
    with pytest.raises(Exception):
        ctx.parallelize([1, "a", 2]).min()


# ── lookup uses Python equality (related to PY-B1 repr fix) ──────────────────

def test_lookup_tuple_key(ctx):
    data = [((1, 2), "found"), ((3, 4), "other")]
    result = ctx.parallelize(data).lookup((1, 2))
    assert result == ["found"]

def test_lookup_missing_returns_empty(ctx):
    result = ctx.parallelize([("a", 1)]).lookup("z")
    assert result == []


# ── Range negative step (bonus fix — same root cause as JS-B1) ───────────────

def test_range_negative_step(ctx):
    assert ctx.range(5, 0, step=-1).collect() == [5, 4, 3, 2, 1]

def test_range_negative_step_even(ctx):
    assert ctx.range(10, 0, step=-2).collect() == [10, 8, 6, 4, 2]

def test_range_positive_step_unchanged(ctx):
    assert ctx.range(0, 5, step=1).collect() == [0, 1, 2, 3, 4]
