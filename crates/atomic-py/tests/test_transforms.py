"""Local-mode coverage for every RDD transform and action (PY-G2)."""
import os
import pytest
import atomic


# ── Basic transforms ──────────────────────────────────────────────────────────

def test_map_doubles(ctx):
    result = ctx.parallelize([1, 2, 3]).map(lambda x: x * 2).collect()
    assert result == [2, 4, 6]

def test_map_empty(ctx):
    result = ctx.parallelize([]).map(lambda x: x * 2).collect()
    assert result == []

def test_map_single(ctx):
    result = ctx.parallelize([42]).map(lambda x: x + 1).collect()
    assert result == [43]

def test_map_multi_partition(ctx):
    rdd = ctx.parallelize(list(range(6)), num_partitions=3)
    result = rdd.map(lambda x: x * 2).collect()
    assert sorted(result) == [0, 2, 4, 6, 8, 10]

def test_filter_keeps_evens(ctx):
    result = ctx.parallelize([1, 2, 3, 4, 5]).filter(lambda x: x % 2 == 0).collect()
    assert result == [2, 4]

def test_filter_all_removed(ctx):
    result = ctx.parallelize([1, 3, 5]).filter(lambda x: x % 2 == 0).collect()
    assert result == []

def test_flat_map_tokenize(ctx):
    result = ctx.parallelize(["a b", "c"]).flat_map(lambda s: s.split()).collect()
    assert sorted(result) == ["a", "b", "c"]

def test_flat_map_empty_inner(ctx):
    result = ctx.parallelize(["", "x"]).flat_map(lambda s: s.split()).collect()
    assert result == ["x"]

def test_map_values_doubles_value(ctx):
    data = [("a", 1), ("b", 2)]
    result = ctx.parallelize(data).map_values(lambda v: v * 10).collect()
    assert dict(result) == {"a": 10, "b": 20}

def test_map_values_preserves_key(ctx):
    result = ctx.parallelize([("x", 5)]).map_values(lambda v: v + 1).collect()
    assert result == [("x", 6)]

def test_flat_map_values(ctx):
    data = [("a", [1, 2]), ("b", [3])]
    result = ctx.parallelize(data).flat_map_values(lambda v: v).collect()
    assert sorted(result) == [("a", 1), ("a", 2), ("b", 3)]

def test_key_by(ctx):
    result = ctx.parallelize([1, 2, 3]).key_by(lambda x: x % 2).collect()
    assert sorted(result) == [(0, 2), (1, 1), (1, 3)]

def test_group_by(ctx):
    result = ctx.parallelize([1, 2, 3, 4]).group_by(lambda x: x % 2).collect()
    groups = {k: sorted(v) for k, v in result}
    assert groups[0] == [2, 4]
    assert groups[1] == [1, 3]


# ── Set operations ────────────────────────────────────────────────────────────

def test_distinct(ctx):
    result = sorted(ctx.parallelize([1, 2, 1, 3, 2]).distinct().collect())
    assert result == [1, 2, 3]

def test_distinct_empty(ctx):
    assert ctx.parallelize([]).distinct().collect() == []

def test_subtract(ctx):
    a = ctx.parallelize([1, 2, 3, 4])
    b = ctx.parallelize([2, 4])
    assert sorted(a.subtract(b).collect()) == [1, 3]

def test_intersection(ctx):
    a = ctx.parallelize([1, 2, 3])
    b = ctx.parallelize([2, 3, 4])
    assert sorted(a.intersection(b).collect()) == [2, 3]

def test_union(ctx):
    a = ctx.parallelize([1, 2])
    b = ctx.parallelize([3, 4])
    assert sorted(a.union(b).collect()) == [1, 2, 3, 4]


# ── Grouping ──────────────────────────────────────────────────────────────────

def test_group_by_key_basic(ctx):
    data = [("a", 1), ("b", 2), ("a", 3)]
    result = ctx.parallelize(data).group_by_key().collect()
    groups = {k: sorted(v) for k, v in result}
    assert groups["a"] == [1, 3]
    assert groups["b"] == [2]

def test_reduce_by_key_sum(ctx):
    data = [("a", 1), ("b", 1), ("a", 2)]
    result = dict(ctx.parallelize(data).reduce_by_key(lambda a, b: a + b).collect())
    assert result == {"a": 3, "b": 1}

def test_reduce_by_key_single_key(ctx):
    data = [("x", 1), ("x", 2), ("x", 3)]
    result = dict(ctx.parallelize(data).reduce_by_key(lambda a, b: a + b).collect())
    assert result == {"x": 6}


# ── Joins ─────────────────────────────────────────────────────────────────────

def test_zip(ctx):
    a = ctx.parallelize([1, 2, 3])
    b = ctx.parallelize(["x", "y", "z"])
    assert ctx.parallelize([1, 2, 3]).zip(b).collect() == [(1, "x"), (2, "y"), (3, "z")]

def test_zip_unequal_raises(ctx):
    with pytest.raises(Exception):
        ctx.parallelize([1, 2]).zip(ctx.parallelize([1])).collect()

def test_cartesian(ctx):
    a = ctx.parallelize([1, 2])
    b = ctx.parallelize(["a", "b"])
    result = sorted(a.cartesian(b).collect())
    assert result == [(1, "a"), (1, "b"), (2, "a"), (2, "b")]


# ── Partition ops ─────────────────────────────────────────────────────────────

def test_coalesce(ctx):
    rdd = ctx.parallelize(list(range(8)), num_partitions=4).coalesce(2)
    assert rdd.num_partitions() == 2
    assert sorted(rdd.collect()) == list(range(8))

def test_repartition(ctx):
    rdd = ctx.parallelize([1, 2, 3]).repartition(3)
    assert rdd.num_partitions() == 3

def test_map_partitions(ctx):
    rdd = ctx.parallelize([1, 2, 3, 4], num_partitions=2)
    result = rdd.map_partitions(lambda p: [sum(p)]).collect()
    assert sum(result) == 10


# ── Key-value ops ─────────────────────────────────────────────────────────────

def test_keys(ctx):
    result = sorted(ctx.parallelize([("a", 1), ("b", 2)]).keys().collect())
    assert result == ["a", "b"]

def test_values(ctx):
    result = sorted(ctx.parallelize([("a", 1), ("b", 2)]).values().collect())
    assert result == [1, 2]

def test_lookup(ctx):
    data = [("a", 1), ("b", 2), ("a", 3)]
    result = sorted(ctx.parallelize(data).lookup("a"))
    assert result == [1, 3]

def test_lookup_missing_key(ctx):
    result = ctx.parallelize([("a", 1)]).lookup("z")
    assert result == []

def test_count_by_key(ctx):
    data = [("a", 1), ("b", 1), ("a", 2)]
    result = ctx.parallelize(data).count_by_key()
    assert result["a"] == 2
    assert result["b"] == 1


# ── Actions ───────────────────────────────────────────────────────────────────

def test_collect(ctx):
    assert ctx.parallelize([1, 2, 3]).collect() == [1, 2, 3]

def test_count(ctx):
    assert ctx.parallelize([1, 2, 3]).count() == 3

def test_count_empty(ctx):
    assert ctx.parallelize([]).count() == 0

def test_first(ctx):
    assert ctx.parallelize([10, 20]).first() == 10

def test_first_empty_raises(ctx):
    with pytest.raises(Exception):
        ctx.parallelize([]).first()

def test_take(ctx):
    assert ctx.parallelize([1, 2, 3, 4]).take(2) == [1, 2]

def test_take_more_than_available(ctx):
    assert ctx.parallelize([1, 2]).take(10) == [1, 2]

def test_reduce(ctx):
    assert ctx.parallelize([1, 2, 3, 4]).reduce(lambda a, b: a + b) == 10

def test_reduce_empty_raises(ctx):
    with pytest.raises(Exception):
        ctx.parallelize([]).reduce(lambda a, b: a + b)

def test_fold(ctx):
    assert ctx.parallelize([1, 2, 3, 4]).fold(0, lambda a, b: a + b) == 10

def test_fold_with_zero(ctx):
    assert ctx.parallelize([]).fold(99, lambda a, b: a + b) == 99

def test_for_each(ctx):
    seen = []
    ctx.parallelize([1, 2, 3]).for_each(lambda x: seen.append(x))
    assert sorted(seen) == [1, 2, 3]

def test_for_each_partition(ctx):
    sizes = []
    ctx.parallelize([1, 2, 3, 4], num_partitions=2).for_each_partition(
        lambda p: sizes.append(len(p))
    )
    assert sum(sizes) == 4


# ── Aggregation ───────────────────────────────────────────────────────────────

def test_count_by_value(ctx):
    result = ctx.parallelize([1, 2, 1, 3, 2, 1]).count_by_value()
    assert result[1] == 3
    assert result[2] == 2
    assert result[3] == 1

def test_aggregate(ctx):
    result = ctx.parallelize([1, 2, 3, 4], num_partitions=2).aggregate(
        0,
        lambda acc, x: acc + x,
        lambda a, b: a + b,
    )
    assert result == 10


# ── Ordering ──────────────────────────────────────────────────────────────────

def test_max(ctx):
    assert ctx.parallelize([3, 1, 4, 1, 5, 9]).max() == 9

def test_min(ctx):
    assert ctx.parallelize([3, 1, 4, 1, 5, 9]).min() == 1

def test_max_with_key(ctx):
    assert ctx.parallelize(["bb", "a", "ccc"]).max(key=len) == "ccc"

def test_min_with_key(ctx):
    assert ctx.parallelize(["bb", "a", "ccc"]).min(key=len) == "a"

def test_top(ctx):
    assert ctx.parallelize([3, 1, 4, 1, 5]).top(3) == [5, 4, 3]

def test_take_ordered(ctx):
    assert ctx.parallelize([3, 1, 4, 1, 5]).take_ordered(3) == [1, 1, 3]


# ── IO ────────────────────────────────────────────────────────────────────────

def test_save_as_text_file(ctx, tmp_path):
    path = str(tmp_path / "out.txt")
    ctx.parallelize(["hello", "world"]).save_as_text_file(path)
    lines = open(path).read().splitlines()
    assert sorted(lines) == ["hello", "world"]

def test_text_file(ctx, tmp_path):
    path = str(tmp_path / "input.txt")
    open(path, "w").write("line1\nline2\nline3\n")
    result = ctx.text_file(path).collect()
    assert sorted(result) == ["line1", "line2", "line3"]


# ── Meta ──────────────────────────────────────────────────────────────────────

def test_is_empty_true(ctx):
    assert ctx.parallelize([]).is_empty() is True

def test_is_empty_false(ctx):
    assert ctx.parallelize([1]).is_empty() is False

def test_default_parallelism(ctx):
    assert ctx.default_parallelism() == 2

def test_range_basic(ctx):
    assert ctx.range(0, 5).collect() == [0, 1, 2, 3, 4]

def test_range_step(ctx):
    assert ctx.range(0, 10, step=2).collect() == [0, 2, 4, 6, 8]

def test_range_negative_step(ctx):
    assert ctx.range(5, 0, step=-1).collect() == [5, 4, 3, 2, 1]

def test_range_negative_step_by_two(ctx):
    assert ctx.range(10, 0, step=-2).collect() == [10, 8, 6, 4, 2]
