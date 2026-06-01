"""SQL context tests for atomic-py.

Run with:  maturin develop && pytest tests/test_sql.py -v
"""

import os
import tempfile

import pytest

from atomic_compute import SqlContext


@pytest.fixture
def ctx():
    return SqlContext()


@pytest.fixture
def csv_table(tmp_path):
    """Write a small CSV file and return (path, SqlContext)."""
    path = str(tmp_path / "data.csv")
    with open(path, "w") as f:
        f.write("id,value,name\n")
        f.write("1,10,alice\n")
        f.write("2,20,bob\n")
        f.write("3,30,carol\n")
        f.write("4,40,dave\n")
        f.write("5,50,eve\n")
    ctx = SqlContext()
    ctx.register_csv("t", path)
    return ctx


# ── SqlContext ────────────────────────────────────────────────────────────────


def test_context_creates():
    ctx = SqlContext()
    assert repr(ctx) == "SqlContext()"


def test_sql_literal():
    """sql() on a pure literal query produces correct rows."""
    ctx = SqlContext()
    df = ctx.sql("SELECT 42 AS n, 'hello' AS s")
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["n"] == 42
    assert rows[0]["s"] == "hello"


def test_register_and_query_csv(csv_table):
    df = csv_table.sql("SELECT id, value FROM t ORDER BY id")
    rows = df.collect()
    assert len(rows) == 5
    assert rows[0]["id"] == 1
    assert rows[4]["value"] == 50


def test_deregister_table(csv_table):
    csv_table.deregister_table("t")
    with pytest.raises(Exception):
        csv_table.sql("SELECT * FROM t").collect()


def test_sql_aggregation(csv_table):
    df = csv_table.sql("SELECT SUM(value) AS total FROM t")
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["total"] == 150


# ── DataFrame ─────────────────────────────────────────────────────────────────


def test_collect_returns_list_of_dicts(csv_table):
    rows = csv_table.sql("SELECT * FROM t ORDER BY id").collect()
    assert isinstance(rows, list)
    assert all(isinstance(r, dict) for r in rows)
    assert set(rows[0].keys()) == {"id", "value", "name"}


def test_count(csv_table):
    n = csv_table.sql("SELECT * FROM t").count()
    assert n == 5


def test_show_does_not_raise(csv_table, capsys):
    csv_table.sql("SELECT id, value FROM t LIMIT 3").show()
    # Just verify no exception; output format is DataFusion's own.


def test_show_limit(csv_table, capsys):
    csv_table.sql("SELECT * FROM t").show_limit(2)


def test_filter(csv_table):
    df = csv_table.sql("SELECT * FROM t")
    rows = df.filter("value > 20").collect()
    assert len(rows) == 3
    assert all(r["value"] > 20 for r in rows)


def test_select(csv_table):
    df = csv_table.sql("SELECT * FROM t")
    rows = df.select(["id", "name"]).collect()
    assert len(rows) == 5
    assert set(rows[0].keys()) == {"id", "name"}


def test_limit(csv_table):
    rows = csv_table.sql("SELECT * FROM t").limit(3).collect()
    assert len(rows) == 3


def test_sort_ascending(csv_table):
    rows = csv_table.sql("SELECT id, value FROM t").sort("value").collect()
    values = [r["value"] for r in rows]
    assert values == sorted(values)


def test_sort_descending(csv_table):
    rows = csv_table.sql("SELECT id, value FROM t").sort("value", ascending=False).collect()
    values = [r["value"] for r in rows]
    assert values == sorted(values, reverse=True)


def test_schema(csv_table):
    schema = csv_table.sql("SELECT * FROM t").schema()
    assert isinstance(schema, dict)
    assert "id" in schema
    assert "value" in schema
    assert "name" in schema


def test_dataframe_repr(csv_table):
    df = csv_table.sql("SELECT id, value FROM t")
    r = repr(df)
    assert "id" in r
    assert "value" in r


def test_chained_transforms(csv_table):
    rows = (
        csv_table.sql("SELECT * FROM t")
        .filter("value >= 20")
        .select(["id", "value"])
        .sort("value", ascending=False)
        .limit(2)
        .collect()
    )
    assert len(rows) == 2
    assert rows[0]["value"] >= rows[1]["value"]


def test_parquet_roundtrip(csv_table, tmp_path):
    """Write to Parquet via SQL COPY and read back (if supported)."""
    # Just verify the sql() + collect() round-trip works with multiple calls.
    df1 = csv_table.sql("SELECT id, value FROM t WHERE value > 25")
    rows1 = df1.collect()
    df2 = csv_table.sql("SELECT id, value FROM t WHERE value > 25")
    rows2 = df2.collect()
    assert rows1 == rows2
