# Test Coverage Report: atomic-py and atomic-js

**Date:** 2026-05-22  
**Scope:** `crates/atomic-py` and `crates/atomic-js`  
**Status:** Zero test coverage — neither crate has any test files or test infrastructure.

---

## Executive Summary

Both `atomic-py` and `atomic-js` have **no tests whatsoever** — no test files, no CI test steps,
no test framework configured. This is a critical gap: the Python and JavaScript bindings are the
primary user-facing API for prototyping and PoC workflows, yet every method, edge case, and
distributed-mode code path is entirely unverified.

Beyond missing coverage, source-code review found **9 confirmed bugs** (incorrect behavior
regardless of input) and **6 incomplete implementations** (methods that silently fall through
to wrong behavior in distributed mode or with non-trivial input).

---

## atomic-py (`crates/atomic-py`)

### Infrastructure Gap

| Item | Status |
|---|---|
| Test files | None |
| pytest configured | No — `pyproject.toml` exists but no test section |
| Cargo unit tests | Impossible — `test = false` in Cargo.toml (cdylib link requirement) |
| How to test | `maturin develop && pytest` |

To add tests:
1. Add `pytest` and `pytest-asyncio` to `pyproject.toml` dev dependencies.
2. Create `crates/atomic-py/tests/` directory with `conftest.py` and test modules.
3. Add `maturin develop && pytest` to CI.

---

### Bug PY-B1: `group_by_key` uses `repr()` for key equality

**File:** `crates/atomic-py/src/rdd.rs` (the `group_by_key` method)  
**Severity:** High — silently produces wrong results for any non-primitive key.

The implementation builds groups with `key_obj.bind(py).repr()?.to_string()` as the HashMap key.
This means two logically equal objects that have different repr strings (e.g., different attribute
ordering in a dataclass, a custom `__repr__`) are treated as distinct keys. Conversely, two
different objects with the same repr are merged.

**Expected behavior:** Group keys should use Python's `==` operator (via PyO3's `eq()` method
or by calling `hash()` + `eq()`).

**Test to add:**
```python
def test_group_by_key_uses_python_equality():
    ctx = Context()
    data = [((1, 2), "a"), ((1, 2), "b"), ((2, 3), "c")]
    result = ctx.parallelize(data).group_by_key().collect()
    groups = dict(result)
    assert sorted(groups[(1, 2)]) == ["a", "b"]
    assert groups[(2, 3)] == ["c"]
```

---

### Bug PY-B2: `reduce_by_key` uses `repr()` for key equality

**File:** `crates/atomic-py/src/rdd.rs` (the `reduce_by_key` method)  
**Severity:** High — same root cause as PY-B1.

**Test to add:**
```python
def test_reduce_by_key_uses_python_equality():
    ctx = Context()
    data = [("hello", 1), ("hello", 1), ("world", 1)]
    result = dict(ctx.parallelize(data).reduce_by_key(lambda a, b: a + b).collect())
    assert result["hello"] == 2
    assert result["world"] == 1
```

---

### Bug PY-B3: `count_by_value` uses `repr()` as the dict key

**File:** `crates/atomic-py/src/rdd.rs` (the `count_by_value` method)  
**Severity:** Medium — returns a dict keyed by `repr(item)` strings, not by the items themselves.
Users expect `result[("a", 1)]` to work; they get `result["('a', 1)"]` instead.

**Test to add:**
```python
def test_count_by_value_keys_are_items():
    ctx = Context()
    result = ctx.parallelize([1, 2, 1, 3, 2, 1]).count_by_value()
    assert result[1] == 3
    assert result[2] == 2
    assert result[3] == 1
```

---

### Bug PY-B4: `map_values` in distributed mode dispatches `TaskAction::Map` instead of `MapValues`

**File:** `crates/atomic-py/src/rdd.rs` (the `map_values` distributed branch)  
**Severity:** High — in distributed mode, the key is dropped; workers receive only the value and
apply the task function, returning an ungrouped sequence rather than `(key, new_value)` pairs. The result
is unrecoverable.

**Test to add:**
```python
# Requires a distributed Context (two-worker setup)
def test_map_values_preserves_keys_distributed():
    ctx = Context(...)  # distributed
    data = [("a", 1), ("b", 2), ("a", 3)]
    result = ctx.parallelize(data).map_values(lambda v: v * 10).collect()
    assert set(result) == {("a", 10), ("b", 20), ("a", 30)}
```

---

### Bug PY-B5: `__iter__` declared in type stub but not implemented in Rust

**File:** `crates/atomic-py/python/atomic/__init__.pyi`  
**Severity:** Medium — the stub declares `def __iter__(self) -> Iterator[Any]: ...` on `Rdd`.
There is no `#[pymethods]` block implementing `__iter__` in `crates/atomic-py/src/rdd.rs`.
Calling `for x in rdd:` raises `TypeError` at runtime.

**Test to add:**
```python
def test_rdd_is_iterable():
    ctx = Context()
    result = list(ctx.parallelize([1, 2, 3]))
    assert result == [1, 2, 3]
```

Either implement `__iter__` (returning `iter(self.collect())`) or remove the declaration from the stub.

---

### Bug PY-B6: `max`/`min`/`top`/`take_ordered` swallow sort errors silently

**File:** `crates/atomic-py/src/rdd.rs` (sorting methods)  
**Severity:** Low — sort comparison errors are stored in a closure-captured `sort_error: Option<PyErr>`
variable and only raised after the sort completes (or doesn't). For a heterogeneous list where
Python raises `TypeError` on comparison, the method may return a partial or wrong result rather
than propagating the error.

**Test to add:**
```python
import pytest

def test_max_raises_on_uncomparable_elements():
    ctx = Context()
    with pytest.raises(Exception):
        ctx.parallelize([1, "a", 2]).max()
```

---

### Missing Type Stubs (PY-G1)

The following methods are implemented in `crates/atomic-py/src/rdd.rs` but **absent from**
`crates/atomic-py/python/atomic/__init__.pyi`:

| Method | Notes |
|---|---|
| `distinct()` | No overload or return type |
| `subtract(other)` | Completely missing |
| `intersection(other)` | Completely missing |
| `map_partitions(f)` | Completely missing |
| `keys()` | Completely missing |
| `values()` | Completely missing |
| `lookup(key)` | Completely missing |
| `for_each(f)` | Completely missing |
| `for_each_partition(f)` | Completely missing |
| `count_by_value()` | Completely missing |
| `count_by_key()` | Completely missing |
| `is_empty()` | Completely missing |
| `max(key=None)` | Completely missing |
| `min(key=None)` | Completely missing |
| `top(n, key=None)` | Completely missing |
| `take_ordered(n, key=None)` | Completely missing |
| `save_as_text_file(path)` | Completely missing |
| `aggregate(zero, seq_op, comb_op)` | Completely missing |

All these methods work at runtime but IDEs and type checkers report them as non-existent.

---

### Missing Test Coverage — Core Local Mode (PY-G2)

No tests exist for any of the following in local mode:

| Area | Missing tests |
|---|---|
| Basic transforms | `map`, `filter`, `flat_map`, `map_values`, `flat_map_values`, `key_by` |
| Set operations | `distinct`, `subtract`, `intersection`, `union` |
| Grouping | `group_by`, `group_by_key`, `reduce_by_key` |
| Joins | `zip`, `cartesian` |
| Partition ops | `coalesce`, `repartition`, `map_partitions` |
| Key-value ops | `keys`, `values`, `lookup`, `count_by_key` |
| Actions | `collect`, `count`, `first`, `take`, `reduce`, `fold`, `for_each`, `for_each_partition` |
| Aggregation | `count_by_value`, `aggregate` |
| Ordering | `max`, `min`, `top`, `take_ordered` |
| IO | `save_as_text_file`, `text_file` |
| Meta | `is_empty`, `default_parallelism`, `range` |

---

### Missing Test Coverage — Distributed Mode (PY-G3)

No distributed-mode tests exist at all. Critical paths to cover:

| Path | Concern |
|---|---|
| `collect` with staged pipeline | Dispatches to workers; aggregates results correctly |
| `map` in distributed mode | `stage_python_task` + pickle round-trip |
| `filter` in distributed mode | Python predicate serialization |
| `reduce_by_key` in distributed mode | Shuffle + repr-based key bug surfaces here |
| Multi-stage pipeline (map → filter → collect) | Op chain serialization |
| Worker-side Python task deserialization | `pickle.loads` + call on worker |

---

## atomic-js (`crates/atomic-js`)

### Infrastructure Gap

| Item | Status |
|---|---|
| Test files | None |
| Jest/Vitest/Mocha configured | No |
| `package.json` test script | Missing — only `build`, `build:debug`, `type-check` |
| `devDependencies` | No test framework listed |
| TypeScript declarations | Present (`index.d.ts`) and comprehensive |

To add tests:
1. Add `vitest` (or `jest` + `@types/jest`) to `package.json` devDependencies.
2. Create `crates/atomic-js/test/` directory.
3. Add `"test": "vitest"` to `package.json` scripts.
4. Build native module first: `cargo build -p atomic-js && node -e "require('./index.js')"`.

---

### Bug JS-B1: `range()` with negative step is silently empty

**File:** `crates/atomic-js/src/rdd.rs` (the `range` method)  
**Severity:** High — `range(10, 0, -1)` should produce `[10, 9, 8, ..., 1]` (like Python's
`range(10, 0, -1)` or Spark's `sc.range(10, 0, -1)`). Instead, the implementation does:

```rust
(start..end).step_by(step.unsigned_abs())
```

A Rust range `10..0` is immediately empty (ascending range with start > end). The negative step
is converted to `1` via `unsigned_abs` but the range never iterates.

**Fix:** Check if `step < 0` and use `(end..start).rev().step_by(step.unsigned_abs())`.

**Test to add:**
```typescript
test("range with negative step produces descending sequence", () => {
    const ctx = new Context();
    const result = ctx.range(10, 0, -2).collect();
    expect(result).toEqual([10, 8, 6, 4, 2]);
});

test("range with negative step from positive to zero", () => {
    const ctx = new Context();
    const result = ctx.range(5, 0, -1).collect();
    expect(result).toEqual([5, 4, 3, 2, 1]);
});
```

---

### Bug JS-B2: `reduce_by_key` and `group_by_key` have no distributed path

**File:** `crates/atomic-js/src/rdd.rs`  
**Severity:** High — every other distributed-mode transform (`map`, `filter`, `flat_map`, `fold`,
`reduce`) checks `self.is_distributed()` and calls `stage_js_task()`. `reduce_by_key` and
`group_by_key` do not. They always run on the driver using local data regardless of the context
mode. This silently returns wrong results (misses data on workers) in distributed deployments.

**Test to add:**
```typescript
// In a distributed context:
test("reduce_by_key dispatches to workers in distributed mode", async () => {
    const ctx = new Context({ workers: ["127.0.0.1:19300"] });
    const data = [["hello", 1], ["world", 1], ["hello", 1]];
    const result = ctx.parallelize(data).reduceByKey((a, b) => a + b).collect();
    const map = Object.fromEntries(result);
    expect(map["hello"]).toBe(2);
    expect(map["world"]).toBe(1);
});
```

---

### Bug JS-B3: `group_by()` inherits distributed-mode gap via `group_by_key()`

**File:** `crates/atomic-js/src/rdd.rs` (the `group_by` method)  
**Severity:** Medium — `group_by(f)` is implemented as `key_by(f).group_by_key()`. Since `key_by`
has a distributed path but `group_by_key` does not, the staging done by `key_by` is discarded when
`group_by_key` materializes locally. The pipeline silently collapses to driver-only execution.

---

### Bug JS-B4: `key_to_string` throws on object keys — undocumented restriction

**File:** `crates/atomic-js/src/rdd.rs` (the `key_to_string` helper)  
**Severity:** Medium — `reduce_by_key`, `group_by_key`, `count_by_key`, and `lookup` all rely on
`key_to_string()` which only handles string, number, and boolean. For any object key (e.g.,
`[1, 2]` or `{x: 1}`) it throws `"Unsupported key type"` at runtime. This restriction is not
documented in `index.d.ts`.

**Fix options:**
1. Use `JSON.stringify()` as fallback for object keys.
2. Document the restriction in TypeScript types via overloads or JSDoc.

**Test to add:**
```typescript
test("reduce_by_key throws a clear error on object keys", () => {
    const ctx = new Context();
    const data = [[{x: 1}, "a"], [{x: 1}, "b"]];
    expect(() => ctx.parallelize(data).reduceByKey((a, b) => a + b).collect())
        .toThrow(/Unsupported key type/);
});
```

---

### Bug JS-B5: `fold` distributed path generates syntactically broken JavaScript

**File:** `crates/atomic-js/src/rdd.rs` (the `fold` method, distributed branch)  
**Severity:** High — the distributed fold constructs a JS function string like:

```
"(partition) => [partition.reduce(({}), {})]"
```

where the `{}` placeholders are filled with the serialized `zero` value and `fn_source`. A
`zero` value of `0` produces `partition.reduce((0), fn)` which is a syntax error in JS
(`(0)` is not a valid expression in this position — it needs to be just `0`). Additionally,
`partition.reduce()` returns a scalar, not an array, so wrapping it in `[...]` produces a
one-element array where a scalar is expected.

**Test to add:**
```typescript
test("fold with numeric zero works correctly", () => {
    const ctx = new Context();
    const result = ctx.parallelize([1, 2, 3, 4]).fold(0, (a, b) => a + b);
    expect(result).toBe(10);
});

test("fold with string zero works correctly", () => {
    const ctx = new Context();
    const result = ctx.parallelize(["a", "b", "c"]).fold("", (a, b) => a + b);
    expect(result).toMatch(/abc/);  // order may vary across partitions
});
```

---

### Bug JS-B6: `distinct`/`subtract`/`intersection` use `toString()` for identity

**File:** `crates/atomic-js/src/rdd.rs`  
**Severity:** Medium — object identity uses `e.to_string()` (Rust's Display for `serde_json::Value`).
For JSON objects, `serde_json::Value` serializes keys in insertion order, so `{a:1, b:2}` and
`{b:2, a:1}` are treated as distinct. Arrays are compared element-wise by their JSON serialization,
which is correct. Numbers and strings work correctly. The behavior for objects should be documented.

**Test to add:**
```typescript
test("distinct deduplicates primitive values", () => {
    const ctx = new Context();
    expect(ctx.parallelize([1, 2, 1, 3, 2]).distinct().collect().sort())
        .toEqual([1, 2, 3]);
});

test("subtract removes matching elements", () => {
    const ctx = new Context();
    const a = ctx.parallelize([1, 2, 3, 4]);
    const b = ctx.parallelize([2, 4]);
    expect(a.subtract(b).collect().sort()).toEqual([1, 3]);
});
```

---

### Missing Test Coverage — Core Local Mode (JS-G1)

No tests exist for any of the following:

| Area | Missing tests |
|---|---|
| Basic transforms | `map`, `filter`, `flatMap`, `mapValues`, `flatMapValues`, `keyBy` |
| Set operations | `distinct`, `subtract`, `intersection`, `union` |
| Grouping | `groupBy`, `groupByKey`, `reduceByKey` |
| Joins | `zip`, `cartesian` |
| Partition ops | `coalesce`, `repartition`, `mapPartitions` |
| Key-value ops | `keys`, `values`, `lookup`, `countByKey` |
| Actions | `collect`, `count`, `first`, `take`, `reduce`, `fold`, `forEach`, `forEachPartition` |
| Aggregation | `countByValue`, `aggregate` |
| Ordering | `max`, `min`, `top`, `takeOrdered` |
| IO | `saveAsTextFile`, `textFile` |
| Meta | `isEmpty`, `defaultParallelism`, `range` |

---

### Missing Test Coverage — Distributed Mode (JS-G2)

No distributed-mode tests exist. Critical paths to cover:

| Path | Concern |
|---|---|
| `collect` with staged pipeline | Full JS task dispatch round-trip |
| `map` in distributed mode | `fn.toString()` serialization + V8 eval on worker |
| `filter` in distributed mode | Boolean task over wire |
| `fold` in distributed mode | Broken JS string generation (JS-B5) |
| Multi-stage pipeline | Op chain serialization ordering |
| `reduce_by_key` in distributed mode | Currently has no distributed path (JS-B2) |

---

### Missing Test Coverage — TypeScript Types (JS-G3)

The `index.d.ts` file declares conditional `this` types for pair operations (e.g.,
`reduceByKey` only available on `RDD<[K, V]>`). These constraints are not tested:

```typescript
test("type error: reduceByKey on non-pair RDD should not compile", () => {
    // This is a compile-time check; add to a separate type-check test suite
    const ctx = new Context();
    // @ts-expect-error
    ctx.parallelize([1, 2, 3]).reduceByKey((a: number, b: number) => a + b);
});
```

---

## Prioritized Work Queue

### P0 — Fix confirmed bugs (broken behavior)

| ID | Crate | Bug | Fix complexity |
|---|---|---|---|
| PY-B1 | atomic-py | `group_by_key` repr equality | Medium |
| PY-B2 | atomic-py | `reduce_by_key` repr equality | Medium |
| PY-B3 | atomic-py | `count_by_value` repr keys | Small |
| PY-B4 | atomic-py | `map_values` drops keys in distributed mode | Medium |
| PY-B5 | atomic-py | `__iter__` declared but not implemented | Small |
| JS-B1 | atomic-js | `range()` negative step empty | Small |
| JS-B2 | atomic-js | `reduce_by_key`/`group_by_key` no distributed path | Large |
| JS-B5 | atomic-js | `fold` broken JS function string | Medium |

### P1 — Test infrastructure setup

| Task | Crate | Notes |
|---|---|---|
| Add pytest + `maturin develop` to CI | atomic-py | Pre-requisite for all other py tests |
| Add vitest/jest to package.json | atomic-js | Pre-requisite for all other js tests |
| Create `conftest.py` with `Context` fixture | atomic-py | One fixture reused by all tests |
| Create `test/context.test.ts` skeleton | atomic-js | One Context fixture reused by all tests |

### P2 — Unit tests for local mode (all transforms and actions)

Write one test per method, covering at minimum:
- Basic happy path
- Empty input
- Single-element input
- Multi-partition behavior (at least 2 partitions)

### P3 — Type stub completion

Update `crates/atomic-py/python/atomic/__init__.pyi` to declare all 18 missing methods listed in PY-G1.

### P4 — Distributed integration tests

Add at minimum:
- One test per task type (Python pickle round-trip, JS source string round-trip)
- `reduce_by_key` end-to-end in distributed mode (after JS-B2 fix)
- Multi-stage distributed pipeline (map → filter → collect)

---

## Files to Create / Modify

| File | Action | Notes |
|---|---|---|
| `crates/atomic-py/tests/__init__.py` | Create | Empty |
| `crates/atomic-py/tests/conftest.py` | Create | `Context` fixture |
| `crates/atomic-py/tests/test_transforms.py` | Create | PY-G2 local mode coverage |
| `crates/atomic-py/tests/test_bugs.py` | Create | PY-B1 through PY-B6 regression tests |
| `crates/atomic-py/tests/test_distributed.py` | Create | PY-G3 distributed coverage |
| `crates/atomic-py/python/atomic/__init__.pyi` | Modify | Add 18 missing method stubs |
| `crates/atomic-py/src/rdd.rs` | Modify | Fix PY-B1/B2/B3/B4/B5 |
| `crates/atomic-js/test/transforms.test.ts` | Create | JS-G1 local mode coverage |
| `crates/atomic-js/test/bugs.test.ts` | Create | JS-B1 through JS-B6 regression tests |
| `crates/atomic-js/test/distributed.test.ts` | Create | JS-G2 distributed coverage |
| `crates/atomic-js/package.json` | Modify | Add vitest + test script |
| `crates/atomic-js/src/rdd.rs` | Modify | Fix JS-B1/B2/B3/B4/B5 |
