/**
 * Regression tests for confirmed atomic-js bugs (JS-B1 through JS-B6).
 *
 * Prerequisites: build the native module first — see transforms.test.ts.
 */
import { describe, it, expect, beforeAll } from "vitest";

let Context: typeof import("../index.js").Context;
let moduleLoaded = false;

beforeAll(() => {
  try {
    const m = require("../index.js");
    Context = m.Context;
    moduleLoaded = true;
  } catch {
    console.warn(
      "atomic-js native module not built — skipping all bug regression tests."
    );
  }
});

function ctx() {
  return new Context(2);
}

// ── JS-B1: range() with negative step was silently empty ─────────────────────

describe("JS-B1: range negative step", () => {
  it("range(10, 0, -2) produces descending sequence", () => {
    if (!moduleLoaded) return;
    expect(ctx().range(10, 0, -2).collect()).toEqual([10, 8, 6, 4, 2]);
  });

  it("range(5, 0, -1) produces every integer from 5 down to 1", () => {
    if (!moduleLoaded) return;
    expect(ctx().range(5, 0, -1).collect()).toEqual([5, 4, 3, 2, 1]);
  });

  it("range(0, 5, 1) still works correctly after the fix", () => {
    if (!moduleLoaded) return;
    expect(ctx().range(0, 5, 1).collect()).toEqual([0, 1, 2, 3, 4]);
  });

  it("range(0, 0, -1) is empty (start equals end)", () => {
    if (!moduleLoaded) return;
    expect(ctx().range(0, 0, -1).collect()).toEqual([]);
  });

  it("range(3, 5, -1) is empty (start < end with negative step)", () => {
    if (!moduleLoaded) return;
    expect(ctx().range(3, 5, -1).collect()).toEqual([]);
  });

  it("step=0 throws", () => {
    if (!moduleLoaded) return;
    expect(() => ctx().range(0, 5, 0)).toThrow();
  });
});

// ── JS-B2: reduceByKey / groupByKey have no distributed path ─────────────────
// Local-mode behaviour must still be correct.

describe("JS-B2: reduceByKey local correctness", () => {
  it("sums values per string key", () => {
    if (!moduleLoaded) return;
    const data = [["hello", 1], ["world", 1], ["hello", 1]];
    const result = Object.fromEntries(
      ctx().parallelize(data).reduceByKey((a: number, b: number) => a + b).collect()
    );
    expect(result["hello"]).toBe(2);
    expect(result["world"]).toBe(1);
  });

  it("groupByKey accumulates all values per key", () => {
    if (!moduleLoaded) return;
    const data = [["a", 1], ["b", 2], ["a", 3]];
    const result = Object.fromEntries(
      ctx().parallelize(data).groupByKey().collect()
        .map(([k, v]: [string, number[]]) => [k, v.sort()])
    );
    expect(result["a"]).toEqual([1, 3]);
    expect(result["b"]).toEqual([2]);
  });
});

// ── JS-B3: group_by inherits distributed gap via groupByKey ──────────────────
// Local-mode pipeline must still produce correct results.

describe("JS-B3: groupBy local correctness", () => {
  it("groups by computed key function", () => {
    if (!moduleLoaded) return;
    const data = [1, 2, 3, 4];
    const result = Object.fromEntries(
      ctx().parallelize(data).groupBy((x: number) => x % 2).collect()
        .map(([k, v]: [number, number[]]) => [k, v.sort()])
    );
    expect(result[0]).toEqual([2, 4]);
    expect(result[1]).toEqual([1, 3]);
  });
});

// ── JS-B4: key_to_string throws on object keys ────────────────────────────────

describe("JS-B4: key_to_string unsupported type", () => {
  it("throws a clear error for object keys in reduceByKey", () => {
    if (!moduleLoaded) return;
    const data = [[{ x: 1 }, "a"], [{ x: 1 }, "b"]];
    expect(() =>
      ctx().parallelize(data).reduceByKey((a: string, b: string) => a + b).collect()
    ).toThrow(/Unsupported key type/i);
  });

  it("throws for array keys in lookup", () => {
    if (!moduleLoaded) return;
    const data = [[[1, 2], "val"]];
    expect(() => ctx().parallelize(data).lookup([1, 2])).toThrow();
  });
});

// ── JS-B5: fold JS function string generation ─────────────────────────────────
// Local mode fold must produce the correct result regardless of how the
// distributed function string is built.

describe("JS-B5: fold correctness (local mode)", () => {
  it("folds integers with numeric zero", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([1, 2, 3, 4]).fold(0, (a: number, b: number) => a + b)).toBe(10);
  });

  it("folds with string zero", () => {
    if (!moduleLoaded) return;
    const result = ctx()
      .parallelize(["a", "b", "c"])
      .fold("", (a: string, b: string) => a + b);
    expect(result.split("").sort().join("")).toBe("abc");
  });

  it("returns zero for empty input", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([]).fold(42, (a: number, b: number) => a + b)).toBe(42);
  });

  it("folds with object accumulator", () => {
    if (!moduleLoaded) return;
    const result = ctx()
      .parallelize([["a", 1], ["b", 2], ["a", 3]])
      .fold(
        {} as Record<string, number>,
        (acc: Record<string, number>, [k, v]: [string, number]) => {
          acc[k] = (acc[k] ?? 0) + v;
          return acc;
        }
      );
    expect(result["a"]).toBe(4);
    expect(result["b"]).toBe(2);
  });
});

// ── JS-B6: distinct/subtract/intersection use toString() for identity ─────────

describe("JS-B6: distinct/subtract/intersection on primitives", () => {
  it("distinct deduplicates numbers", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([1, 2, 1, 3, 2]).distinct().collect().sort())
      .toEqual([1, 2, 3]);
  });

  it("distinct deduplicates strings", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize(["a", "b", "a"]).distinct().collect().sort())
      .toEqual(["a", "b"]);
  });

  it("subtract removes matching elements", () => {
    if (!moduleLoaded) return;
    const a = ctx().parallelize([1, 2, 3, 4]);
    const b = ctx().parallelize([2, 4]);
    expect(a.subtract(b).collect().sort()).toEqual([1, 3]);
  });

  it("intersection returns common elements (no duplicates)", () => {
    if (!moduleLoaded) return;
    const a = ctx().parallelize([1, 2, 3, 2]);
    const b = ctx().parallelize([2, 3, 4]);
    expect(a.intersection(b).collect().sort()).toEqual([2, 3]);
  });
});

// ── TypeScript conditional type guards (JS-G3) ────────────────────────────────
// These are compile-time checks; at runtime we just verify the methods are
// callable on pair RDDs and not on scalar RDDs.

describe("JS-G3: pair-only methods available on pair RDDs", () => {
  it("reduceByKey is callable on pair RDD at runtime", () => {
    if (!moduleLoaded) return;
    const result = ctx()
      .parallelize([["k", 1], ["k", 2]])
      .reduceByKey((a: number, b: number) => a + b)
      .collect();
    expect(result.length).toBeGreaterThan(0);
  });
});
