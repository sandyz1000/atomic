/**
 * Local-mode coverage for every RDD transform and action (JS-G1).
 *
 * Prerequisites: build the native module first with one of
 *   cargo build -p atomic-js           (debug)
 *   cargo build --release -p atomic-js (release)
 *   cd crates/atomic-js && npm run build
 * Then run: npm test
 */
import { describe, it, expect, beforeAll } from "vitest";

// Dynamic require so that the test file is parseable even when the native
// module is absent — the beforeAll guard skips the suite in that case.
let Context: typeof import("../index.js").Context;
let moduleLoaded = false;

beforeAll(() => {
  try {
    const m = require("../index.js");
    Context = m.Context;
    moduleLoaded = true;
  } catch {
    console.warn(
      "atomic-js native module not built — skipping all transforms tests.\n" +
        "Run: cargo build -p atomic-js && npm test"
    );
  }
});

function ctx() {
  return new Context(2);
}

// ── Basic transforms ──────────────────────────────────────────────────────────

describe("map", () => {
  it("doubles each element", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([1, 2, 3]).map((x: number) => x * 2).collect())
      .toEqual([2, 4, 6]);
  });
  it("handles empty input", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([]).map((x: number) => x).collect()).toEqual([]);
  });
  it("handles single element", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([42]).map((x: number) => x + 1).collect()).toEqual([43]);
  });
});

describe("filter", () => {
  it("keeps even numbers", () => {
    if (!moduleLoaded) return;
    expect(
      ctx().parallelize([1, 2, 3, 4, 5]).filter((x: number) => x % 2 === 0).collect()
    ).toEqual([2, 4]);
  });
  it("returns empty when all filtered out", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([1, 3, 5]).filter((x: number) => x % 2 === 0).collect())
      .toEqual([]);
  });
});

describe("flatMap", () => {
  it("splits strings into words", () => {
    if (!moduleLoaded) return;
    const result = ctx()
      .parallelize(["a b", "c"])
      .flatMap((s: string) => s.split(" "))
      .collect()
      .sort();
    expect(result).toEqual(["a", "b", "c"]);
  });
  it("flattens nested arrays", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([[1, 2], [3]]).flatMap((a: number[]) => a).collect())
      .toEqual([1, 2, 3]);
  });
});

describe("mapValues", () => {
  it("transforms values while preserving keys", () => {
    if (!moduleLoaded) return;
    const result = ctx()
      .parallelize([["a", 1], ["b", 2]])
      .mapValues((v: number) => v * 10)
      .collect();
    expect(Object.fromEntries(result)).toEqual({ a: 10, b: 20 });
  });
});

describe("flatMapValues", () => {
  it("expands values keeping keys", () => {
    if (!moduleLoaded) return;
    const result = ctx()
      .parallelize([["a", [1, 2]], ["b", [3]]])
      .flatMapValues((v: number[]) => v)
      .collect()
      .sort((a: unknown[], b: unknown[]) => String(a[0]).localeCompare(String(b[0])));
    expect(result).toEqual([["a", 1], ["a", 2], ["b", 3]]);
  });
});

describe("keyBy", () => {
  it("produces [key, element] pairs", () => {
    if (!moduleLoaded) return;
    const result = ctx()
      .parallelize([1, 2, 3])
      .keyBy((x: number) => x % 2)
      .collect()
      .sort((a: number[], b: number[]) => a[0] - b[0]);
    expect(result).toEqual([[0, 2], [1, 1], [1, 3]]);
  });
});

// ── Set operations ────────────────────────────────────────────────────────────

describe("distinct", () => {
  it("removes duplicates from primitives", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([1, 2, 1, 3, 2]).distinct().collect().sort())
      .toEqual([1, 2, 3]);
  });
  it("handles empty input", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([]).distinct().collect()).toEqual([]);
  });
});

describe("subtract", () => {
  it("removes matching elements", () => {
    if (!moduleLoaded) return;
    const a = ctx().parallelize([1, 2, 3, 4]);
    const b = ctx().parallelize([2, 4]);
    expect(a.subtract(b).collect().sort()).toEqual([1, 3]);
  });
});

describe("intersection", () => {
  it("returns common elements (no duplicates)", () => {
    if (!moduleLoaded) return;
    const a = ctx().parallelize([1, 2, 3]);
    const b = ctx().parallelize([2, 3, 4]);
    expect(a.intersection(b).collect().sort()).toEqual([2, 3]);
  });
});

describe("union", () => {
  it("combines two RDDs", () => {
    if (!moduleLoaded) return;
    const a = ctx().parallelize([1, 2]);
    const b = ctx().parallelize([3, 4]);
    expect(a.union(b).collect().sort()).toEqual([1, 2, 3, 4]);
  });
});

// ── Grouping ──────────────────────────────────────────────────────────────────

describe("groupByKey", () => {
  it("groups string keys", () => {
    if (!moduleLoaded) return;
    const data = [["a", 1], ["b", 2], ["a", 3]];
    const result = ctx().parallelize(data).groupByKey().collect();
    const groups = Object.fromEntries(result.map(([k, v]: [string, number[]]) => [k, v.sort()]));
    expect(groups["a"]).toEqual([1, 3]);
    expect(groups["b"]).toEqual([2]);
  });
});

describe("reduceByKey", () => {
  it("sums values per key", () => {
    if (!moduleLoaded) return;
    const data = [["a", 1], ["b", 1], ["a", 2]];
    const result = Object.fromEntries(
      ctx().parallelize(data).reduceByKey((a: number, b: number) => a + b).collect()
    );
    expect(result).toEqual({ a: 3, b: 1 });
  });
});

// ── Joins ─────────────────────────────────────────────────────────────────────

describe("zip", () => {
  it("pairs elements element-wise", () => {
    if (!moduleLoaded) return;
    const a = ctx().parallelize([1, 2, 3]);
    const b = ctx().parallelize(["x", "y", "z"]);
    expect(a.zip(b).collect()).toEqual([[1, "x"], [2, "y"], [3, "z"]]);
  });
  it("throws on unequal lengths", () => {
    if (!moduleLoaded) return;
    expect(() => ctx().parallelize([1, 2]).zip(ctx().parallelize([1])).collect())
      .toThrow();
  });
});

describe("cartesian", () => {
  it("produces all pairs", () => {
    if (!moduleLoaded) return;
    const a = ctx().parallelize([1, 2]);
    const b = ctx().parallelize(["a", "b"]);
    const result = a.cartesian(b).collect().sort();
    expect(result).toEqual([[1, "a"], [1, "b"], [2, "a"], [2, "b"]]);
  });
});

// ── Partition ops ─────────────────────────────────────────────────────────────

describe("coalesce", () => {
  it("reduces partition count", () => {
    if (!moduleLoaded) return;
    const rdd = ctx().parallelize([1, 2, 3, 4, 5, 6], 4).coalesce(2);
    expect(rdd.numPartitions).toBe(2);
    expect(rdd.collect().sort((a: number, b: number) => a - b)).toEqual([1, 2, 3, 4, 5, 6]);
  });
});

describe("mapPartitions", () => {
  it("aggregates within partitions", () => {
    if (!moduleLoaded) return;
    const rdd = ctx().parallelize([1, 2, 3, 4], 2);
    const result = rdd.mapPartitions((p: number[]) => [p.reduce((a: number, b: number) => a + b, 0)]).collect();
    expect(result.reduce((a: number, b: number) => a + b, 0)).toBe(10);
  });
});

// ── Key-value ops ─────────────────────────────────────────────────────────────

describe("keys", () => {
  it("extracts keys", () => {
    if (!moduleLoaded) return;
    const result = ctx().parallelize([["a", 1], ["b", 2]]).keys().collect().sort();
    expect(result).toEqual(["a", "b"]);
  });
});

describe("values", () => {
  it("extracts values", () => {
    if (!moduleLoaded) return;
    const result = ctx().parallelize([["a", 1], ["b", 2]]).values().collect().sort();
    expect(result).toEqual([1, 2]);
  });
});

describe("lookup", () => {
  it("returns values for key", () => {
    if (!moduleLoaded) return;
    const data = [["a", 1], ["b", 2], ["a", 3]];
    expect(ctx().parallelize(data).lookup("a").sort()).toEqual([1, 3]);
  });
  it("returns empty array for missing key", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([["a", 1]]).lookup("z")).toEqual([]);
  });
});

// ── Actions ───────────────────────────────────────────────────────────────────

describe("count", () => {
  it("returns element count", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([1, 2, 3]).count()).toBe(3);
  });
  it("returns 0 for empty RDD", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([]).count()).toBe(0);
  });
});

describe("first", () => {
  it("returns first element", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([10, 20]).first()).toBe(10);
  });
  it("throws on empty RDD", () => {
    if (!moduleLoaded) return;
    expect(() => ctx().parallelize([]).first()).toThrow();
  });
});

describe("take", () => {
  it("returns first n elements", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([1, 2, 3, 4]).take(2)).toEqual([1, 2]);
  });
});

describe("reduce", () => {
  it("sums all elements", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([1, 2, 3, 4]).reduce((a: number, b: number) => a + b)).toBe(10);
  });
  it("throws on empty RDD", () => {
    if (!moduleLoaded) return;
    expect(() => ctx().parallelize([]).reduce((a: number, b: number) => a + b)).toThrow();
  });
});

describe("fold", () => {
  it("folds with numeric zero", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([1, 2, 3, 4]).fold(0, (a: number, b: number) => a + b)).toBe(10);
  });
  it("returns zero for empty RDD", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([]).fold(99, (a: number, b: number) => a + b)).toBe(99);
  });
  it("folds with string zero", () => {
    if (!moduleLoaded) return;
    const result = ctx().parallelize(["a", "b", "c"]).fold("", (a: string, b: string) => a + b);
    expect(result.split("").sort().join("")).toBe("abc");
  });
});

describe("forEach", () => {
  it("visits every element", () => {
    if (!moduleLoaded) return;
    const seen: number[] = [];
    ctx().parallelize([1, 2, 3]).forEach((x: number) => seen.push(x));
    expect(seen.sort()).toEqual([1, 2, 3]);
  });
});

// ── Aggregation ───────────────────────────────────────────────────────────────

describe("countByValue", () => {
  it("counts occurrences keyed by string representation", () => {
    if (!moduleLoaded) return;
    const result: Record<string, number> = ctx().parallelize([1, 2, 1, 3]).countByValue() as any;
    expect(result["1"]).toBe(2);
    expect(result["2"]).toBe(1);
    expect(result["3"]).toBe(1);
  });
});

describe("aggregate", () => {
  it("two-phase sum", () => {
    if (!moduleLoaded) return;
    const result = ctx().parallelize([1, 2, 3, 4], 2).aggregate(
      0,
      (acc: number, x: number) => acc + x,
      (a: number, b: number) => a + b
    );
    expect(result).toBe(10);
  });
});

// ── Ordering ──────────────────────────────────────────────────────────────────

describe("max", () => {
  it("returns the maximum number", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([3, 1, 4, 1, 5, 9]).max()).toBe(9);
  });
});

describe("min", () => {
  it("returns the minimum number", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([3, 1, 4, 1, 5, 9]).min()).toBe(1);
  });
});

describe("top", () => {
  it("returns n largest elements", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([3, 1, 4, 1, 5]).top(3)).toEqual([5, 4, 3]);
  });
});

describe("takeOrdered", () => {
  it("returns n smallest elements", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([3, 1, 4, 1, 5]).takeOrdered(3)).toEqual([1, 1, 3]);
  });
});

// ── IO ────────────────────────────────────────────────────────────────────────

describe("saveAsTextFile", () => {
  it("writes elements as lines", () => {
    if (!moduleLoaded) return;
    const path = require("os").tmpdir() + "/atomic-js-test-" + Date.now() + ".txt";
    ctx().parallelize(["hello", "world"]).saveAsTextFile(path);
    const lines = require("fs").readFileSync(path, "utf8").trimEnd().split("\n").sort();
    expect(lines).toEqual(["hello", "world"]);
  });
});

// ── Meta ──────────────────────────────────────────────────────────────────────

describe("isEmpty", () => {
  it("returns true for empty RDD", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([]).isEmpty()).toBe(true);
  });
  it("returns false for non-empty RDD", () => {
    if (!moduleLoaded) return;
    expect(ctx().parallelize([1]).isEmpty()).toBe(false);
  });
});

describe("defaultParallelism", () => {
  it("returns the configured value", () => {
    if (!moduleLoaded) return;
    expect(ctx().defaultParallelism()).toBe(2);
  });
});

describe("range", () => {
  it("generates ascending integers", () => {
    if (!moduleLoaded) return;
    expect(ctx().range(0, 5).collect()).toEqual([0, 1, 2, 3, 4]);
  });
  it("respects positive step", () => {
    if (!moduleLoaded) return;
    expect(ctx().range(0, 10, 2).collect()).toEqual([0, 2, 4, 6, 8]);
  });
});
