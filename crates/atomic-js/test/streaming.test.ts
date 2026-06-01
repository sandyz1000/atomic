/**
 * Streaming bindings tests — Phase 4.4 (Node.js)
 *
 * Prerequisites: build the native module first with:
 *   cargo build -p atomic-js
 *   cd crates/atomic-js && npm run build
 * Then run: npm test
 */
import { describe, it, expect, beforeAll } from "vitest";

let StreamingContext: typeof import("../index.js").StreamingContext;
let moduleLoaded = false;

beforeAll(() => {
  try {
    const m = require("../index.js");
    StreamingContext = m.StreamingContext;
    moduleLoaded = true;
  } catch {
    console.warn(
      "atomic-js native module not built — skipping all streaming tests.\n" +
        "Run: cargo build -p atomic-js && npm test"
    );
  }
});

function ssc() {
  return new StreamingContext(0.1);
}

// ── foreach_rdd basic ─────────────────────────────────────────────────────────

describe("foreachRdd basic", () => {
  it("delivers pushed batch to output callback", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream, queue] = ctx.testQueueStream();
    const results: number[] = [];
    ctx.foreachRdd(stream, (batch: number[]) => results.push(...batch));
    queue.push([1, 2, 3]);
    ctx.runOneBatch();
    expect(results).toEqual([1, 2, 3]);
  });

  it("empty batch produces no output", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream, queue] = ctx.testQueueStream();
    const results: number[] = [];
    ctx.foreachRdd(stream, (b: number[]) => results.push(...b));
    queue.push([]);
    ctx.runOneBatch();
    expect(results).toEqual([]);
  });

  it("empty queue produces empty callback", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream] = ctx.testQueueStream();
    const results: number[] = [];
    ctx.foreachRdd(stream, (b: number[]) => results.push(...b));
    ctx.runOneBatch(); // nothing pushed
    expect(results).toEqual([]);
  });

  it("multiple batches are consumed one at a time", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream, queue] = ctx.testQueueStream();
    const results: number[] = [];
    ctx.foreachRdd(stream, (b: number[]) => results.push(...b));
    queue.push([1, 2]);
    queue.push([3, 4]);
    ctx.runOneBatch();
    ctx.runOneBatch();
    expect(results).toEqual([1, 2, 3, 4]);
  });
});

// ── map ───────────────────────────────────────────────────────────────────────

describe("map", () => {
  it("doubles each element", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream, queue] = ctx.testQueueStream();
    const results: number[] = [];
    ctx.foreachRdd(stream.map((x: number) => x * 2), (b: number[]) => results.push(...b));
    queue.push([1, 2, 3]);
    ctx.runOneBatch();
    expect(results).toEqual([2, 4, 6]);
  });

  it("handles empty input", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream, queue] = ctx.testQueueStream();
    const results: number[] = [];
    ctx.foreachRdd(stream.map((x: number) => x + 100), (b: number[]) => results.push(...b));
    queue.push([]);
    ctx.runOneBatch();
    expect(results).toEqual([]);
  });
});

// ── filter ────────────────────────────────────────────────────────────────────

describe("filter", () => {
  it("keeps only even numbers", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream, queue] = ctx.testQueueStream();
    const results: number[] = [];
    ctx.foreachRdd(
      stream.filter((x: number) => x % 2 === 0),
      (b: number[]) => results.push(...b)
    );
    queue.push([1, 2, 3, 4, 5]);
    ctx.runOneBatch();
    expect(results).toEqual([2, 4]);
  });
});

// ── flatMap ───────────────────────────────────────────────────────────────────

describe("flatMap", () => {
  it("splits words", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream, queue] = ctx.testQueueStream();
    const results: string[] = [];
    ctx.foreachRdd(
      stream.flatMap((x: string) => x.split(" ")),
      (b: string[]) => results.push(...b)
    );
    queue.push(["hello world", "foo bar"]);
    ctx.runOneBatch();
    expect(results.sort()).toEqual(["bar", "foo", "hello", "world"]);
  });
});

// ── chained transforms ────────────────────────────────────────────────────────

describe("chained map + filter", () => {
  it("doubles then filters > 4", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream, queue] = ctx.testQueueStream();
    const results: number[] = [];
    ctx.foreachRdd(
      stream.map((x: number) => x * 2).filter((x: number) => x > 4),
      (b: number[]) => results.push(...b)
    );
    queue.push([1, 2, 3, 4, 5]);
    ctx.runOneBatch();
    expect(results.sort((a, b) => a - b)).toEqual([6, 8, 10]);
  });
});

// ── reduceByKey ───────────────────────────────────────────────────────────────

describe("reduceByKey", () => {
  it("sums values by key", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream, queue] = ctx.testPairQueueStream();
    const results: [string, number][] = [];
    ctx.foreachRdd(
      stream.reduceByKey((a: number, b: number) => a + b),
      (b: [string, number][]) => results.push(...b)
    );
    queue.push([["a", 1], ["b", 2], ["a", 3]]);
    ctx.runOneBatch();
    const dict = Object.fromEntries(results);
    expect(dict["a"]).toBe(4);
    expect(dict["b"]).toBe(2);
  });

  it("throws on non-pair stream", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream] = ctx.testQueueStream();
    expect(() => stream.reduceByKey((a: number, b: number) => a + b)).toThrow();
  });
});

// ── groupByKey ────────────────────────────────────────────────────────────────

describe("groupByKey", () => {
  it("groups values by key", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream, queue] = ctx.testPairQueueStream();
    const results: [string, number[]][] = [];
    ctx.foreachRdd(
      stream.groupByKey(),
      (b: [string, number[]][]) => results.push(...b)
    );
    queue.push([["a", 1], ["b", 2], ["a", 3]]);
    ctx.runOneBatch();
    const dict = Object.fromEntries(results);
    expect(dict["a"].sort()).toEqual([1, 3]);
    expect(dict["b"]).toEqual([2]);
  });
});

// ── join ──────────────────────────────────────────────────────────────────────

describe("join", () => {
  it("inner joins matching keys", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [left, q1] = ctx.testPairQueueStream();
    const [right, q2] = ctx.testPairQueueStream();
    const results: any[] = [];
    ctx.foreachRdd(left.join(right), (b: any[]) => results.push(...b));
    q1.push([["a", 1], ["b", 2]]);
    q2.push([["a", 10], ["c", 30]]);
    ctx.runOneBatch();
    const dict = Object.fromEntries(results.map(([k, v]: any) => [k, v]));
    expect(dict["a"]).toEqual([1, 10]);
    expect(dict["b"]).toBeUndefined();
  });
});

// ── leftOuterJoin ─────────────────────────────────────────────────────────────

describe("leftOuterJoin", () => {
  it("includes unmatched left keys with null", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [left, q1] = ctx.testPairQueueStream();
    const [right, q2] = ctx.testPairQueueStream();
    const results: any[] = [];
    ctx.foreachRdd(left.leftOuterJoin(right), (b: any[]) => results.push(...b));
    q1.push([["a", 1], ["b", 2]]);
    q2.push([["a", 10]]);
    ctx.runOneBatch();
    const dict = Object.fromEntries(results.map(([k, v]: any) => [k, v]));
    expect(dict["a"]).toEqual([1, 10]);
    expect(dict["b"][0]).toBe(2);
    expect(dict["b"][1]).toBeNull();
  });
});

// ── mapValues ─────────────────────────────────────────────────────────────────

describe("mapValues", () => {
  it("applies function to values only", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream, queue] = ctx.testPairQueueStream();
    const results: any[] = [];
    ctx.foreachRdd(
      stream.mapValues((v: number) => v * 10),
      (b: any[]) => results.push(...b)
    );
    queue.push([["a", 1], ["b", 2]]);
    ctx.runOneBatch();
    const dict = Object.fromEntries(results.map(([k, v]: any) => [k, v]));
    expect(dict["a"]).toBe(10);
    expect(dict["b"]).toBe(20);
  });
});

// ── updateStateByKey ──────────────────────────────────────────────────────────

describe("updateStateByKey", () => {
  it("accumulates state across batches", () => {
    if (!moduleLoaded) return;
    const ctx = ssc();
    const [stream, queue] = ctx.testPairQueueStream();
    const resultsBatch: Record<string, number>[] = [];

    ctx.foreachRdd(
      stream.updateStateByKey(
        (newValues: number[], oldState: number | null) =>
          (oldState ?? 0) + newValues.reduce((a, b) => a + b, 0)
      ),
      (b: any[]) =>
        resultsBatch.push(Object.fromEntries(b.map(([k, v]: any) => [k, v])))
    );

    // batch 1
    queue.push([["a", 1], ["b", 2]]);
    ctx.runOneBatch();

    // batch 2 — accumulates
    queue.push([["a", 10]]);
    ctx.runOneBatch();

    // After batch 1: a=1, b=2
    expect(resultsBatch[0]["a"]).toBe(1);
    // After batch 2: a=11 (1+10), b=2 (persisted from state)
    expect(resultsBatch[1]["a"]).toBe(11);
  });
});
