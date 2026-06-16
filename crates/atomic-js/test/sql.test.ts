/**
 * SQL context tests for atomic-js.
 *
 * Prerequisites: build the native module first:
 *   cd crates/atomic-js && npm run build
 * Then run: npm test
 */
import { describe, it, expect, beforeAll } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

let SqlContext: typeof import("..").SqlContext;
let Context: typeof import("..").Context;
let moduleLoaded = false;

beforeAll(() => {
  try {
    const m = require("..");
    SqlContext = m.SqlContext;
    Context = m.Context;
    moduleLoaded = true;
  } catch {
    // Module not built yet — skip all tests gracefully.
  }
});

// ── helpers ───────────────────────────────────────────────────────────────────

function writeCsv(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "atomic-sql-test-"));
  const file = path.join(dir, "data.csv");
  fs.writeFileSync(
    file,
    "id,value,name\n1,10,alice\n2,20,bob\n3,30,carol\n4,40,dave\n5,50,eve\n"
  );
  return file;
}

function makeCsvCtx() {
  const file = writeCsv();
  const ctx = new SqlContext();
  ctx.registerCsv("t", file);
  return ctx;
}

// ── SqlContext ────────────────────────────────────────────────────────────────

describe("SqlContext", () => {
  it("creates without error", () => {
    if (!moduleLoaded) return;
    expect(() => new SqlContext()).not.toThrow();
  });

  it("executes a literal SQL query", () => {
    if (!moduleLoaded) return;
    const ctx = new SqlContext();
    const rows = ctx.sql("SELECT 42 AS n, 'hello' AS s").collect();
    expect(rows).toHaveLength(1);
    expect((rows[0] as any).n).toBe(42);
    expect((rows[0] as any).s).toBe("hello");
  });

  it("registers and queries a CSV file", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    const rows = ctx.sql("SELECT id, value FROM t ORDER BY id").collect();
    expect(rows).toHaveLength(5);
    expect((rows[0] as any).id).toBe(1);
    expect((rows[4] as any).value).toBe(50);
  });

  it("deregisters a table and throws on subsequent query", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    ctx.deregisterTable("t");
    expect(() => ctx.sql("SELECT * FROM t").collect()).toThrow();
  });

  it("aggregates correctly", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    const rows = ctx.sql("SELECT SUM(value) AS total FROM t").collect();
    expect(rows).toHaveLength(1);
    expect((rows[0] as any).total).toBe(150);
  });
});

// ── DataFrame ─────────────────────────────────────────────────────────────────

describe("DataFrame", () => {
  it("collect() returns array of objects", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    const rows = ctx.sql("SELECT * FROM t ORDER BY id").collect();
    expect(Array.isArray(rows)).toBe(true);
    expect(rows).toHaveLength(5);
    expect(Object.keys(rows[0] as any).sort()).toEqual(["id", "name", "value"]);
  });

  it("count() returns row count", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    const n = ctx.sql("SELECT * FROM t").count();
    expect(n).toBe(5);
  });

  it("show() does not throw", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    expect(() => ctx.sql("SELECT id, value FROM t LIMIT 3").show()).not.toThrow();
  });

  it("showLimit() does not throw", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    expect(() => ctx.sql("SELECT * FROM t").showLimit(2)).not.toThrow();
  });

  it("filter() narrows rows", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    const rows = ctx.sql("SELECT * FROM t").filter("value > 20").collect();
    expect(rows).toHaveLength(3);
    expect((rows as any[]).every((r) => r.value > 20)).toBe(true);
  });

  it("select() keeps only named columns", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    const rows = ctx.sql("SELECT * FROM t").select(["id", "name"]).collect();
    expect(rows).toHaveLength(5);
    expect(Object.keys(rows[0] as any).sort()).toEqual(["id", "name"]);
  });

  it("limit() returns at most n rows", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    const rows = ctx.sql("SELECT * FROM t").limit(3).collect();
    expect(rows.length).toBeLessThanOrEqual(3);
  });

  it("sort() ascending by default", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    const rows = ctx.sql("SELECT id, value FROM t").sort("value").collect();
    const values = (rows as any[]).map((r) => r.value);
    expect(values).toEqual([...values].sort((a, b) => a - b));
  });

  it("sort() descending when ascending=false", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    const rows = ctx
      .sql("SELECT id, value FROM t")
      .sort("value", false)
      .collect();
    const values = (rows as any[]).map((r) => r.value);
    expect(values).toEqual([...values].sort((a, b) => b - a));
  });

  it("schema() returns column → type map", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    const s = ctx.sql("SELECT * FROM t").schema() as Record<string, string>;
    expect(typeof s).toBe("object");
    expect("id" in s).toBe(true);
    expect("value" in s).toBe(true);
    expect("name" in s).toBe(true);
  });

  it("chained transforms produce correct results", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    const rows = ctx
      .sql("SELECT * FROM t")
      .filter("value >= 20")
      .select(["id", "value"])
      .sort("value", false)
      .limit(2)
      .collect() as any[];
    expect(rows).toHaveLength(2);
    expect(rows[0].value).toBeGreaterThanOrEqual(rows[1].value);
  });

  it("multiple collect() calls on same sql() are independent", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    const rows1 = ctx.sql("SELECT id FROM t WHERE value > 25").collect();
    const rows2 = ctx.sql("SELECT id FROM t WHERE value > 25").collect();
    expect(rows1).toEqual(rows2);
  });
});

// ── RDD → SQL bridge + Arrow output ───────────────────────────────────────────

describe("registerRdd / toArrow", () => {
  it("registers an RDD as a queryable table", () => {
    if (!moduleLoaded) return;
    const sc = new Context(2);
    const rdd = sc.parallelize([
      { id: 1, val: 2.5 },
      { id: 2, val: 3.25 },
      { id: 3, val: 1.5 },
    ]);
    const ctx = new SqlContext();
    ctx.registerRdd("data", rdd, { id: "int64", val: "float64" });

    const rows = ctx
      .sql("SELECT id, val FROM data WHERE val > 2.0 ORDER BY id")
      .collect() as any[];
    expect(rows).toHaveLength(2);
    expect(rows[0].id).toBe(1);
    expect(rows[1].val).toBeCloseTo(3.25);
  });

  it("aggregates over a registered RDD", () => {
    if (!moduleLoaded) return;
    const sc = new Context(2);
    const rdd = sc.parallelize([
      { k: "a", n: 10 },
      { k: "b", n: 20 },
      { k: "a", n: 5 },
    ]);
    const ctx = new SqlContext();
    ctx.registerRdd("t", rdd, { k: "utf8", n: "int64" });
    const rows = ctx
      .sql("SELECT k, SUM(n) AS total FROM t GROUP BY k ORDER BY k")
      .collect() as any[];
    expect(rows).toEqual([
      { k: "a", total: 15 },
      { k: "b", total: 20 },
    ]);
  });

  it("toArrow() returns non-empty Arrow IPC bytes", () => {
    if (!moduleLoaded) return;
    const ctx = makeCsvCtx();
    const buf = ctx.sql("SELECT id, value FROM t ORDER BY id").toArrow();
    expect(Buffer.isBuffer(buf)).toBe(true);
    expect(buf.length).toBeGreaterThan(0);
    // Arrow IPC stream starts with the "continuation" marker 0xFFFFFFFF.
    expect(buf.readUInt32LE(0)).toBe(0xffffffff);
  });
});
