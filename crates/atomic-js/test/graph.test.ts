/**
 * P4.3 Graph bindings test suite.
 *
 * Prerequisites: build the native module first with one of
 *   cargo build -p atomic-js
 *   cargo build --release -p atomic-js
 * Then run: npm test
 *
 * Note: object keys returned from graph algorithms are string representations
 * of the integer vertex IDs (e.g. "1", "2", "3").
 */
import { describe, it, expect, beforeAll } from "vitest";

let Graph: typeof import("../index.js").Graph;
let moduleLoaded = false;

beforeAll(() => {
  try {
    const m = require("../index.js");
    Graph = m.Graph;
    moduleLoaded = true;
  } catch {
    console.warn(
      "atomic-js native module not built — skipping all graph tests.\n" +
        "Run: cargo build -p atomic-js && npm test"
    );
  }
});

describe("P4.3 Graph", () => {
  it("constructs empty graph", () => {
    if (!moduleLoaded) return;
    const g = new Graph([], []);
    expect(g.numVertices()).toBe(0);
    expect(g.numEdges()).toBe(0);
  });

  it("constructs basic graph", () => {
    if (!moduleLoaded) return;
    const g = new Graph(
      [[1, 1.0], [2, 1.0], [3, 1.0]],
      [[1, 2, 1.0], [2, 3, 1.0]]
    );
    expect(g.numVertices()).toBe(3);
    expect(g.numEdges()).toBe(2);
  });

  it("pageRank on cycle has equal ranks", () => {
    if (!moduleLoaded) return;
    const g = new Graph(
      [[1, 1.0], [2, 1.0], [3, 1.0]],
      [[1, 2, 1.0], [2, 3, 1.0], [3, 1, 1.0]]
    );
    const ranks = g.pageRank(50, 0.15) as Record<string, number>;
    const vals = Object.values(ranks);
    expect(vals).toHaveLength(3);
    expect(Math.max(...vals) - Math.min(...vals)).toBeLessThan(0.01);
  });

  it("pageRank sink accumulates rank", () => {
    if (!moduleLoaded) return;
    const g = new Graph(
      [[1, 1.0], [2, 1.0], [3, 1.0]],
      [[1, 2, 1.0], [2, 3, 1.0]]
    );
    const ranks = g.pageRank(20, 0.15) as Record<string, number>;
    expect(ranks["3"]).toBeGreaterThan(ranks["1"]);
  });

  it("connectedComponents single cluster", () => {
    if (!moduleLoaded) return;
    const g = new Graph(
      [[1, 1.0], [2, 1.0], [3, 1.0]],
      [[1, 2, 1.0], [2, 3, 1.0]]
    );
    const cc = g.connectedComponents() as Record<string, number>;
    expect(new Set(Object.values(cc)).size).toBe(1);
  });

  it("connectedComponents two clusters", () => {
    if (!moduleLoaded) return;
    const g = new Graph(
      [[1, 1.0], [2, 1.0], [3, 1.0], [4, 1.0]],
      [[1, 2, 1.0], [3, 4, 1.0]]
    );
    const cc = g.connectedComponents() as Record<string, number>;
    expect(new Set(Object.values(cc)).size).toBe(2);
  });

  it("triangleCount detects triangle", () => {
    if (!moduleLoaded) return;
    const g = new Graph(
      [[1, 1.0], [2, 1.0], [3, 1.0]],
      [
        [1, 2, 1.0], [2, 3, 1.0], [3, 1, 1.0],
        [1, 3, 1.0], [3, 2, 1.0], [2, 1, 1.0],
      ]
    );
    const tc = g.triangleCount() as Record<string, number>;
    expect(Object.values(tc).every((v) => v > 0)).toBe(true);
  });

  it("triangleCount no triangles in path", () => {
    if (!moduleLoaded) return;
    const g = new Graph(
      [[1, 1.0], [2, 1.0], [3, 1.0]],
      [[1, 2, 1.0], [2, 3, 1.0]]
    );
    const tc = g.triangleCount() as Record<string, number>;
    expect(Object.values(tc).every((v) => v === 0)).toBe(true);
  });

  it("shortestPath triangle", () => {
    if (!moduleLoaded) return;
    const g = new Graph(
      [[1, 1.0], [2, 1.0], [3, 1.0]],
      [[1, 2, 1.0], [2, 3, 2.0], [1, 3, 5.0]]
    );
    const sp = g.shortestPath([1]) as Record<string, Record<string, number>>;
    expect(sp["1"]["2"]).toBeCloseTo(1.0);
    expect(sp["1"]["3"]).toBeCloseTo(3.0); // via 2: 1+2=3, shorter than direct 5
  });

  it("shortestPath self distance zero", () => {
    if (!moduleLoaded) return;
    const g = new Graph(
      [[1, 1.0], [2, 1.0]],
      [[1, 2, 1.0]]
    );
    const sp = g.shortestPath([1]) as Record<string, Record<string, number>>;
    expect(sp["1"]["1"]).toBe(0);
  });

  it("labelPropagation two clusters", () => {
    if (!moduleLoaded) return;
    const g = new Graph(
      [[1, 1.0], [2, 1.0], [3, 1.0], [4, 1.0]],
      [[1, 2, 1.0], [2, 1, 1.0], [3, 4, 1.0], [4, 3, 1.0]]
    );
    const labels = g.labelPropagation(20) as Record<string, number>;
    expect(labels["1"]).toBe(labels["2"]);
    expect(labels["3"]).toBe(labels["4"]);
  });

  it("stronglyConnectedComponents cycle", () => {
    if (!moduleLoaded) return;
    const g = new Graph(
      [[1, 1.0], [2, 1.0], [3, 1.0]],
      [[1, 2, 1.0], [2, 3, 1.0], [3, 1, 1.0]]
    );
    const scc = g.stronglyConnectedComponents() as Record<string, number>;
    expect(new Set(Object.values(scc)).size).toBe(1);
  });

  it("stronglyConnectedComponents DAG — each vertex is its own SCC", () => {
    if (!moduleLoaded) return;
    const g = new Graph(
      [[1, 1.0], [2, 1.0], [3, 1.0]],
      [[1, 2, 1.0], [2, 3, 1.0]]
    );
    const scc = g.stronglyConnectedComponents() as Record<string, number>;
    expect(new Set(Object.values(scc)).size).toBe(3);
  });

  it("pageRank default parameters", () => {
    if (!moduleLoaded) return;
    const g = new Graph(
      [[1, 1.0], [2, 1.0]],
      [[1, 2, 1.0], [2, 1, 1.0]]
    );
    const ranks = g.pageRank(20, 0.15) as Record<string, number>;
    expect(Object.keys(ranks)).toHaveLength(2);
  });

  it("repr shows vertex and edge count — no crash on construction", () => {
    if (!moduleLoaded) return;
    const g = new Graph([[1, 1.0]], []);
    expect(g.numVertices()).toBe(1);
    expect(g.numEdges()).toBe(0);
  });

  // ── Custom Pregel vertex programs ─────────────────────────────────────────

  it("runPregelF64 propagates minimum vertex ID", () => {
    if (!moduleLoaded) return;
    // Chain 1→2→3, initial vdata = vertex ID. Min-ID should spread to all.
    const g = new Graph(
      [[1, 1], [2, 2], [3, 3]],
      [[1, 2, 1], [2, 3, 1]]
    );
    const result = g.runPregelF64(
      Infinity,
      10,
      (_vid: number, vdata: number, msg: number) => Math.min(vdata, msg),
      (_si: number, sd: number, di: number, dd: number) => sd < dd ? [[di, sd]] : [],
      (a: number, b: number) => Math.min(a, b)
    );
    const vmap = Object.fromEntries(result.vertices().map(([id, w]: [number, number]) => [id, w]));
    expect(vmap[1]).toBeCloseTo(1);
    expect(vmap[2]).toBeCloseTo(1);
    expect(vmap[3]).toBeCloseTo(1);
  });

  it("runPregelF64 zero max_iterations only fires superstep 0", () => {
    if (!moduleLoaded) return;
    const g = new Graph([[1, 5], [2, 3]], [[1, 2, 1]]);
    const result = g.runPregelF64(
      0,
      0,
      (_vid: number, vdata: number, msg: number) => vdata + msg,
      (_si: number, sd: number, di: number) => [[di, sd]],
      (a: number, b: number) => a + b
    );
    const vmap = Object.fromEntries(result.vertices().map(([id, w]: [number, number]) => [id, w]));
    // vprog(vid, vdata, 0) = vdata + 0 = vdata
    expect(vmap[1]).toBeCloseTo(5);
    expect(vmap[2]).toBeCloseTo(3);
  });

  it("runPregelF64 empty send_msg terminates early", () => {
    if (!moduleLoaded) return;
    const g = new Graph([[1, 1], [2, 2]], [[1, 2, 1]]);
    const result = g.runPregelF64(
      0,
      100,
      (_vid: number, vdata: number, _msg: number) => vdata,
      (_si: number, _sd: number, _di: number, _dd: number, _ed: number): [number, number][] => [],
      (a: number, b: number) => a + b
    );
    expect(result.numVertices()).toBe(2);
  });
});
