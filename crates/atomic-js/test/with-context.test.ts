/**
 * Coverage for the *WithContext RDD methods (A1).
 *
 * These exist because a plain closure shipped to a worker loses its captured
 * free variables — `fn.toString()` only carries the function body, not the
 * enclosing scope. `mapWithContext` etc. serialize that scope explicitly as
 * JSON and inject it as a second argument, so the captured state survives
 * the trip. Local mode (exercised here) threads the same `ctx` value
 * directly, matching the wire contract used in distributed mode.
 *
 * Prerequisites: build the native module first with one of
 *   cargo build -p atomic-js           (debug)
 *   cargo build --release -p atomic-js (release)
 *   cd crates/atomic-js && npm run build
 * Then run: npm test
 */
import { describe, it, expect, beforeAll } from "vitest";

let Context: typeof import("..").Context;
let moduleLoaded = false;

beforeAll(() => {
  try {
    const m = require("..");
    Context = m.Context;
    moduleLoaded = true;
  } catch {
    console.warn(
      "atomic-js native module not built — skipping all with-context tests.\n" +
        "Run: cargo build -p atomic-js && npm test"
    );
  }
});

function ctx() {
  return new Context(2);
}

describe("mapWithContext", () => {
  it("ctx_roundtrips: captured threshold reaches the worker function", () => {
    if (!moduleLoaded) return;
    const result = ctx()
      .parallelize([1, 5, 10, 15])
      .mapWithContext(
        { threshold: 7 },
        (x: number, c: { threshold: number }) => (x > c.threshold ? x : 0)
      )
      .collect()
      .sort((a: number, b: number) => a - b);
    expect(result).toEqual([0, 0, 10, 15]);
  });

  it("propagates a non-trivial captured object", () => {
    if (!moduleLoaded) return;
    const result = ctx()
      .parallelize([1, 2, 3])
      .mapWithContext(
        { offset: 100, label: "x" },
        (x: number, c: { offset: number; label: string }) => `${c.label}${x + c.offset}`
      )
      .collect()
      .sort();
    expect(result).toEqual(["x101", "x102", "x103"]);
  });
});

describe("filterWithContext", () => {
  it("filters using the captured context", () => {
    if (!moduleLoaded) return;
    const result = ctx()
      .parallelize([1, 2, 3, 4, 5])
      .filterWithContext({ min: 3 }, (x: number, c: { min: number }) => x >= c.min)
      .collect()
      .sort((a: number, b: number) => a - b);
    expect(result).toEqual([3, 4, 5]);
  });
});

describe("foldWithContext", () => {
  it("folds with a captured multiplier", () => {
    if (!moduleLoaded) return;
    const result = ctx()
      .parallelize([1, 2, 3])
      .foldWithContext({ mult: 10 }, 0, (acc: number, x: number, c: { mult: number }) => acc + x * c.mult);
    expect(result).toBe(60);
  });
});
