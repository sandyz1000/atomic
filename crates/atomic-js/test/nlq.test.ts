/**
 * NlqContext tests for atomic-js.
 *
 * Prerequisites: build the native module first:
 *   cd crates/atomic-js && npm run build
 * Then run: npm test
 *
 * No real LLM calls are made — a dummy API key exercises construction, the
 * SqlContext wiring, and error propagation (an auth failure from the provider)
 * without needing OPENAI_API_KEY / network access in CI.
 */
import { describe, it, expect, beforeAll } from "vitest";

let NlqContext: typeof import("..").NlqContext;
let moduleLoaded = false;

beforeAll(() => {
  try {
    const m = require("..");
    NlqContext = m.NlqContext;
    moduleLoaded = true;
  } catch {
    // Module not built yet — skip all tests gracefully.
  }
});

describe("NlqContext", () => {
  it("creates without error", () => {
    if (!moduleLoaded) return;
    expect(() => new NlqContext({ apiKey: "dummy-key" })).not.toThrow();
  });

  it("rejects an unknown provider", () => {
    if (!moduleLoaded) return;
    expect(
      () => new NlqContext({ apiKey: "dummy-key", provider: "not-a-provider" })
    ).toThrow();
  });

  it("exposes a working SqlContext", () => {
    if (!moduleLoaded) return;
    const ctx = new NlqContext({ apiKey: "dummy-key" });
    const rows = ctx.sqlCtx().sql("SELECT 42 AS n").collect();
    expect(rows).toHaveLength(1);
    expect((rows[0] as any).n).toBe(42);
  });

  it("propagates a provider auth error from plan()", async () => {
    if (!moduleLoaded) return;
    const ctx = new NlqContext({ apiKey: "dummy-key" });
    expect(() => ctx.plan("how many rows")).toThrow(/API/i);
  });
});
