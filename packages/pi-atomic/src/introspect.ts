import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { existsSync, readFileSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { sqlCtx } from "./context";

const MAX_HEAD_LINES = 200;
const MAX_HEAD_BYTES = 64 * 1024;

/// The binding's `exports` map blocks the `./package.json` subpath, so walk up from the resolved entry.
function engineVersion(): string {
  let dir = dirname(require.resolve("@atomic-compute/js"));
  for (let depth = 0; depth < 4; depth++) {
    const candidate = join(dir, "package.json");
    if (existsSync(candidate)) {
      const pkg = JSON.parse(readFileSync(candidate, "utf8")) as { name?: string; version?: string };
      if (pkg.name === "@atomic-compute/js") return pkg.version ?? "unknown";
    }
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  return "unknown";
}

interface PlanRow {
  plan: string;
  plan_type: string;
}

function json(payload: unknown) {
  return { content: [{ type: "text" as const, text: JSON.stringify(payload) }] };
}

function fail(message: string) {
  return { content: [{ type: "text" as const, text: message }], isError: true };
}

// DataFusion collapses a stage whose plan matches the previous one to this sentinel,
// so the text has to be carried forward before a stage can be read.
const SAME_AS_ABOVE = "SAME TEXT AS ABOVE";

function verbosePlan(sql: string): PlanRow[] {
  const rows = sqlCtx.sql(`EXPLAIN VERBOSE ${sql}`).collect() as PlanRow[];
  let last = "";
  return rows.map((row) => {
    if (row.plan.trim() === SAME_AS_ABOVE) return { plan_type: row.plan_type, plan: last };
    last = row.plan;
    return row;
  });
}

/// Table names reach SQL as text, so they are checked against the registry rather than interpolated blind.
function registeredTable(name: string): string | undefined {
  const tables = sqlCtx.tableNames();
  return tables.includes(name) ? name : undefined;
}

export function registerIntrospectTools(server: McpServer) {
  server.registerTool(
    "atomic_version",
    {
      description: "Version of the Atomic engine binding loaded by this server.",
      inputSchema: {},
    },
    async () => {
      return json({ engine: "@atomic-compute/js", version: engineVersion(), node: process.version });
    },
  );

  server.registerTool(
    "atomic_explain",
    {
      description:
        "Return a query's plan without executing it. `analyzed` is the resolved logical plan, `optimized` the plan after the optimizer, `physical` the execution plan.",
      inputSchema: {
        sql: z.string().describe("SQL query to explain"),
        stage: z.enum(["analyzed", "optimized", "physical"]).default("optimized"),
      },
    },
    async ({ sql, stage }) => {
      try {
        const rows = verbosePlan(sql);
        const wanted =
          stage === "analyzed" ? "analyzed_logical_plan" : stage === "optimized" ? "logical_plan" : "physical_plan";
        const found = rows.find((r) => r.plan_type === wanted);
        if (!found) return fail(`Plan stage '${wanted}' not produced. Stages seen: ${rows.map((r) => r.plan_type).join(", ")}`);
        return json({ stage, plan: found.plan });
      } catch (e) {
        return fail(String((e as Error).message ?? e));
      }
    },
  );

  server.registerTool(
    "atomic_plan_tables",
    {
      description: "List the tables a query reads, extracted from its logical plan.",
      inputSchema: {
        sql: z.string().describe("SQL query to inspect"),
      },
    },
    async ({ sql }) => {
      try {
        const plan = verbosePlan(sql).find((r) => r.plan_type === "logical_plan")?.plan ?? "";
        const tables = [...new Set([...plan.matchAll(/TableScan:\s*(\S+)/g)].map((m) => m[1]))];
        return json({ tables });
      } catch (e) {
        return fail(String((e as Error).message ?? e));
      }
    },
  );

  server.registerTool(
    "atomic_estimate_size",
    {
      description:
        "Estimate a query's result size from the physical plan's statistics, without running it. Statistics come from source metadata, so Parquet reports a row estimate and CSV/JSON report 'Absent'.",
      inputSchema: {
        sql: z.string().describe("SQL query to estimate"),
      },
    },
    async ({ sql }) => {
      try {
        const rows = verbosePlan(sql);
        const plan = rows.find((r) => r.plan_type === "physical_plan_with_stats")?.plan;
        if (plan === undefined) return fail("Physical plan with statistics was not produced for this query.");
        const root = plan.split("\n")[0] ?? "";
        const stats = root.slice(root.indexOf("statistics="));
        const rowsStat = /Rows=([^,\]]+)/.exec(stats)?.[1] ?? "Absent";
        const bytesStat = /Bytes=([^,\]]+)/.exec(stats)?.[1] ?? "Absent";
        return json({
          rows: rowsStat,
          bytes: bytesStat,
          available: rowsStat !== "Absent" || bytesStat !== "Absent",
          rootOperator: root.split(", statistics=")[0],
        });
      } catch (e) {
        return fail(String((e as Error).message ?? e));
      }
    },
  );

  server.registerTool(
    "atomic_list_tables",
    {
      description: "List the tables registered in this server's SQL context.",
      inputSchema: {},
    },
    async () => json({ tables: sqlCtx.tableNames() }),
  );

  server.registerTool(
    "atomic_table_schema",
    {
      description: "Column names, types, and nullability for a registered table.",
      inputSchema: {
        table: z.string().describe("Registered table name"),
      },
    },
    async ({ table }) => {
      if (registeredTable(table) === undefined) {
        return fail(`Unknown table '${table}'. Registered: ${sqlCtx.tableNames().join(", ") || "(none)"}`);
      }
      try {
        return json({ table, columns: sqlCtx.sql(`DESCRIBE ${table}`).collect() });
      } catch (e) {
        return fail(String((e as Error).message ?? e));
      }
    },
  );

  server.registerTool(
    "atomic_query_schema",
    {
      description: "Column names and types a query would return, resolved without executing it.",
      inputSchema: {
        sql: z.string().describe("SQL query to describe"),
      },
    },
    async ({ sql }) => {
      try {
        const dtypes = sqlCtx.sql(sql).dtypes() as [string, string][];
        return json({ columns: dtypes.map(([column, type]) => ({ column, type })) });
      } catch (e) {
        return fail(String((e as Error).message ?? e));
      }
    },
  );

  server.registerTool(
    "atomic_read_head",
    {
      description:
        "Read the first lines of a local file, to discover a data source's format before registering it. Reads at most 200 lines / 64 KiB.",
      inputSchema: {
        path: z.string().describe("Path to the file"),
        lines: z.number().int().positive().max(MAX_HEAD_LINES).default(20),
      },
    },
    async ({ path, lines }) => {
      try {
        if (statSync(path).isDirectory()) return fail(`${path} is a directory; pass a file.`);
        const text = readFileSync(path, { encoding: "utf8", flag: "r" }).slice(0, MAX_HEAD_BYTES);
        const all = text.split("\n");
        return json({ path, lines: all.slice(0, lines), truncated: all.length > lines });
      } catch (e) {
        return fail(String((e as Error).message ?? e));
      }
    },
  );
}
