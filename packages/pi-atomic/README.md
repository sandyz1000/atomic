# pi-atomic

An MCP server that exposes Atomic's distributed compute engine as tools an MCP client
(Pi, Claude Code) calls directly. The client's own multi-turn tool-calling loop plans and
executes against Atomic; this package holds no agentic logic of its own. This replaces the
earlier `atomic-nlq` coordinator-loop approach (`LlmPlanner`/`AgentLoop`/`WorkflowExecutor`),
which has been removed — see `notes/pi-plugin-integration-plan.md` for the rationale.

## Install and build

`pi-atomic` depends on `@atomic-compute/js`, Atomic's Node bindings, via a local `file:`
dependency on `../../crates/atomic-js`. Build that package first:

```sh
cd crates/atomic-js
npm install
npm run build
```

Then build `pi-atomic`:

```sh
cd packages/pi-atomic
npm install
npm run build
```

## Running the server

`npm run build` compiles to `dist/src/server.js`, which speaks MCP over stdio. Start it
directly with `npm start`, or point an MCP client at it. For example, in Claude Code's
`.mcp.json` or Pi's MCP config:

```json
{
  "mcpServers": {
    "atomic": {
      "command": "node",
      "args": ["packages/pi-atomic/dist/src/server.js"]
    }
  }
}
```

On startup the server constructs one long-lived `Context` and `SqlContext` for the process
lifetime — every tool call in the session shares the same compute context.

## Fixed tools

Data tools, always registered:

- `atomic_register_source({ name, path, format })` — registers a CSV, Parquet, or JSON
  file or directory as a named SQL table.
- `atomic_sql({ sql })` — runs a query against registered tables. Returns a handle, the
  result schema, a preview of the first 20 rows, and the total row count — not the full
  result, so large results don't consume the client's context budget.
- `atomic_collect_handle({ handle, offset?, limit? })` — pages through the rows behind a
  handle returned by `atomic_sql`.

Inspection tools, which let the agent reason about a query without running it. These mirror
[`pyspark-mcp`](https://github.com/SemyonSinchenko/pyspark-mcp-server), which exposes Spark's
logical and physical plans plus catalog metadata so an agent can optimize a query:

- `atomic_explain({ sql, stage })` — the `analyzed`, `optimized`, or `physical` plan.
  Plans come from `EXPLAIN VERBOSE`, and because DataFusion collapses an unchanged stage to
  the literal `SAME TEXT AS ABOVE`, the previous stage's text is carried forward.
- `atomic_plan_tables({ sql })` — the tables a query reads, extracted from its logical plan.
- `atomic_estimate_size({ sql })` — estimated result rows and bytes from the physical plan's
  statistics. Read the caveat below.
- `atomic_list_tables()` — tables registered in this server's SQL context.
- `atomic_table_schema({ table })` — column names, types, and nullability.
- `atomic_query_schema({ sql })` — the columns and types a query would return, resolved
  without executing it.
- `atomic_read_head({ path, lines })` — first lines of a local file, capped at 200 lines /
  64 KiB, to discover a source's format before registering it.
- `atomic_version()` — the engine binding version.

### What does not map

`pyspark-mcp` tools that have no Atomic equivalent, and why:

- **Catalog / database hierarchy** (current catalog, list databases, database exists, table
  comments). Atomic's SQL context has a flat namespace of registered tables and no catalog
  concept. DataFusion's `information_schema` would provide most of this, but it is disabled
  in Atomic's `SessionConfig`, so `SHOW TABLES` and `information_schema.tables` both fail
  today. Enabling it is a small change in `atomic-sql`, not something this package can do.
- **Size estimation is source-dependent.** `atomic_estimate_size` reads
  `physical_plan_with_stats`. Parquet reports a row count from file metadata; CSV and JSON
  report nothing because they carry none, and DataFusion's `ANALYZE TABLE` is parsed but
  unimplemented. `rows`/`bytes` are `number | null` — `null` means unavailable, not zero,
  the same sentinel role `pyspark-mcp`'s size-estimation tool fills with `-1`/`"missing"`.

### File reads

`atomic_read_head` reads any path the server process can open. It exists because
determining a source's format is otherwise guesswork, but it means an agent driving this
server can read local files. The same is true of the SQL tools, which can read any path
passed to `atomic_register_source`.

Example: register a CSV and query it.

```
atomic_register_source({ name: "orders", path: "/data/orders.csv", format: "csv" })
atomic_sql({ sql: "SELECT customer_id, SUM(total) FROM orders GROUP BY customer_id" })
// -> { handle: "...", columns: [...], previewRows: [...], rowCount: 1284 }
atomic_collect_handle({ handle: "...", offset: 20, limit: 20 })
```

## Authoring a task

A task is a named function that becomes its own MCP tool automatically. Tasks live in
`tasks/` and register themselves at import time by calling `registerTask()` from
`src/registry.ts`:

```ts
export interface TaskSpec {
  name: string;
  description: string;
  inputSchema: Record<string, z.ZodTypeAny>; // Zod raw shape
}

export type TaskFn = (ctx: Context, args: any) => any;

export function registerTask(spec: TaskSpec, fn: TaskFn): void;
```

`tasks/index.ts` has a real example, `word_count`, which distributes a map-reduce word
count over `ctx`, the shared Atomic `Context`:

```ts
import { z } from "zod";
import { registerTask } from "../src/registry";

registerTask(
  {
    name: "word_count",
    description: "Count word occurrences across an array of input lines using a distributed map-reduce pipeline.",
    inputSchema: {
      lines: z.array(z.string()).describe("Lines of text to word-count"),
    },
  },
  (ctx, args: { lines: string[] }) => {
    const counts = ctx
      .parallelize(args.lines)
      .flatMap((line: string) => line.split(/\s+/).filter(Boolean))
      .map((word: string): [string, number] => [word, 1])
      .reduceByKey((a: number, b: number) => a + b)
      .collect();
    return Object.fromEntries(counts);
  },
);
```

To add a task, write a new file under `tasks/` (or add to `index.ts`) that calls
`registerTask()` at the top level. `src/server.ts` imports `tasks/index.ts` on startup and
calls `registerTaskTools()`, which turns every registered task into an MCP tool named and
described exactly as given, with the Zod shape as its input schema. If a task returns an
array longer than 50 elements, the tool response stores it behind a handle and returns a
preview instead of the full array, matching the fixed tools' preview convention.

## Chaining

A task that consumes an earlier result instead of inline data lists those argument names in
`handleArgs`. The dispatcher swaps each handle for its stored rows before calling the task, so
the data moves server-side and only the handle crosses the model's context:

```typescript
registerTask(
  {
    name: "count_by",
    description: "Count rows from an earlier tool result, grouped by one of their columns.",
    inputSchema: {
      rows: z.string().describe("Handle to rows produced by an earlier tool call"),
      column: z.string().describe("Column to group by"),
    },
    handleArgs: ["rows"],
  },
  (ctx, args: { rows: Record<string, unknown>[]; column: string }) => {
    // args.rows is already materialized here
  },
);
```

Handles come from any tool that produced one — `atomic_sql`, or another task that returned a
large array — so `atomic_register_source` → `atomic_sql` → task is a working chain. An unknown
handle is rejected as a tool error rather than reaching the task as `undefined`. The agent
drives the chain one hop per turn; nothing pre-plans it.

## Transports

Stdio is the default. Set `ATOMIC_TRANSPORT=http` to serve Streamable HTTP instead, on
`ATOMIC_HTTP_PORT` (default `3000`); each HTTP session gets its own protocol instance against
the shared task registry.

## Distributed mode

The `Context` is built with `Context::from_env()`, so distributed execution is configured by
environment rather than by this package: set `ATOMIC_DEPLOYMENT_MODE=distributed` and
`ATOMIC_LOCAL_IP`, and list workers in `~/hosts.conf`. Nothing in this package needs to change
to run against a cluster. A live multi-process cluster is required to exercise the path; with
no workers reachable, the first action fails with `worker handshake failed: no reachable
workers found`.
