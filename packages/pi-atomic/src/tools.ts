import { z } from "zod";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { sqlCtx } from "./context";
import { storeRows, getRows } from "./handles";

const PREVIEW_ROWS = 20;

function json(payload: unknown) {
  return { content: [{ type: "text" as const, text: JSON.stringify(payload) }] };
}

export function registerFixedTools(server: McpServer) {
  server.registerTool(
    "atomic_register_source",
    {
      description: "Register a CSV/Parquet/JSON file or directory as a named SQL table.",
      inputSchema: {
        name: z.string().describe("Table name to use in SQL queries"),
        path: z.string().describe("Path to the file or directory"),
        format: z.enum(["csv", "parquet", "json"]),
      },
    },
    async ({ name, path, format }) => {
      if (format === "csv") sqlCtx.registerCsv(name, path);
      else if (format === "parquet") sqlCtx.registerParquet(name, path);
      else sqlCtx.registerJson(name, path);
      return json({ success: true, table: name });
    },
  );

  server.registerTool(
    "atomic_sql",
    {
      description:
        "Run a SQL query against registered tables. Returns a handle plus a small preview, not the full result set.",
      inputSchema: {
        sql: z.string().describe("SQL query string"),
      },
    },
    async ({ sql }) => {
      const df = sqlCtx.sql(sql);
      const schema = (df.dtypes() as [string, string][]).map(([column, type]) => ({ column, type }));
      const rows = df.collect();
      const handle = storeRows(rows);
      return json({
        handle,
        schema,
        columns: schema.map((c) => c.column),
        previewRows: rows.slice(0, PREVIEW_ROWS),
        rowCount: rows.length,
      });
    },
  );

  server.registerTool(
    "atomic_collect_handle",
    {
      description: "Page through rows previously produced by atomic_sql.",
      inputSchema: {
        handle: z.string(),
        offset: z.number().int().nonnegative().optional(),
        limit: z.number().int().positive().optional(),
      },
    },
    async ({ handle, offset, limit }) => {
      const rows = getRows(handle);
      if (rows === undefined) {
        return {
          content: [{ type: "text" as const, text: `Unknown handle: ${handle}` }],
          isError: true,
        };
      }
      const start = offset ?? 0;
      const end = limit !== undefined ? start + limit : undefined;
      return json({ rows: rows.slice(start, end), offset: start, total: rows.length });
    },
  );
}
