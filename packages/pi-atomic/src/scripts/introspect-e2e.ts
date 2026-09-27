import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { writeFileSync } from "node:fs";
import path from "node:path";
import { sqlCtx as localSqlCtx } from "../context";

const FIXTURE = "/tmp/chain-fixture.csv";
const PARQUET_DIR = "/tmp/chain-fixture-parquet";

function check(label: string, ok: boolean, detail?: unknown) {
  if (!ok) {
    console.error(`FAIL ${label}${detail === undefined ? "" : `: ${JSON.stringify(detail)}`}`);
    process.exit(1);
  }
  console.log(`ok   ${label}`);
}

async function main() {
  writeFileSync(FIXTURE, "id,region,amount\n1,eu,10\n2,us,80\n3,eu,60\n4,us,20\n5,eu,5\n");

  // Parquet fixture, written directly via the binding (not through the server) so
  // atomic_estimate_size has a source that actually carries row-count metadata.
  localSqlCtx.registerCsv("sales_src", FIXTURE);
  localSqlCtx.sql("SELECT * FROM sales_src").writeParquet(PARQUET_DIR);

  const client = new Client({ name: "pi-atomic-introspect-e2e", version: "0.1.0" });
  await client.connect(
    new StdioClientTransport({
      command: "node",
      args: [path.join(__dirname, "..", "..", "dist", "src", "server.js")],
    }),
  );

  const call = async (name: string, args: Record<string, unknown> = {}) => {
    const r = await client.callTool({ name, arguments: args });
    return JSON.parse((r.content as any)[0].text);
  };

  const sql = "SELECT region, COUNT(*) AS n FROM sales GROUP BY region";

  check("atomic_version", (await call("atomic_version")).version.length > 0);

  await call("atomic_register_source", { name: "sales", path: FIXTURE, format: "csv" });

  check("atomic_list_tables", (await call("atomic_list_tables")).tables.includes("sales"));

  const optimized = await call("atomic_explain", { sql, stage: "optimized" });
  check("atomic_explain optimized", optimized.plan.includes("TableScan: sales"), optimized);

  const analyzed = await call("atomic_explain", { sql, stage: "analyzed" });
  check("atomic_explain analyzed", analyzed.plan.includes("TableScan: sales"), analyzed);

  const physical = await call("atomic_explain", { sql, stage: "physical" });
  check("atomic_explain physical", physical.plan.includes("Exec:"), physical);

  check("atomic_plan_tables", JSON.stringify((await call("atomic_plan_tables", { sql })).tables) === '["sales"]');

  const schema = await call("atomic_table_schema", { table: "sales" });
  check("atomic_table_schema", schema.columns.some((c: any) => c.column_name === "region"), schema);

  const querySchema = await call("atomic_query_schema", { sql });
  check(
    "atomic_query_schema",
    querySchema.columns.some((c: any) => c.column === "n" && c.type === "Int64"),
    querySchema,
  );

  // CSV carries no statistics, so the estimate is null rather than a guessed number.
  const size = await call("atomic_estimate_size", { sql });
  check("atomic_estimate_size reports null for csv", size.rows === null && size.bytes === null, size);

  // Parquet carries row-count metadata, so the same tool resolves a real number here.
  await call("atomic_register_source", { name: "sales_pq", path: PARQUET_DIR, format: "parquet" });
  const pqSize = await call("atomic_estimate_size", {
    sql: "SELECT region, COUNT(*) AS n FROM sales_pq GROUP BY region",
  });
  check("atomic_estimate_size resolves rows for parquet", pqSize.rows === 5, pqSize);

  const head = await call("atomic_read_head", { path: FIXTURE, lines: 1 });
  check("atomic_read_head", head.lines[0] === "id,region,amount", head);

  const unknown = await client.callTool({ name: "atomic_table_schema", arguments: { table: "nope" } });
  check("unknown table rejected", unknown.isError === true);

  const badSql = await client.callTool({ name: "atomic_explain", arguments: { sql: "SELECT * FROM nope" } });
  check("bad sql rejected", badSql.isError === true);

  const run = await call("atomic_sql", { sql });
  check("atomic_sql returns schema", run.schema.some((c: any) => c.column === "region"), run);

  await client.close();
  console.log("introspect e2e passed");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
