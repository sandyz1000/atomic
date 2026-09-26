import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { writeFileSync } from "node:fs";
import path from "node:path";

const FIXTURE = "/tmp/chain-fixture.csv";

// reduceByKey output order is not stable, so compare with sorted keys.
function canonical(value: unknown): string {
  return JSON.stringify(value, (_k, v) =>
    v && typeof v === "object" && !Array.isArray(v)
      ? Object.fromEntries(Object.entries(v).sort())
      : v,
  );
}

function assertEqual(actual: unknown, expected: unknown, label: string) {
  const a = canonical(actual);
  const e = canonical(expected);
  if (a !== e) {
    console.error(`FAIL ${label}: expected ${e}, got ${a}`);
    process.exit(1);
  }
  console.log(`ok   ${label}`);
}

async function main() {
  writeFileSync(FIXTURE, "id,region,amount\n1,eu,10\n2,us,80\n3,eu,60\n4,us,20\n5,eu,5\n");

  const transport = new StdioClientTransport({
    command: "node",
    args: [path.join(__dirname, "..", "..", "dist", "src", "server.js")],
  });
  const client = new Client({ name: "pi-atomic-chain-e2e", version: "0.1.0" });
  await client.connect(transport);

  const text = (r: any) => JSON.parse(r.content[0].text);

  await client.callTool({
    name: "atomic_register_source",
    arguments: { name: "sales", path: FIXTURE, format: "csv" },
  });

  const sql = text(
    await client.callTool({
      name: "atomic_sql",
      arguments: { sql: "SELECT region, amount FROM sales WHERE amount > 15" },
    }),
  );
  assertEqual(sql.rowCount, 3, "sql rows selected");

  // The handle is the only thing crossing into the next hop; rows never reach the caller.
  const counts = text(
    await client.callTool({
      name: "count_by",
      arguments: { rows: sql.handle, column: "region" },
    }),
  );
  assertEqual(counts, { eu: 1, us: 2 }, "chained count_by over sql handle");

  const bad = await client.callTool({
    name: "count_by",
    arguments: { rows: "not-a-handle", column: "region" },
  });
  assertEqual(bad.isError, true, "unknown handle is rejected");

  await client.close();
  console.log("chain e2e passed");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
