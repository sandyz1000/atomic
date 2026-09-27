import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import path from "node:path";

async function main() {
  const transport = new StdioClientTransport({
    command: "node",
    args: [path.join(__dirname, "..", "..", "dist", "src", "server.js")],
  });
  const client = new Client({ name: "pi-atomic-e2e", version: "0.1.0" });
  await client.connect(transport);

  const reg = await client.callTool({
    name: "atomic_register_source",
    arguments: { name: "orders", path: "/tmp/fixture.csv", format: "csv" },
  });
  console.log("register:", JSON.stringify(reg));

  const sql = await client.callTool({
    name: "atomic_sql",
    arguments: { sql: "SELECT * FROM orders WHERE amount > 30 ORDER BY id" },
  });
  console.log("sql:", JSON.stringify(sql));

  const handle = JSON.parse((sql.content as any)[0].text).handle;

  const page = await client.callTool({
    name: "atomic_collect_handle",
    arguments: { handle, offset: 1, limit: 1 },
  });
  console.log("page:", JSON.stringify(page));

  const wc = await client.callTool({
    name: "word_count",
    arguments: { lines: ["the quick brown fox", "the lazy dog", "the fox jumps"] },
  });
  console.log("word_count:", JSON.stringify(wc));

  await client.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
