import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

async function main() {
  const client = new Client({ name: "pi-atomic-http-e2e", version: "0.1.0" });
  await client.connect(
    new StreamableHTTPClientTransport(new URL("http://127.0.0.1:3124/mcp")),
  );

  const tools = await client.listTools();
  console.log("tools:", tools.tools.map((t) => t.name).join(","));

  const wc = await client.callTool({
    name: "word_count",
    arguments: { lines: ["a b a", "b a"] },
  });
  console.log("word_count:", JSON.stringify(wc));

  await client.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
