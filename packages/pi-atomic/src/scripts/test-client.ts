import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import path from "node:path";

async function main() {
  const transport = new StdioClientTransport({
    command: "node",
    args: [path.join(__dirname, "..", "..", "dist", "src", "server.js")],
  });
  const client = new Client({ name: "pi-atomic-test-client", version: "0.1.0" });
  await client.connect(transport);

  const tools = await client.listTools();
  console.log("tools:", JSON.stringify(tools, null, 2));

  const command = process.argv[2];
  if (command === "call") {
    const toolName = process.argv[3];
    const args = process.argv[4] ? JSON.parse(process.argv[4]) : {};
    const result = await client.callTool({ name: toolName, arguments: args });
    console.log("result:", JSON.stringify(result, null, 2));
  }

  await client.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
