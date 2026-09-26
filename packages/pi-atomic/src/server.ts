import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { registerFixedTools } from "./tools";
import { registerTaskTools } from "./dispatch";

function buildServer(): McpServer {
  const server = new McpServer({ name: "pi-atomic", version: "0.1.0" });
  registerFixedTools(server);
  registerTaskTools(server);
  return server;
}

async function connectStdio() {
  await buildServer().connect(new StdioServerTransport());
}

async function connectHttp(port: number) {
  // A Protocol instance owns exactly one transport, so each HTTP session gets its own
  // server; the task registry is module-level and shared across them.
  const sessions = new Map<string, StreamableHTTPServerTransport>();

  const http = createServer(async (req, res) => {
    try {
      const sid = req.headers["mcp-session-id"];
      let transport = typeof sid === "string" ? sessions.get(sid) : undefined;

      if (!transport) {
        if (req.method !== "POST") {
          res.statusCode = 400;
          res.end("Missing or unknown mcp-session-id");
          return;
        }
        const created: StreamableHTTPServerTransport = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => randomUUID(),
          onsessioninitialized: (id) => {
            sessions.set(id, created);
          },
        });
        created.onclose = () => {
          if (created.sessionId) sessions.delete(created.sessionId);
        };
        await buildServer().connect(created);
        transport = created;
      }

      await transport.handleRequest(req, res);
    } catch (err) {
      if (!res.headersSent) res.statusCode = 500;
      res.end(String(err));
    }
  });

  http.listen(port);
}

async function main() {
  await import("../tasks/index");

  if ((process.env.ATOMIC_TRANSPORT ?? "stdio") === "http") {
    await connectHttp(Number(process.env.ATOMIC_HTTP_PORT ?? 3000));
    return;
  }

  await connectStdio();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
