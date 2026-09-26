import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ctx } from "./context";
import { getRegisteredTasks } from "./registry";
import { storeRows } from "./handles";

const LARGE_ARRAY_THRESHOLD = 50;
const PREVIEW_ROWS = 20;

export function registerTaskTools(server: McpServer) {
  for (const { spec, fn } of getRegisteredTasks()) {
    server.registerTool(
      spec.name,
      { description: spec.description, inputSchema: spec.inputSchema },
      async (args: any) => {
        const result = fn(ctx, args);
        const payload =
          Array.isArray(result) && result.length > LARGE_ARRAY_THRESHOLD
            ? {
                handle: storeRows(result),
                previewRows: result.slice(0, PREVIEW_ROWS),
                rowCount: result.length,
              }
            : result;
        return { content: [{ type: "text" as const, text: JSON.stringify(payload) }] };
      },
    );
  }
}
