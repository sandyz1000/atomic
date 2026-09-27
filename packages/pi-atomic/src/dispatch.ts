import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ctx } from "./context";
import { getRegisteredTasks } from "./registry";
import { storeRows, getRows } from "./handles";

const LARGE_ARRAY_THRESHOLD = 50;
const PREVIEW_ROWS = 20;

function text(payload: unknown) {
  return { content: [{ type: "text" as const, text: JSON.stringify(payload) }] };
}

function resolveHandles(spec: { handleArgs?: string[] }, args: any) {
  const resolved = { ...args };
  for (const name of spec.handleArgs ?? []) {
    const rows = getRows(resolved[name]);
    if (rows === undefined) {
      return { error: `Unknown handle for '${name}': ${String(resolved[name])}` };
    }
    resolved[name] = rows;
  }
  return { resolved };
}

export function registerTaskTools(server: McpServer) {
  for (const { spec, fn } of getRegisteredTasks()) {
    server.registerTool(
      spec.name,
      { description: spec.description, inputSchema: spec.inputSchema },
      async (args: any) => {
        const { resolved, error } = resolveHandles(spec, args);
        if (error !== undefined) {
          return { content: [{ type: "text" as const, text: error }], isError: true };
        }

        const result = fn(ctx, resolved);
        const payload =
          Array.isArray(result) && result.length > LARGE_ARRAY_THRESHOLD
            ? {
                handle: storeRows(result),
                previewRows: result.slice(0, PREVIEW_ROWS),
                rowCount: result.length,
              }
            : result;
        return text(payload);
      },
    );
  }
}
