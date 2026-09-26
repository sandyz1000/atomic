import { Context } from "@atomic-compute/js";
import type { z } from "zod";

export interface TaskSpec {
  name: string;
  description: string;
  /** Zod raw shape (e.g. `{ words: z.array(z.string()) }`) — matches the MCP SDK's registerTool input schema. */
  inputSchema: Record<string, z.ZodTypeAny>;
  /**
   * Names of args that carry a handle from an earlier tool result instead of inline data.
   * The dispatcher swaps each handle for its stored rows before calling the task.
   */
  handleArgs?: string[];
}

export type TaskFn = (ctx: Context, args: any) => any;

export interface RegisteredTaskEntry {
  spec: TaskSpec;
  fn: TaskFn;
}

const tasks: RegisteredTaskEntry[] = [];

export function registerTask(spec: TaskSpec, fn: TaskFn): void {
  tasks.push({ spec, fn });
}

export function getRegisteredTasks(): RegisteredTaskEntry[] {
  return tasks;
}
