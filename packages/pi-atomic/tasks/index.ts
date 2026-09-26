import { z } from "zod";
import { registerTask } from "../src/registry";

registerTask(
  {
    name: "word_count",
    description: "Count word occurrences across an array of input lines using a distributed map-reduce pipeline.",
    inputSchema: {
      lines: z.array(z.string()).describe("Lines of text to word-count"),
    },
  },
  (ctx, args: { lines: string[] }) => {
    const counts = ctx
      .parallelize(args.lines)
      .flatMap((line: string) => line.split(/\s+/).filter(Boolean))
      .map((word: string): [string, number] => [word, 1])
      .reduceByKey((a: number, b: number) => a + b)
      .collect();
    return Object.fromEntries(counts);
  },
);

registerTask(
  {
    name: "count_by",
    description:
      "Count rows from an earlier tool result, grouped by one of their columns. Takes the handle returned by atomic_sql or by another task.",
    inputSchema: {
      rows: z.string().describe("Handle to rows produced by an earlier tool call"),
      column: z.string().describe("Column to group by"),
    },
    handleArgs: ["rows"],
  },
  (ctx, args: { rows: Record<string, unknown>[]; column: string }) => {
    const counts = ctx
      .parallelize(args.rows)
      .map((row: Record<string, unknown>): [string, number] => [String(row[args.column]), 1])
      .reduceByKey((a: number, b: number) => a + b)
      .collect();
    return Object.fromEntries(counts);
  },
);
