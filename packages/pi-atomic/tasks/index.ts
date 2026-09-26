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
