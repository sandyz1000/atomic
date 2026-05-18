/**
 * Distributed word count demo using the Atomic RDD API (TypeScript).
 *
 * JavaScript/TypeScript closures are serialised as source strings
 * (via Function.prototype.toString) and executed on workers via their
 * embedded V8 (deno_core) runtime — no binary deployment of your code required.
 * The `atomic-worker` binary handles all JS UDF execution.
 *
 * Setup:
 *   cd examples/ts-demo && npm install
 *
 *   # 1. Build the worker binary
 *   cargo build --release -p atomic-worker --manifest-path ../../Cargo.toml
 *
 *   # 2. Start a worker in a separate terminal
 *   RUST_LOG=info ../../target/release/atomic-worker --worker --port 10001
 *
 *   # 3. Run this driver
 *   npm run distributed
 *
 * Run (local fallback — no workers needed):
 *   npm run distributed
 */

import Atomic = require('@atomic-compute/js');

const ctx = new Atomic.Context();

const lines: string[] = [
  'the quick brown fox jumps over the lazy dog',
  'the fox and the dog are friends',
  'quick brown foxes jump over lazy dogs',
  'atomic distributed compute engine written in rust',
];

const wordCounts: Array<readonly [string, number]> = ctx
  .parallelize(lines, 2)
  .flatMap((line: string): [string, number][] =>
    line.split(/\s+/).map((word): [string, number] => [word, 1]),
  )
  .reduceByKey((a: number, b: number): number => a + b)
  .collect();

// Sort by count descending and print top 10.
const top10 = [...wordCounts]
  .sort((a, b) => b[1] - a[1])
  .slice(0, 10);

console.log('word count');
console.log('----------------------------');
for (const [word, count] of top10) {
  console.log(word.padEnd(20) + ' ' + count);
}
