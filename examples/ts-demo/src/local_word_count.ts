/**
 * Local word count using the Atomic RDD API (TypeScript).
 *
 * Demonstrates: parallelize → flatMap → reduceByKey → collect
 * Everything runs in-process on the local scheduler.
 *
 * Prerequisites:
 *   cd examples/ts-demo
 *   npm install
 *   npm run build:native    # builds atomic_js.node via cargo
 *
 * Run:
 *   npm run wordcount
 */

import Atomic = require('@atomic-compute/js');

const ctx = new Atomic.Context();

const lines: Atomic.RDD<string> = ctx.parallelize([
  'hello world hello',
  'world foo bar hello',
  'atomic is fast and atomic is small',
  'fast systems need fast tools',
]);

// Split each line into words and pair each word with 1, then reduce by key.
const wordCounts: Array<readonly [string, number]> = lines
  .flatMap((line: string): [string, number][] =>
    line.split(/\s+/).map((word): [string, number] => [word, 1]),
  )
  .reduceByKey((a: number, b: number): number => a + b)
  .collect();

wordCounts.sort((a, b) => a[0].localeCompare(b[0]));

console.log('word                 count');
console.log('----------------------------');
for (const [word, count] of wordCounts) {
  console.log(word.padEnd(20) + ' ' + count);
}
