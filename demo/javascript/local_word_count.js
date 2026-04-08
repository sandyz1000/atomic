/**
 * Local word count job using the atomic JavaScript API.
 *
 * Prerequisites:
 *   cargo build --release -p atomic-js
 *
 * Run:
 *   node demo/javascript/local_word_count.js
 */

const { Context } = require('../../crates/atomic-js');

const ctx = new Context();

const lines = ctx.parallelize([
  "hello world hello",
  "world foo bar hello",
  "atomic is fast and atomic is small",
  "fast systems need fast tools",
]);

// Classic Spark word count:
//   split lines → emit [word, 1] pairs → aggregate by key
const wordCounts = lines
  .flatMap(line => line.split(/\s+/).map(w => [w, 1]))
  .reduceByKey((a, b) => a + b)
  .collect();

// Sort alphabetically and print
wordCounts.sort((a, b) => a[0].localeCompare(b[0]));

console.log("word                 count");
console.log("----------------------------");
for (const [word, count] of wordCounts) {
  console.log(word.padEnd(20) + " " + count);
}
