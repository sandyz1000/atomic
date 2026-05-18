/**
 * Map-reduce pipeline example using the atomic JavaScript API.
 *
 * Demonstrates the full RDD transformation API: map, filter, flatMap,
 * keyBy, groupByKey, reduceByKey, union, zip, cartesian, and actions.
 *
 * Prerequisites:
 *   cargo build --release -p atomic-js
 *
 * Run:
 *   node demo/javascript/map_reduce.js
 */

const { Context } = require('../../crates/atomic-js');

const ctx = new Context();

// ── map / filter / collect ──────────────────────────────────────────────────
const doubled = ctx.parallelize([1, 2, 3, 4, 5])
  .map(x => x * 2)
  .filter(x => x > 4)
  .collect();
console.log("doubled (>4):", doubled);   // [6, 8, 10]

// ── flatMap ─────────────────────────────────────────────────────────────────
const chars = ctx.parallelize(["ab", "cd", "ef"])
  .flatMap(s => s.split(""))
  .collect();
console.log("chars:", chars);            // ["a", "b", "c", "d", "e", "f"]

// ── reduce ──────────────────────────────────────────────────────────────────
const total = ctx.parallelize([1, 2, 3, 4, 5])
  .reduce((a, b) => a + b);
console.log("sum:", total);              // 15

// ── fold (with initial value) ───────────────────────────────────────────────
const product = ctx.parallelize([1, 2, 3, 4, 5])
  .fold(1, (a, b) => a * b);
console.log("product:", product);        // 120

// ── keyBy / groupByKey ──────────────────────────────────────────────────────
const grouped = ctx.parallelize(["apple", "banana", "avocado", "blueberry"])
  .keyBy(s => s[0])                    // first letter as key
  .groupByKey()
  .collect();
console.log("grouped by first letter:");
grouped.sort((a, b) => a[0].localeCompare(b[0])).forEach(([letter, words]) => {
  console.log(" ", letter, "->", words);
});

// ── reduceByKey ─────────────────────────────────────────────────────────────
const counts = ctx.parallelize([
  ["a", 1], ["b", 2], ["a", 3], ["b", 4], ["c", 5],
])
  .reduceByKey((a, b) => a + b)
  .collect();
console.log("counts:", counts.sort((x, y) => x[0].localeCompare(y[0])));

// ── union ────────────────────────────────────────────────────────────────────
const rdd1 = ctx.parallelize([1, 2, 3]);
const rdd2 = ctx.parallelize([4, 5, 6]);
const merged = rdd1.union(rdd2).collect();
console.log("union:", merged);           // [1, 2, 3, 4, 5, 6]

// ── zip ──────────────────────────────────────────────────────────────────────
const zipped = ctx.parallelize(["a", "b", "c"])
  .zip(ctx.parallelize([1, 2, 3]))
  .collect();
console.log("zipped:", zipped);          // [["a",1],["b",2],["c",3]]

// ── range ────────────────────────────────────────────────────────────────────
const rangeSum = ctx.range(1, 11, 1)   // 1..10 inclusive step 1
  .reduce((a, b) => a + b);
console.log("sum 1..10:", rangeSum);    // 55

// ── take / first ─────────────────────────────────────────────────────────────
const first3 = ctx.parallelize([10, 20, 30, 40, 50]).take(3);
console.log("take(3):", first3);         // [10, 20, 30]

const head = ctx.parallelize([99, 1, 2]).first();
console.log("first:", head);             // 99

// ── count ─────────────────────────────────────────────────────────────────────
const n = ctx.parallelize([1, 2, 3, 4, 5]).count();
console.log("count:", n);                // 5
