/**
 * Map-reduce pipeline example using the atomic JavaScript API.
 *
 * Demonstrates the full RDD transformation API: map, filter, flat_map,
 * key_by, group_by_key, reduce_by_key, union, zip, cartesian, and actions.
 *
 * Run with:
 *   cargo build -p atomic-js --release
 *   ./target/release/atomic-js examples/javascript/map_reduce.js
 */

// ── map / filter / collect ──────────────────────────────────────────────────
const doubled = atomic.parallelize([1, 2, 3, 4, 5])
    .map(x => x * 2)
    .filter(x => x > 4)
    .collect();
atomic.print("doubled (>4):", doubled);   // [6, 8, 10]

// ── flat_map ────────────────────────────────────────────────────────────────
const chars = atomic.parallelize(["ab", "cd", "ef"])
    .flat_map(s => s.split(""))
    .collect();
atomic.print("chars:", chars);            // ["a", "b", "c", "d", "e", "f"]

// ── reduce ──────────────────────────────────────────────────────────────────
const total = atomic.parallelize([1, 2, 3, 4, 5])
    .reduce((a, b) => a + b);
atomic.print("sum:", total);              // 15

// ── fold (with initial value) ───────────────────────────────────────────────
const product = atomic.parallelize([1, 2, 3, 4, 5])
    .fold(1, (a, b) => a * b);
atomic.print("product:", product);        // 120

// ── key_by / group_by_key ───────────────────────────────────────────────────
const grouped = atomic.parallelize(["apple", "banana", "avocado", "blueberry"])
    .key_by(s => s[0])                   // first letter as key
    .group_by_key()
    .collect();
atomic.print("grouped by first letter:");
for (const [letter, words] of grouped.sort((a, b) => a[0].localeCompare(b[0]))) {
    atomic.print(" ", letter, "->", words);
}

// ── reduce_by_key ───────────────────────────────────────────────────────────
const counts = atomic.parallelize([
    ["a", 1], ["b", 2], ["a", 3], ["b", 4], ["c", 5],
])
    .reduce_by_key((a, b) => a + b)
    .collect();
atomic.print("counts:", counts.sort((x, y) => x[0].localeCompare(y[0])));

// ── union ────────────────────────────────────────────────────────────────────
const rdd1 = atomic.parallelize([1, 2, 3]);
const rdd2 = atomic.parallelize([4, 5, 6]);
const merged = rdd1.union(rdd2).collect();
atomic.print("union:", merged);           // [1, 2, 3, 4, 5, 6]

// ── zip ──────────────────────────────────────────────────────────────────────
const zipped = atomic.parallelize(["a", "b", "c"])
    .zip(atomic.parallelize([1, 2, 3]))
    .collect();
atomic.print("zipped:", zipped);          // [["a",1],["b",2],["c",3]]

// ── range ────────────────────────────────────────────────────────────────────
const rangeSum = atomic.range(1, 11, 1)  // 1..10 inclusive step 1
    .reduce((a, b) => a + b);
atomic.print("sum 1..10:", rangeSum);     // 55

// ── take / first ─────────────────────────────────────────────────────────────
const first3 = atomic.parallelize([10, 20, 30, 40, 50]).take(3);
atomic.print("take(3):", first3);         // [10, 20, 30]

const head = atomic.parallelize([99, 1, 2]).first();
atomic.print("first:", head);             // 99

// ── count ─────────────────────────────────────────────────────────────────────
const n = atomic.parallelize([1, 2, 3, 4, 5]).count();
atomic.print("count:", n);                // 5
