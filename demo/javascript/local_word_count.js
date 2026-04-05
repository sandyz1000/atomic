/**
 * Local word count job using the atomic JavaScript API.
 *
 * Run with:
 *   cargo build -p atomic-js --release
 *   ./target/release/atomic-js demo/javascript/local_word_count.js
 */

const lines = atomic.parallelize([
    "hello world hello",
    "world foo bar hello",
    "atomic is fast and atomic is small",
    "fast systems need fast tools",
]);

// Classic Spark word count:
//   split lines → emit [word, 1] pairs → aggregate by key
const wordCounts = lines
    .flat_map(line => line.split(/\s+/))       // ["hello", "world", "hello", ...]
    .map(w => [w, 1])                          // [["hello", 1], ["world", 1], ...]
    .reduce_by_key((a, b) => a + b)            // [["hello", 3], ["world", 2], ...]
    .collect();

// Sort alphabetically and print
const sorted = wordCounts.sort((a, b) => a[0].localeCompare(b[0]));

atomic.print("word                 count");
atomic.print("----------------------------");
for (const [word, count] of sorted) {
    atomic.print(word.padEnd(20) + " " + count);
}
