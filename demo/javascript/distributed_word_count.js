/**
 * Distributed word count demo using the atomic JavaScript API.
 *
 * JS closures are serialized as source strings (via Function.prototype.toString)
 * and executed on workers via their embedded QuickJS runtime — no binary
 * deployment of your code is required. The `atomic-worker` pre-built binary
 * handles all JS UDF execution.
 *
 * Setup:
 *   # 1. Build the worker binary
 *   cargo build --release -p atomic-worker
 *
 *   # 2. Start a worker in a separate terminal
 *   RUST_LOG=info ./target/release/atomic-worker --worker --port 10001
 *
 *   # 3. Run this driver (from the repo root)
 *   VEGA_DEPLOYMENT_MODE=distributed VEGA_LOCAL_IP=127.0.0.1 \
 *     node demo/javascript/distributed_word_count.js
 *
 * Prerequisites:
 *   cargo build --release -p atomic-js atomic-worker
 */

const { Context } = require('../../crates/atomic-js');

const ctx = new Context();

const lines = [
  "the quick brown fox jumps over the lazy dog",
  "the fox and the dog are friends",
  "quick brown foxes jump over lazy dogs",
  "atomic distributed compute engine written in rust",
];

const wordCounts = ctx.parallelize(lines, 2)
  .flatMap(line => line.split(/\s+/).map(w => [w, 1]))
  .reduceByKey((a, b) => a + b)
  .collect();

// Sort by count descending and print top 10
wordCounts
  .sort((a, b) => b[1] - a[1])
  .slice(0, 10)
  .forEach(([word, count]) => {
    console.log(word.padEnd(20) + " " + count);
  });
