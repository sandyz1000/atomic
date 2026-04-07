# JavaScript Examples

Spark-like jobs written in JavaScript using the `@atomic-compute/js` native Node.js module,
backed by the Atomic Rust engine via napi-rs.

## Setup

```bash
# Build the native Node.js module
cargo build --release -p atomic-js

# Run examples with Node.js (>= 18)
node demo/javascript/local_word_count.js
```

## Examples

### `local_word_count.js` — Word count (local)

Classic word count using RDD transformations. Everything runs in-process.

```bash
node demo/javascript/local_word_count.js
```

Expected output:
```
word                 count
----------------------------
and                  1
atomic               2
bar                  1
fast                 3
foo                  1
hello                3
is                   2
need                 1
small                1
systems              1
tools                1
world                2
```

### `map_reduce.js` — Full API showcase

Demonstrates every transformation and action available on `Rdd`:

```bash
node demo/javascript/map_reduce.js
```

### `distributed_word_count.js` — Distributed word count

JS closures are serialized as source strings (via `Function.prototype.toString`)
and executed on workers via their embedded QuickJS runtime. The `atomic-worker`
pre-built binary handles all JS UDF execution — no binary deployment of your
code is required.

```bash
# Start a worker in a separate terminal
RUST_LOG=info ./target/release/atomic-worker --worker --port 10001

# Run the driver
VEGA_DEPLOYMENT_MODE=distributed VEGA_LOCAL_IP=127.0.0.1 \
  node demo/javascript/distributed_word_count.js
```

## API overview

```javascript
const { Context } = require('@atomic-compute/js');
// or (dev): require('../../crates/atomic-js')

const ctx = new Context();

// Create RDDs
ctx.parallelize([1, 2, 3])          // from array
ctx.range(start, end, step)          // numeric range
ctx.textFile('/path/to/file.txt')    // one RDD element per line

// Transformations (return a new Rdd)
rdd.map(x => ...)                    // transform each element
rdd.filter(x => ...)                 // keep matching elements
rdd.flatMap(x => [...])              // f returns Array; results are flattened
rdd.keyBy(x => ...)                  // produce [key, x]
rdd.mapValues(([k, v]) => ...)       // for [k, v]: produce [k, f(v)]
rdd.flatMapValues(([k, v]) => ...)   // for [k, v]: f returns Array of values
rdd.groupBy(x => ...)                // group by function
rdd.groupByKey()                     // group [k, v] pairs by k
rdd.reduceByKey((a, b) => ...)       // aggregate values per key
rdd.union(other)                     // concatenate two Rdds
rdd.zip(other)                       // element-wise [a, b] pairs
rdd.cartesian(other)                 // all [a, b] combinations
rdd.coalesce(n)                      // reduce to n partitions
rdd.repartition(n)                   // change partition count

// Actions (eager, trigger execution)
rdd.collect()                        // all elements as Array
rdd.count()                          // number of elements
rdd.first()                          // first element (throws if empty)
rdd.take(n)                          // first n elements
rdd.reduce((a, b) => ...)            // fold left
rdd.fold(zero, (a, b) => ...)        // fold with initial value
rdd.numPartitions                    // getter — number of partitions
```

## Notes

- Elements are serialized as JSON values — all element types must be JSON-serializable
- Lambda functions are captured via `Function.prototype.toString()` for distributed dispatch
- Workers execute serialized lambdas via their embedded QuickJS (ES2020) runtime
- For distributed jobs: only the `atomic-worker` binary needs to be on the workers; your code stays on the driver
