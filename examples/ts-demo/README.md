# TypeScript / JavaScript Examples

Spark-like jobs written in TypeScript using the `@atomic-compute/js` native Node.js module,
backed by the Atomic Rust engine via napi-rs.

## Prerequisites

- Node.js ≥ 18
- Rust toolchain (stable)
- The native `.node` binary built from `crates/atomic-js`

## Setup

```bash
cd examples/ts-demo

# 1. Install npm dependencies (TypeScript, tsx, the atomic-js local package)
npm install

# 2. Build the native Rust module (produces target/release/atomic_js.node)
npm run build:native
```

## Running examples

```bash
# Word count (local mode)
npm run wordcount

# Full API showcase — every transformation and action
npm run mapreduce

# Distributed word count (see distributed section below)
npm run distributed

# Type check all TypeScript sources
npm run type-check
```

## Examples

### `local_word_count.ts` — Word count (local)

Classic word count using typed RDD transformations. Everything runs in-process.

```text
Expected output:

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

### `map_reduce.ts` — Full API showcase

Demonstrates every transformation and action available on `RDD<T>`:
`map`, `filter`, `flatMap`, `mapValues`, `keyBy`, `groupByKey`, `reduceByKey`,
`union`, `zip`, `distinct`, `range`, `aggregate`, `take`, `first`, `count`,
`min`, `max`, `top`, `isEmpty`, `numPartitions`.

### `distributed_word_count.ts` — Distributed word count

TypeScript/JavaScript closures are serialised as source strings
(`Function.prototype.toString`) and executed on workers via their embedded
V8 runtime (deno_core). The `atomic-worker` binary handles all JS task
execution — no binary deployment of your code is required.

```bash
# Build the worker binary
cargo build --release -p atomic-worker --manifest-path ../../Cargo.toml

# Start a worker in a separate terminal
RUST_LOG=info ../../target/release/atomic-worker --worker --port 10001

# Run the driver
npm run distributed
```

## API overview

```typescript
import Atomic = require('@atomic-compute/js');

const ctx = new Atomic.Context();           // local mode, parallelism = CPU count
const ctx2 = new Atomic.Context(8);        // 8 default partitions

// Create RDDs
ctx.parallelize([1, 2, 3])                 // RDD<number>
ctx.parallelize(['a', 'b', 'c'], 2)        // RDD<string>, 2 partitions
ctx.range(0, 100, 1, 4)                    // RDD<number> — 0..99 step 1, 4 partitions
ctx.textFile('/path/to/file.txt')          // RDD<string> — one element per line

// Transformations (return a new RDD, typed)
rdd.map((x: number) => x * 2)             // RDD<number>
rdd.filter((x: number) => x > 0)          // RDD<number>
rdd.flatMap((s: string) => s.split(' '))   // RDD<string>
rdd.keyBy((s: string) => s[0])            // RDD<readonly [string, string]>
rdd.mapValues((v: number) => v * 2)       // RDD<readonly [K, number]>
rdd.groupByKey()                           // RDD<readonly [K, readonly V[]]>
rdd.reduceByKey((a, b) => a + b)           // RDD<readonly [K, V]>
rdd.union(other)                           // concatenate two RDDs
rdd.zip(other)                             // element-wise [a, b] pairs
rdd.distinct()                             // remove duplicates
rdd.coalesce(n)                            // reduce to n partitions
rdd.repartition(n)                         // change partition count

// Actions (eager, trigger execution)
rdd.collect()                              // T[]
rdd.count()                                // number
rdd.first()                                // T
rdd.take(n)                                // T[]
rdd.reduce((a, b) => a + b)               // T
rdd.fold(0, (a, b) => a + b)              // T
rdd.min() / rdd.max()                      // T
rdd.top(n)                                 // T[]
rdd.isEmpty()                              // boolean
rdd.numPartitions                          // number (getter)
```

## Notes

- All element types must be JSON-serialisable (numbers, strings, booleans, arrays, plain objects).
- Lambda functions are captured via `Function.prototype.toString()` for distributed dispatch.
- Workers execute serialised lambdas via their embedded V8 (ES2020) runtime (deno_core).
- For distributed jobs: only the `atomic-worker` binary needs to be on the workers; your TypeScript
  code stays on the driver.
