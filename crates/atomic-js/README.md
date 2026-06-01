# @atomic-compute/js

Spark-like distributed RDD API for **Node.js** and **TypeScript**, backed by the Atomic Rust
engine via [napi-rs](https://napi.rs).

- **Full TypeScript support** — generic `RDD<T>` with end-to-end type inference
- **Local mode** — eager execution on Node.js threads; no worker setup required
- **Distributed mode** — lazy pipeline dispatch to Rust workers over TCP
- **V8 runtime on workers** — JavaScript functions are serialized as source strings and executed
  inside a V8 runtime embedded in the worker binary

> **Recommended runner for TypeScript jobs:** [`tsx`](https://tsx.is) or `ts-node`.
> Both transpile TypeScript before functions are captured, so TypeScript syntax never reaches
> workers — type-annotated functions work transparently.

---

## Build

```bash
# Recommended: build via napi CLI (produces atomic_js.node next to this file)
cd crates/atomic-js
npm install
npm run build

# Alternative: build via cargo (produces atomic_js.node in target/release/)
cargo build --release -p atomic-js
```

---

## Quick Start — TypeScript

```typescript
import { Context } from '@atomic-compute/js';

const ctx = new Context();

const result = ctx
  .parallelize([1, 2, 3, 4, 5, 6, 7, 8], 4)  // RDD<number>
  .filter((x: number) => x % 2 === 0)          // RDD<number>
  .map((x: number) => x * x)                   // RDD<number>
  .collect();                                   // number[]

console.log(result);  // [4, 16, 36, 64]
```

Run with `tsx` (no compile step needed):

```bash
npx tsx job.ts
```

---

## Quick Start — JavaScript

```javascript
const { Context } = require('@atomic-compute/js');

const ctx = new Context();
const result = ctx
  .parallelize([1, 2, 3, 4, 5, 6, 7, 8], 4)
  .filter(x => x % 2 === 0)
  .map(x => x * x)
  .collect();

console.log(result);  // [4, 16, 36, 64]
```

```bash
node job.js
```

---

## Spark-like Job Submission (TypeScript)

Write a self-contained TypeScript file and run it with `tsx` — the same mental model as
`spark-submit` for Python/Scala.

```typescript
// word_count.ts
import { Context } from '@atomic-compute/js';

const ctx = new Context();

const wordCounts = ctx
  .textFile('data.txt')                                           // RDD<string>
  .flatMap((line: string) => line.split(/\s+/))                  // RDD<string>
  .map((word: string): [string, number] => [word, 1])            // RDD<[string, number]>
  .reduceByKey((a: number, b: number) => a + b)                  // RDD<[string, number]>
  .collect();                                                     // [string, number][]

wordCounts
  .sort(([a], [b]) => a.localeCompare(b))
  .forEach(([word, count]) => console.log(`${word.padEnd(20)} ${count}`));
```

**Local execution:**

```bash
npx tsx word_count.ts
```

**Distributed execution** (same code, different environment):

```bash
# Start workers first (each runs the same atomic-worker binary)
RUST_LOG=info ./target/release/atomic-worker --worker --port 10001 &
RUST_LOG=info ./target/release/atomic-worker --worker --port 10002 &

# Submit the job
ATOMIC_DEPLOYMENT_MODE=distributed \
ATOMIC_WORKERS=127.0.0.1:10001,127.0.0.1:10002 \
npx tsx word_count.ts
```

TypeScript functions are transpiled to JavaScript by `tsx` before `.map()` / `.filter()` etc.
are called. The JavaScript source string — not TypeScript — is what the worker receives.

---

## TypeScript Type Inference

The generic `RDD<T>` class tracks element types through the full pipeline:

```typescript
const ctx = new Context();

// Type flows through transformations
const lines: RDD<string>         = ctx.textFile('data.txt');
const words: RDD<string>         = lines.flatMap(l => l.split(' '));
const pairs: RDD<[string,number]>= words.map(w => [w, 1] as [string, number]);
const counts: [string, number][] = pairs
  .reduceByKey((a, b) => a + b)
  .collect();

// Pair-specific operations are typed with conditional this-types
const rdd: RDD<readonly [string, number]> = ctx.parallelize([['a', 1], ['b', 2]] as const);
rdd.mapValues(v => v * 10)            // RDD<readonly [string, number]>
rdd.groupByKey()                      // RDD<readonly [string, readonly number[]]>
rdd.reduceByKey((a, b) => a + b)      // RDD<readonly [string, number]>
```

---

## API Reference

### `Context`

| Method | Returns | Description |
|--------|---------|-------------|
| `new Context(parallelism?)` | `Context` | Create a context. `parallelism` defaults to CPU count. |
| `.parallelize(data, partitions?)` | `RDD<T>` | Distribute a JS/TS array as an RDD |
| `.textFile(path)` | `RDD<string>` | Read a text file as one string per line |
| `.range(start, end, step?, partitions?)` | `RDD<number>` | Integer range `[start, end)` |
| `.defaultParallelism()` | `number` | Default partition count |

### `Context` — Additional methods (Phase 4)

| Method | Returns | Description |
| --- | --- | --- |
| `.broadcast(value)` | `BroadcastVar` | Broadcast a JSON-serializable value to all tasks |
| `.accumulator(zero)` | `Accumulator` | Create a counter/accumulator starting at `zero` |
| `.stop()` | `void` | Graceful shutdown |

### `BroadcastVar`

```typescript
const bv = ctx.broadcast({ threshold: 10 });
rdd.filter(x => x > bv.value().threshold);
```

| Method | Returns | Description |
| --- | --- | --- |
| `.value()` | `unknown` | Return the broadcast value |

### `Accumulator`

```typescript
const acc = ctx.accumulator(0);
rdd.forEach(x => acc.add(x));
console.log(acc.value()); // sum of all elements
```

| Method | Returns | Description |
| --- | --- | --- |
| `.add(delta)` | `void` | Add delta (number, array append, or string concat) |
| `.value()` | `unknown` | Return the current accumulated value |
| `.reset()` | `void` | Reset to the initial zero value |

---

### `RDD<T>` — Transformations

All transformations return a new `RDD`. In local mode they execute eagerly; in distributed mode
they are staged and dispatched together when an action is called.

| Method | Returns | Description |
| --- | --- | --- |
| `.map<U>(f)` | `RDD<U>` | Apply `f` to each element |
| `.filter(predicate)` | `RDD<T>` | Keep elements where `predicate(x)` is truthy |
| `.flatMap<U>(f)` | `RDD<U>` | Apply `f` and flatten; `f` must return an array |
| `.mapValues<K,V,W>(f)` | `RDD<[K,W]>` | Map over value in `[key, value]` pairs |
| `.flatMapValues<K,V,W>(f)` | `RDD<[K,W]>` | FlatMap over value in `[key, value]` pairs |
| `.keyBy<K>(f)` | `RDD<[K, T]>` | Produce `[f(x), x]` pairs |
| `.groupBy<K>(f)` | `RDD<[K, T[]]>` | Group by `f(element)` |
| `.groupByKey<K,V>()` | `RDD<[K, V[]]>` | Group `[key, value]` pairs by key |
| `.reduceByKey<K,V>(f)` | `RDD<[K, V]>` | Aggregate values per key with `f(acc, v)` |
| `.join(other)` | `RDD<[K,[V1,V2]]>` | Inner join on pair RDDs |
| `.leftOuterJoin(other)` | `RDD<[K,[V1,V2\|null]]>` | Left outer join |
| `.sortByKey(ascending?)` | `RDD<T>` | Sort pair RDD by key |
| `.sortBy(keyFn, ascending?)` | `RDD<T>` | Sort by key function |
| `.glom()` | `T[][]` | Collect each partition as a sublist |
| `.union(other)` | `RDD<T>` | Concatenate two RDDs |
| `.zip<U>(other)` | `RDD<[T, U]>` | Zip two equal-length RDDs element-wise |
| `.cartesian<U>(other)` | `RDD<[T, U]>` | Cartesian product |
| `.distinct()` | `RDD<T>` | Remove duplicates |
| `.subtract(other)` | `RDD<T>` | Elements in self but not in other |
| `.intersection(other)` | `RDD<T>` | Elements in both RDDs |
| `.mapPartitions(f)` | `RDD<U>` | Apply `f` to each partition array |
| `.coalesce(n)` | `RDD<T>` | Reduce to `n` logical partitions |
| `.repartition(n)` | `RDD<T>` | Change partition count |
| `.cache()` / `.persist()` / `.unpersist()` | `RDD<T>` | Cache hints (no-op in local mode) |
| `.checkpoint(path)` | `RDD<T>` | Write to `{path}/checkpoint.json`; truncate lineage |

### `RDD<T>` — Actions

| Method | Returns | Description |
| --- | --- | --- |
| `.collect()` | `T[]` | Return all elements as an array |
| `.count()` | `number` | Count all elements |
| `.first()` | `T` | Return the first element |
| `.take(n)` | `T[]` | Return the first `n` elements |
| `.reduce(f)` | `T` | Aggregate with `f(acc, x)` |
| `.fold(zero, f)` | `T` | Aggregate with an initial value |
| `.aggregate(zero, seqFn, combFn)` | `T` | Two-phase aggregation |
| `.forEach(f)` | `void` | Call `f(x)` for each element |
| `.forEachPartition(f)` | `void` | Call `f(partition)` for each partition |
| `.countByValue()` | `Record<string, number>` | Count by value |
| `.countByKey()` | `Record<string, number>` | Count by key for pair RDDs |
| `.isEmtpy()` | `boolean` | `true` if the RDD is empty |
| `.max(comparator?)` / `.min(comparator?)` | `T` | Max/min element |
| `.top(n, comparator?)` / `.takeOrdered(n, comparator?)` | `T[]` | Top/bottom n |
| `.saveAsTextFile(path)` | `void` | Write elements to a text file |

### `RDD<T>` — Properties

| Property | Type | Description |
| --- | --- | --- |
| `.numPartitions` | `number` | Number of logical partitions |

---

### Graph

```typescript
import { Graph } from '@atomic-compute/js';

const g = new Graph(
  [[1, 1.0], [2, 1.0], [3, 1.0]],          // [id, weight]
  [[1, 2, 1.0], [2, 3, 1.0], [3, 1, 1.0]], // [src, dst, weight]
);

const ranks = g.pageRank(20, 0.15);
// { '1': 0.33, '2': 0.33, '3': 0.33 }
```

| Method | Returns | Description |
| --- | --- | --- |
| `new Graph(vertices, edges)` | `Graph` | Construct from `[[id,w]]` and `[[src,dst,w]]` |
| `.numVertices()` | `number` | Vertex count |
| `.numEdges()` | `number` | Edge count |
| `.pageRank(numIter, resetProb)` | `Record<string, number>` | PageRank score per vertex |
| `.connectedComponents()` | `Record<string, number>` | Component label per vertex |
| `.stronglyConnectedComponents()` | `Record<string, number>` | SCC label per vertex |
| `.labelPropagation(maxIter)` | `Record<string, number>` | Community label per vertex |
| `.triangleCount()` | `Record<string, number>` | Triangle count per vertex |
| `.shortestPath(landmarks)` | `Record<string, Record<string, number>>` | Distance from each landmark |

> **Note:** Vertex IDs are returned as string keys in JS objects (e.g. `"1"`, `"2"`).

---

### Streaming

```typescript
import { StreamingContext } from '@atomic-compute/js';

const ssc = new StreamingContext(0.1);
const [stream, queue] = ssc.testQueueStream();

const results: unknown[] = [];
ssc.foreachRdd(
  stream.map((x: number) => x * 2),
  (batch: unknown[]) => results.push(...batch),
);

queue.push([1, 2, 3]);
ssc.runOneBatch();
// results === [2, 4, 6]
```

**`StreamingContext` methods:**

| Method | Description |
| --- | --- |
| `new StreamingContext(batchSecs)` | Create a context with the given batch interval |
| `.socketTextStream(host, port)` | Read text lines from a TCP socket |
| `.textFileStream(directory)` | Watch a directory for new text files |
| `.testQueueStream()` | Returns `[DStream, BatchQueue]` for testing |
| `.testPairQueueStream()` | Same, but stream is marked as a pair stream |
| `.foreachRdd(stream, f)` | Register output op: `f(batch)` called each tick |
| `.runOneBatch()` | Execute one batch tick synchronously |
| `.start()` | Start the batch loop |
| `.stop()` | Signal the batch loop to stop |
| `.awaitTerminationOrTimeout(secs)` | Block until stopped or timeout |

**`DStream` transforms:**

| Method | Description |
| --- | --- |
| `.map(f)` | Apply `f` to each element |
| `.filter(f)` | Keep elements where `f` is truthy |
| `.flatMap(f)` | Apply `f` and flatten |
| `.reduceByKey(f)` | Aggregate `[k, v]` pairs per key per batch |
| `.groupByKey()` | Group `[k, v]` pairs → `[k, values[]]` per batch |
| `.join(other)` | Inner join two pair DStreams per batch |
| `.leftOuterJoin(other)` | Left outer join per batch |
| `.updateStateByKey(f)` | Stateful: `f(newValues, oldState) → newState` |
| `.mapValues(f)` | Apply `f` to value in `[k, v]` elements |

---

## Distributed Mode

The same TypeScript code runs in local or distributed mode based on environment variables:

```bash
# Local mode (default)
npx tsx job.ts

# Distributed mode
export ATOMIC_DEPLOYMENT_MODE=distributed
export ATOMIC_WORKERS=host1:10001,host2:10001   # comma-separated worker addresses
npx tsx job.ts
```

Workers run the `atomic-worker` binary (built with `cargo build --release -p atomic-worker`):

```bash
# On each worker machine
RUST_LOG=info ./atomic-worker --worker --port 10001
```

### How distributed dispatch works

1. Driver stages pipeline ops lazily (no data moves until an action)
2. On `collect()` / `count()` / etc., the driver sends one `TaskEnvelope` per partition over TCP
3. Each envelope carries: the JS function source strings + the JSON-encoded partition data
4. Workers execute the functions in V8, serialize results as JSON, and return them
5. Driver merges all partition results and returns the final array

### Cross-compilation and deployment

Use `atomic-cli` to cross-compile and ship the worker binary to remote machines:

```bash
cargo install --path crates/atomic-cli

atomic build --target x86_64-unknown-linux-musl
atomic ship --workers user@host1,user@host2
```

---

## Runtime Notes

- **V8 runtime** — Workers run JavaScript in a V8 engine embedded via `deno_core`. ES2020 is
  fully supported. Node.js built-in modules (`fs`, `path`, `require`, etc.) are not available
  inside worker functions because workers run in an isolated V8 context, not Node.js.
- **Function serialization** — Functions are captured via `Function.prototype.toString()`. The
  source string is sent to workers. Closures that capture variables from outer scopes work when
  running in local mode, but the captured values are not serialized for distributed execution.
  Pass configuration by injecting it into the data or using `[key, value]` pairs.
- **TypeScript compatibility** — When using `tsx`, `ts-node`, or compiling with `tsc`, TypeScript
  is transpiled to JavaScript before functions are captured. The V8 worker runtime only receives
  valid JavaScript, so TypeScript type annotations have no effect on workers.
- **JSON wire format** — All element data is JSON-encoded. Elements must be JSON-serializable:
  numbers, strings, booleans, arrays, and plain objects. `Date`, `Map`, `Set`, `undefined`,
  `BigInt`, and functions cannot be stored in an RDD.

---

## Examples

See [`demo/javascript/`](../../demo/javascript/) for runnable examples:

```bash
# Word count (local)
node demo/javascript/local_word_count.js

# Full RDD API showcase
node demo/javascript/map_reduce.js

# Distributed word count (requires atomic-worker running)
ATOMIC_DEPLOYMENT_MODE=distributed \
ATOMIC_WORKERS=127.0.0.1:10001 \
node demo/javascript/distributed_word_count.js
```
