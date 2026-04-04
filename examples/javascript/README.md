# JavaScript Examples

Spark-like jobs written in JavaScript and run with the `atomic-js` CLI,
which embeds QuickJS (ES2020) via `rquickjs`.

## Setup

```bash
cargo build -p atomic-js --release
# Binary: ./target/release/atomic-js
```

## Examples

### `local_word_count.js` — Word count

```bash
./target/release/atomic-js examples/javascript/local_word_count.js
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
./target/release/atomic-js examples/javascript/map_reduce.js
```

## API overview

```javascript
// Create RDDs
atomic.parallelize([1, 2, 3])          // from array
atomic.range(start, end, step)         // numeric range

// Transformations (return a new Rdd)
rdd.map(x => ...)                      // transform each element
rdd.filter(x => ...)                   // keep matching elements
rdd.flat_map(x => [...])              // f returns Array; results are flattened
rdd.key_by(x => ...)                   // produce [key, x]
rdd.map_values(([k, v]) => ...)        // for [k, v]: produce [k, f(v)]
rdd.flat_map_values(([k, v]) => ...)   // for [k, v]: f returns Array of values
rdd.group_by(x => ...)                 // group by function
rdd.group_by_key()                     // group [k, v] pairs by k
rdd.reduce_by_key((a, b) => ...)       // aggregate values per key
rdd.union(other)                       // concatenate two Rdds
rdd.zip(other)                         // element-wise [a, b] pairs
rdd.cartesian(other)                   // all [a, b] combinations
rdd.coalesce(n)                        // reduce to n partitions
rdd.repartition(n)                     // change partition count

// Actions (eager, return a value)
rdd.collect()                          // all elements as Array
rdd.count()                            // number of elements
rdd.first()                            // first element (throws if empty)
rdd.take(n)                            // first n elements
rdd.reduce((a, b) => ...)             // fold
rdd.fold(zero, (a, b) => ...)         // fold with initial value
rdd.num_partitions                     // getter — number of partitions

// Output
atomic.print(...args)                  // console.log equivalent
```

## Notes

- QuickJS implements **ES2020** — no Node.js builtins (`require`, `process`, `fs`)
- Scripts must be self-contained
- Rdd operations are **eager** (not lazy) — each call runs immediately
- `reduce_by_key` uses string/number keys; complex key types are stringified
