# atomic-js

JavaScript bindings for the Atomic distributed compute engine — a Spark-like API for local data processing using an embedded QuickJS runtime.

## Build

```bash
cargo build -p atomic-js --release
# binary: ./target/release/atomic-js
```

### npm wrapper

```bash
cd crates/atomic-js
npm install      # triggers `cargo build --release` via postinstall
```

## Run a script

```bash
./target/release/atomic-js path/to/script.js
```

## Quick start

```javascript
// atomic is a global — no import or require needed

const result = atomic.parallelize([1, 2, 3, 4])
    .map(x => x * 2)
    .filter(x => x > 4)
    .collect();

atomic.print(result);  // [6, 8]
```

## Running demos

```bash
cargo build -p atomic-js --release

# Word count
./target/release/atomic-js demo/javascript/local_word_count.js

# Map-reduce
./target/release/atomic-js demo/javascript/map_reduce.js
```

## API

### Globals

| Name | Description |
|---|---|
| `atomic.parallelize(array, numPartitions?)` | Create an RDD from a JS array |
| `atomic.print(...args)` | Print to stdout (use instead of `console.log`) |

### Transformations (return a new Rdd)

| Method | Description |
|---|---|
| `map(f)` | Apply `f` to each element |
| `filter(f)` | Keep elements where `f` returns truthy |
| `flat_map(f)` | Apply `f` and flatten; `f` must return an Array |
| `map_values(f)` | Apply `f` to value in `[key, value]` pairs |
| `flat_map_values(f)` | Flatten values in `[key, value]` pairs |
| `key_by(f)` | Produce `[f(x), x]` pairs |
| `group_by(f)` | Group by `f(element)` → `[key, [elements]]` pairs |
| `group_by_key()` | Group `[key, value]` pairs → `[key, [values]]` |
| `reduce_by_key(f)` | Aggregate values per key with `f(acc, v)` |
| `union(other)` | Concatenate two Rdds |
| `zip(other)` | Zip two equal-length Rdds into `[a, b]` pairs |
| `cartesian(other)` | Cartesian product as `[a, b]` pairs |
| `coalesce(n)` / `repartition(n)` | Change logical partition count |

### Actions (return a value)

| Method | Description |
|---|---|
| `collect()` | Return all elements as a JS Array |
| `count()` | Return number of elements |
| `first()` | Return the first element |
| `take(n)` | Return the first `n` elements |
| `reduce(f)` | Aggregate all elements with `f(acc, x)` |
| `fold(zero, f)` | Aggregate with an initial value |

### Properties

| Property | Description |
|---|---|
| `rdd.num_partitions` | Number of logical partitions |

## Notes

- Runtime is [QuickJS](https://bellard.org/quickjs/) — ES2020, no Node.js builtins (`fs`, `path`, `require`, etc.)
- All operations are **eager** — transformations execute immediately, not lazily
- Keys in `reduce_by_key` / `group_by_key` must be strings, numbers, or booleans
