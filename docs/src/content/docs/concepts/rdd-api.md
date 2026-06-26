---
title: The RDD API
description: Transformations, actions, and the _task family that runs locally and distributed.
---

An RDD (`TypedRdd<T>`) is a partitioned, immutable collection. Transformations
return a new RDD and build a lazy DAG; actions trigger execution and return a
result to the driver.

## The `_task` family

Narrow transformations and reductions have a `_task` variant that takes a
registered task instead of a closure. These run in-process in local mode and
dispatch to workers in distributed mode.

| Method | Trait | Description |
|---|---|---|
| `map_task(F)` | `UnaryTask<T, U>` | Apply `F` to each element |
| `filter_task(F)` | `UnaryTask<T, bool>` | Keep elements where `F` returns true |
| `flat_map_task(F)` | `UnaryTask<T, Vec<U>>` | Map each element to zero or more outputs |
| `fold_task(zero, F)` | `BinaryTask<T>` | Fold with an initial value |
| `reduce_task(F)` | `BinaryTask<T>` | Reduce pairwise |

Use these for any work that should run on workers. The closure variants
(`map`, `filter`, `flat_map`, `reduce`, `fold`) run only on the driver's local
scheduler and cannot dispatch to workers.

```rust
use atomic_compute::task;

#[task]
fn square(x: i32) -> i32 { x * x }

let result = ctx
    .parallelize_typed(vec![1, 2, 3, 4], 2)
    .map_task(Square)
    .collect()?;
```

## Actions

Actions run the DAG and return to the driver. In distributed mode they dispatch
the staged pipeline and aggregate the results.

| Action | Returns |
|---|---|
| `collect()` | All elements as a `Vec<T>` |
| `count()` | Number of elements |
| `take(n)` / `first()` | First `n` elements / first element |
| `reduce_task(F)` / `fold_task(z, F)` | A single reduced value |
| `aggregate(...)` | A folded accumulator |
| `for_each(F)` / `for_each_partition(F)` | Nothing; runs `F` for its side effects |
| `count_by_value()` | A map of value to occurrence count |
| `max()` / `min()` / `top(n)` / `take_ordered(n)` | Ordered selections |
| `is_empty()` | Whether the RDD has no elements |

## Pair operations

RDDs of `(K, V)` pairs support keyed operations. The keyed reductions and joins
are shuffle-based: they redistribute data by key across two stages rather than
collecting the full dataset to the driver. The reductions take a registered task,
like the narrow transforms — there is no closure form, so a job runs identically
in local and distributed mode.

| Method | Trait | Description |
|---|---|---|
| `reduce_by_key_task(B)` | `BinaryTask<V>` | Combine values per key |
| `fold_by_key_task(zero, B, n)` | `BinaryTask<V>` | Fold values per key from `zero` |
| `aggregate_by_key_task(L, M, n)` | `UnaryTask<V,C>` + `BinaryTask<C>` | Aggregate per key into a different accumulator type `C` (lift each value, then merge) |
| `reduce_by_key_locally_task(B)` | `BinaryTask<V>` | Reduce per key to a driver `HashMap` — no shuffle (map-side combine, then merge on driver) |
| `group_by_key()` | — | Collect all values per key |
| `join` / `left_outer_join` / `right_outer_join` / `full_outer_join` | — | Keyed joins |
| `cogroup(other)` | — | Group both sides by key |
| `sort_by_key()` / `sort_by_task(F, asc)` | `UnaryTask<T,(K,T)>` | Globally ordered output |
| `repartition_and_sort(P, asc)` | — | Partition by a registered `NamedPartitioner` and key-sort within each partition, one shuffle (secondary sort) |
| `repartition_shuffle(n)` | — | Redistribute elements across `n` partitions |

To transform values while keeping keys, use `map_task` / `flat_map_task` with a
pair-shaped task (`(K, V) -> (K, U)`); to key elements, map to `(K, T)` and then
`group_by_key`.

```rust
#[task]
fn add(a: u32, b: u32) -> u32 { a + b }

let counts = ctx
    .text_file("data/words.txt")?
    .flat_map_task(Tokenize)
    .map_task(PairOne)
    .reduce_by_key_task(Add)
    .collect()?;
```

## Registering tasks

Register a function once with `#[task]`, or inline a one-off with `task_fn!`:

```rust
#[task]
fn double(x: i32) -> i32 { x * 2 }
rdd.map_task(Double);

rdd.map_task(task_fn!(|x: i32| -> i32 { x * 2 }));
```

For shuffle operations, register the key-value handler once in your binary:

```rust
atomic_compute::register_shuffle_map!(String, u32);
// For Ord keys used with sort_by_key:
atomic_compute::register_sort_shuffle_map!(i64, f64);
```

## Persist and cache

Cache an RDD so repeated actions reuse computed partitions:

```rust
let cached = rdd.map_task(Double).cache();
cached.collect()?;   // computes and stores
cached.count()?;     // reads from the store, no recompute
```

Storage levels: `MemoryOnly` (default), `MemoryAndDisk`, and `DiskOnly` via
`persist_with_disk(level)`. Use `unpersist()` to drop cached partitions and
`is_cached()` to check presence.

## Entry point

Build a `Config` at the entry point and pass it to the context. For programs
that run as both driver and worker, use `AtomicApp::build()`, which reads
`--worker`, `--workers`, and `--local-ip` from the command line.

```rust
let app = AtomicApp::build().await?;
let ctx = app.driver_context()?;
```

Workers run the same binary: `./my_app --worker --port 10001`.
