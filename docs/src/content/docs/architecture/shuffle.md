---
title: Shuffle
description: How Atomic redistributes data across partitions between stages.
---

A shuffle redistributes data so that all records with the same key land in the
same partition. Operations like `reduce_by_key`, `group_by_key`, `join`, and
`sort_by_key` need a shuffle. This page describes how the engine plans and runs
one.

## Lazy construction

Shuffle operations are lazy. `reduce_by_key` and `group_by_key` build a
`ShuffleDependency` and a `ShuffledRdd`, then return without doing work.
Execution starts when an action fires. The scheduler splits the DAG at the
shuffle boundary into a map stage and a reduce stage.

## Distributed flow

```text
1. Driver detects a ShuffleDependency in the RDD DAG.
2. Driver encodes each parent partition as rkyv-encoded (K, V) bytes.
3. Driver sends one ShuffleMap task per input partition to the workers.
4. Worker: decode (K, V) → partition by key → write buckets to the shuffle cache.
5. Worker returns its shuffle server address in the task result.
6. Driver registers the addresses with the MapOutputTracker.
7. Driver runs the reduce stage: ShuffledRdd::compute → ShuffleFetcher reads
   each bucket over HTTP and merges them through the Aggregator.
```

Shuffle data is stored in the worker's in-memory shuffle cache and served by a
per-worker HTTP server. The reduce side pulls buckets from each map worker;
there is no direct worker-to-worker push.

## Shuffle types

| Type | Crate | Role |
|---|---|---|
| `ShuffleDependency` | `atomic-data` | Shuffle edge in the RDD DAG (erased; typed source is `TypedShuffle`) |
| `ShuffledRdd` | `atomic-compute` | Reduce-side RDD; fetches and merges on compute |
| `ShuffleManager` | `atomic-data` | Per-worker HTTP server for shuffle reads |
| `ShuffleFetcher` | `atomic-data` | HTTP client with exponential-backoff retry |
| `MapOutputTracker` | `atomic-data` | Tracks which worker holds which shuffle bucket |
| `DashMapShuffleCache` | `atomic-data` | Lock-free in-memory bucket store |

## Sort-based shuffle

Below a configurable threshold, each map task writes one bucket entry per reduce
partition. Above the threshold (`ATOMIC_SORT_SHUFFLE_THRESHOLD`, default 200),
each map task writes a consolidated layout instead — one data blob plus one
offset index — which avoids the file and entry explosion on wide shuffles. The
shuffle cache serves a partition's slice with a ranged read, so the reduce
client is unchanged.

When the key type is `Ord` (for example through `sort_by_key`), the map side
writes sorted runs and the reduce side does a k-way sort-merge instead of
building a hash map. For distributed sort, register the sorted handler with
`register_sort_shuffle_map!(K, V)`; the partitioner is shipped to workers as a
serializable spec, so workers partition with the RDD's real partitioner and the
output is globally ordered.

## Adaptive coalescing

When `ATOMIC_COALESCE_SHUFFLE_THRESHOLD_BYTES` is set, the scheduler queries the
shuffle cache for bucket sizes after the map stage and merges small reduce
partitions. The coalesced partition count is stored in the `MapOutputTracker`,
and `ShuffledRdd` uses it when computing the reduce stage.

## Fault recovery

A reduce task that cannot fetch a bucket from a dead worker surfaces a
`FetchFailed` error. The scheduler invalidates that map output, marks the
producing map stage failed, and recomputes only the lost partition. When a
heartbeat detects a worker death, the scheduler proactively unregisters every
map output that worker held, so the next reduce attempt recomputes from lineage
rather than retrying a dead address.
