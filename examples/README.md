# Atomic Examples

Runnable, self-contained examples — one crate per concept. Each is a workspace
member, so run any of them with `cargo run -p <name>`.

## RDD core

| Example | Module / concept | Run |
|---|---|---|
| [`wordcount`](wordcount) | `flat_map_task` → `map_task` pipeline + driver aggregation | `cargo run -p wordcount` |
| [`task_wordcount`](task_wordcount) | Distributed word count with `reduce_by_key` shuffle | `cargo run -p task_wordcount` |
| [`task_double`](task_double) | `#[task]` / `task_fn!` basics — map / filter / flat_map / fold | `cargo run -p task_double` |
| [`pi`](pi) | Monte-Carlo π — `map_task` + `fold_task` numeric reduction | `cargo run -p pi` |
| [`group_by`](group_by) | Pair RDD grouping — `reduce_by_key`, `group_by_key`, `map_values`, `count_by_key` | `cargo run -p group_by` |
| [`sort`](sort) | Per-partition sort + driver-side k-way merge | `cargo run -p sort` |
| [`joins`](joins) | Shuffle joins — `join`, outer joins, `cogroup` | `cargo run -p joins` |
| [`caching`](caching) | Persistence — `cache`, `persist`, `unpersist`, `checkpoint` | `cargo run -p caching` |

## Subsystems

| Example | Module / concept | Run |
|---|---|---|
| [`sql`](sql) | `atomic-sql` — SQL (DataFusion) over RDD-backed tables: projection, aggregate, join | `cargo run -p sql` |
| [`streaming`](streaming) | `atomic-streaming` — micro-batch word count over a `queue_stream` | `cargo run -p streaming` |
| [`graph`](graph) | `atomic-graph` — PageRank, Connected Components, Triangle Count | `cargo run -p graph` |
| [`nlq`](nlq) | `atomic-nlq` — natural-language query (needs `OPENAI_API_KEY`; falls back to SQL) | `cargo run -p nlq` |

## Language bindings (separate toolchains, excluded from the Cargo workspace)

| Example | Stack | Run |
|---|---|---|
| [`py-demo`](py-demo) | `atomic-py` Python bindings (maturin) | see [`py-demo/README.md`](py-demo/README.md) |
| [`ts-demo`](ts-demo) | `atomic-js` Node.js bindings (NAPI) | see [`ts-demo/README.md`](ts-demo/README.md) |

## Running distributed

The RDD examples that use `AtomicApp::build()` (e.g. `wordcount`, `task_wordcount`,
`task_double`, `pi`, `joins`) run the **same binary** as both driver and worker.
Start one or more workers, then point a driver at them:

```bash
cargo build -p joins --release
./target/release/joins --worker --port 10001    # terminal 1: a worker
./target/release/joins --workers 127.0.0.1:10001 # terminal 2: the driver
```

`sql`, `streaming`, and `graph` run on the driver (local) — see each example's
header comment for how to scale or attach real sources (sockets, more partitions).
