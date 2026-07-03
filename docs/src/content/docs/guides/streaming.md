---
title: Streaming
description: Micro-batch streaming and continuous SQL queries.
---

Atomic has two streaming layers. `atomic-streaming` produces one RDD per batch
interval and runs a processing function over it. `atomic-structured` runs a
declared SQL/DataFrame query incrementally, once per batch, on the same batch
loop.

## Micro-batch streaming

A `StreamingContext` wraps a compute context and a batch duration. A dedicated
thread fires every interval, builds the batch's RDD, and runs the registered
output operation.

```rust
let ctx = Context::local()?;
let ssc = StreamingContext::new(ctx, Duration::from_secs(1));
let queue = Arc::new(Mutex::new(VecDeque::new()));
let stream = ssc.queue_stream(queue.clone(), true);
ssc.foreach_rdd(stream, |rdd, time_ms| { /* process rdd */ });
ssc.start()?;
ssc.await_termination()?;
```

### Input sources

| Source | Description |
|---|---|
| `queue_stream` | In-memory queue of RDDs; used for testing |
| `SocketInputDStream` | Reads lines from a TCP socket |
| `FileInputDStream` | Watches a directory for new files |
| `direct_kafka_stream` | Direct-pull Kafka source, one task per Kafka partition |

### Transformations

`map`, `flat_map`, `filter`, and windowed operations build a new RDD DAG per
batch. Stateful keyed operations are available through `PairDStream`:
`reduce_by_key`, `update_state_by_key` (carries state across batches), and
windowed reduce with an inverse function.

### Distributed sources

The direct-pull model distributes source reads across workers. The driver plans
which byte ranges, file splits, or offsets each batch reads, dispatches one
short-lived task per partition, and commits only after the sink confirms. A
worker death means the split is not committed and is re-planned on the next
batch, giving at-least-once delivery. `DistributedFileSource` and the Kafka
direct source both follow this model.

## Structured streaming

`atomic-structured` runs a SQL/DataFrame query declared once and executed
incrementally. SQL runs through `atomic-sql` (DataFusion); the row format is
Arrow `RecordBatch`.

```rust
let sink = MemorySink::new();
let query = StreamingDataFrame::read_stream(source)
    .sql("SELECT user, amount FROM input WHERE amount > 100")
    .write_stream()
    .output_mode(OutputMode::Append)
    .trigger(Trigger::ProcessingTime(Duration::from_secs(1)))
    .format(sink.clone())
    .start(&ssc)?;
query.await_termination()?;
```

### Window models

- **Tumbling** — `window(col, size)`; one window per row.
- **Sliding** — `window(col, size).slide(step)`; a row contributes to every
  covering window.
- **Session** — `session_window(col, gap)`; window bounds are dynamic and merge
  when events bridge them. A window finalizes when `end + gap` is at or below
  the watermark.

### Constraints

- Aggregates must be mergeable: count, sum, min, max, average. Median, exact
  distinct, and arbitrary user-defined aggregates are rejected at plan time.
- One monotonic global watermark per query.
- Stream-stream joins require a time bound; an unbounded equi-join is rejected so
  state stays bounded.

### Distributed state

By default the window, join, and state-store engines run on the driver. Opting
in with `.distributed(n)` shards the keyed state across `n` workers using a
state-merge task and a worker-resident state store, with per-shard checkpoint
and recovery. Each shard reports back to the driver so it stays on the same
worker across batches. The source read distributes independently through the
direct-pull model.

Each shard writes its post-merge state to `<checkpoint_dir>/shard-<id>.bin`
after every batch and reloads it when the shard has no in-memory state. When a
worker dies, the driver re-routes its shards to surviving workers on the next
batch. Those workers can only reload the checkpoints they can read: put
`checkpoint_dir` on storage shared by all workers (NFS, a mounted volume) to
recover state across workers. With a worker-local directory, a re-routed
shard restarts empty and logs a warning.

### Exactly-once Kafka

With the Kafka feature, a Kafka source running with auto-commit disabled and a
transactional Kafka sink commit source offsets and output records together in
one broker transaction, giving end-to-end exactly-once delivery. At-least-once
mode is also available.
