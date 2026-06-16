# atomic-structured

Spark Structured Streaming–style continuous SQL/DataFrame queries on the micro-batch
loop. Built on top of [`atomic-streaming`] (batch loop) and [`atomic-sql`]
(DataFusion query engine).

A continuous query is a normal SQL or DataFrame query declared **once** and executed
**incrementally**, once per micro-batch, on a live stream of Arrow `RecordBatch` rows.

---

## Quick start — stateless SQL query

```rust
use std::sync::Arc;
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_streaming::context::StreamingContext;
use atomic_structured::{StreamingDataFrame, OutputMode, Trigger};
use atomic_structured::source::QueueSource;
use atomic_structured::sink::{MemorySink, shared};

fn main() -> anyhow::Result<()> {
    let sc = Arc::new(Context::local()?);
    let ssc = StreamingContext::new(sc, Duration::from_secs(1));

    // Push pre-built Arrow batches into the queue source
    let source = Arc::new(QueueSource::from_batches(schema.clone(), batches));
    let sink = Arc::new(MemorySink::new());

    let query = StreamingDataFrame::read_stream(source)
        .sql("SELECT user, amount FROM input WHERE amount > 100")
        .output_mode(OutputMode::Append)
        .trigger(Trigger::Once)
        .format(shared(sink.clone()))
        .start(&ssc)?;

    ssc.start()?;
    query.await_termination()?;

    println!("Emitted {} rows", sink.row_count());
    Ok(())
}
```

---

## Query paths

### 4a — Stateless (per-batch SQL transform)

No state carried between batches. Each batch: register the incoming rows as the
`input` table, run the SQL, emit the results.

```rust
StreamingDataFrame::read_stream(source)
    .sql("SELECT user, COUNT(*) as n FROM input GROUP BY user")
    .output_mode(OutputMode::Append)
    // ...
```

### 4b — Windowed aggregation (event-time + watermark)

Tumbling event-time windows. A watermark drops rows older than
`max(time_col) - delay`; finalized windows are emitted once and never updated.

```rust
use atomic_structured::state::{Agg, AggKind};

StreamingDataFrame::read_stream(source)
    .with_watermark("ts", Duration::from_secs(5))   // 5-second late-data tolerance
    .window("ts", Duration::from_secs(60))           // 1-minute tumbling window
    .group_by(&["user"])
    .aggregate(vec![
        Agg { kind: AggKind::Count, col: "amount".into(), alias: "n".into() },
        Agg { kind: AggKind::Sum,   col: "amount".into(), alias: "total".into() },
    ])
    .write_stream()
    .output_mode(OutputMode::Append)
    // ...
```

Supported aggregates: `Count`, `Sum`, `Min`, `Max`, `Avg`.

Output modes for windowed queries:
- `Append` — emit only finalized (past-watermark) windows.
- `Update` — emit every window that changed this batch (partial results included).

### 4c — Complete output mode

Emit the **entire** result table every batch — equivalent to re-running the query from
scratch each batch but using the accumulated state store. Works with both stateless and
windowed engines.

```rust
.output_mode(OutputMode::Complete)
```

---

## Sources

### `QueueSource` — in-memory queue (testing)

```rust
use atomic_structured::source::QueueSource;

let source = Arc::new(QueueSource::new(schema.clone()));
source.push(vec![batch1]);   // one push = one micro-batch
source.push(vec![batch2]);
```

`QueueSource::from_batches(schema, batches)` pre-loads all batches at construction.

### `KafkaSource` — Kafka topic (`kafka` feature)

```rust
// Cargo.toml: atomic-structured = { ..., features = ["kafka"] }

use atomic_structured::kafka::KafkaSource;

let source = Arc::new(KafkaSource::new(
    schema.clone(),
    "localhost:9092",   // brokers
    "my-group",         // consumer group id
    &["events"],        // topics
));
```

Messages must be JSON objects whose fields match the declared `schema`. Offsets are
auto-committed (at-least-once).

---

## Sinks

| Sink                          | Description                                                |
| ----------------------------- | ---------------------------------------------------------- |
| `MemorySink`                  | Collects all emitted batches in memory — primary test sink |
| `ConsoleSink`                 | Pretty-prints each non-empty batch to stdout               |
| `FileSink`                    | Writes `part-{epoch}.parquet` to a local directory         |
| `KafkaSink` (`kafka` feature) | Serializes rows to JSON and produces to a Kafka topic      |

```rust
use atomic_structured::sink::{MemorySink, ConsoleSink, FileSink, shared};

let sink = shared(MemorySink::new());
let sink = shared(ConsoleSink::new("my-query"));
let sink = shared(FileSink::new("/data/output/"));
```

---

## Checkpoint and recovery

State is serialized with bincode and written atomically
(`state.bin.tmp` → `state.bin`) after each batch.

```rust
let query = StreamingDataFrame::read_stream(source)
    .window("ts", Duration::from_secs(60))
    .group_by(&["user"])
    .aggregate(vec![Agg { kind: AggKind::Sum, col: "amount".into(), alias: "total".into() }])
    .write_stream()
    .checkpoint("/data/checkpoints/my-query")   // enable checkpoint
    .output_mode(OutputMode::Update)
    .format(shared(sink.clone()))
    .start(&ssc)?;
```

After a restart, use `StreamingQuery::restore(dir)` to reload the state store before
calling `start`.

---

## Lifecycle

```rust
ssc.start()?;
query.await_termination()?;    // block until the query stops
query.stop();                  // request graceful shutdown
```

`Trigger::Once` processes exactly one batch then stops automatically — useful for
bounded sources and tests.

---

## Known limitations

- **Driver-local sources.** The Kafka consumer thread runs in the driver process.
  Distributed receiver placement requires distributed streaming execution (not yet
  implemented).
- **Tumbling windows only.** Sliding and session windows are not supported; one
  watermark per query.
- **At-least-once delivery.** Exactly-once requires idempotent sinks (e.g., the
  deterministic `part-{epoch}.parquet` naming of `FileSink`) or Kafka transactions
  (not implemented).
- **No stream-stream joins.** Each query has one source table (`input`).

---

## Running tests

```bash
cargo test -p atomic-structured
# Kafka integration tests (requires a running broker):
cargo test -p atomic-structured --features kafka -- --include-ignored
```
