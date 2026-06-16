# atomic-streaming

Spark Streaming–style micro-batch streaming for the Atomic distributed compute
framework.

`atomic-streaming` runs a batch loop on top of `atomic-compute`: every
`batch_duration` the scheduler fires, materialises an RDD from each active input
stream, and executes registered output operations on the result.

> **Note:** `atomic-streaming` is a **Rust-only** API. It is not exposed through
> the Python (`atomic-compute`) or TypeScript (`@atomic-compute/js`) bindings.

---

## Quick start

```rust
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use atomic_compute::context::Context;
use atomic_streaming::context::StreamingContext;

fn main() -> anyhow::Result<()> {
    let sc = Arc::new(Context::local()?);
    let ssc = StreamingContext::new(sc, Duration::from_secs(1));

    // Push RDDs into this queue from another thread or at test time
    let queue: Arc<Mutex<VecDeque<_>>> = Arc::new(Mutex::new(VecDeque::new()));
    let stream = ssc.queue_stream(queue.clone(), true);

    ssc.foreach_rdd(stream, |rdd, batch_time_ms| {
        let count = rdd.count().unwrap_or(0);
        println!("batch {batch_time_ms}: {count} elements");
    });

    ssc.start()?;
    ssc.await_termination_or_timeout(Duration::from_secs(10))?;
    Ok(())
}
```

---

## Input streams

### `QueueInputDStream` — in-memory queue (testing)

```rust
let queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = i32>>>>> =
    Arc::new(Mutex::new(VecDeque::new()));

// Push an RDD before each batch
queue.lock().push_back(Arc::new(sc.parallelize_typed(vec![1, 2, 3], 1)));

let stream = ssc.queue_stream(queue.clone(), /*one_at_a_time=*/ true);
```

### `SocketInputDStream` — TCP text socket

Reads newline-delimited text from a TCP connection. Each batch yields the lines
received during that interval as `RDD<String>`.

```rust
let stream = ssc.socket_text_stream("localhost", 9999);
```

Start a test server: `nc -lk 9999`

### `KafkaInputDStream` — Kafka topic consumer (`kafka` feature)

Requires the `kafka` feature (vendored librdkafka). Polls one or more Kafka topics in a background thread; each batch drains the buffer into a `RDD<String>` of raw message payloads. Offsets are committed by the consumer group (auto-commit, at-least-once).

```rust
// Cargo.toml: atomic-streaming = { ..., features = ["kafka"] }

let stream = ssc.kafka_stream("localhost:9092", "my-group", &["events"]);
ssc.foreach_rdd(stream, |rdd, batch_time_ms| {
    // rdd is RDD<String> of raw Kafka message payloads
    let count = rdd.count().unwrap_or(0);
    println!("batch {batch_time_ms}: {count} Kafka messages");
});
```

### `FileInputDStream` — directory watcher

Scans a directory each batch and returns lines from files modified since the
previous batch as `RDD<String>`.

```rust
let stream = ssc.text_file_stream("/data/incoming/");
```

---

## Transformations

Transformations on any `Arc<dyn DStream<T>>` produce a new typed DStream.
Register them before calling `ssc.start()`.

### Narrow transforms

These wrap the parent RDD in the corresponding `atomic-compute` RDD type each batch:

| Method                                   | Description                               |
| ---------------------------------------- | ----------------------------------------- |
| `MappedDStream::new(id, parent, f)`      | Apply `f` to each element                 |
| `FlatMappedDStream::new(id, parent, f)`  | Apply `f` and flatten                     |
| `FilteredDStream::new(id, parent, pred)` | Keep elements where `pred` returns `true` |
| `TransformedDStream::new(id, parent, f)` | Apply an arbitrary `Rdd → Rdd` function   |

### Window

`WindowedDStream` unions all parent batches within a sliding window:

```rust
use atomic_streaming::dstream::windowed::WindowedDStream;
use std::time::Duration;

let windowed = WindowedDStream::new(
    stream_id,
    parent_stream.clone(),
    Duration::from_secs(30),   // window length
    Duration::from_secs(10),   // slide interval
    ssc.clone(),
);
```

### Pair (key-value) operations

`PairDStream<K, V>` exposes shuffle-based operations. Wrap a `DStream<(K, V)>`:

```rust
use atomic_streaming::dstream::pair::PairDStream;

let pairs: Arc<dyn DStream<(String, u64)>> = /* ... */;
let pair_dstream = PairDStream::new(pairs);

// Reduce by key — each batch independently
let counts = pair_dstream.reduce_by_key(|a, b| a + b, num_partitions);

// Join two pair streams
let joined = pair_dstream.join(other_pair_dstream, num_partitions);

// Stateful aggregation across batches
let running_counts = pair_dstream.update_state_by_key(
    |new_values: &[u64], old_state: Option<u64>| -> Option<u64> {
        let sum: u64 = new_values.iter().sum();
        Some(old_state.unwrap_or(0) + sum)
    },
);
```

---

## Output operations

Register output operations before `ssc.start()`.

### `foreach_rdd`

```rust
ssc.foreach_rdd(stream, |rdd, batch_time_ms| {
    let results = rdd.collect().unwrap();
    // process results...
});
```

### `print`

Prints the first 10 elements of each batch to stdout.

```rust
ssc.print(stream);
```

### `save_as_text_files`

Writes each batch RDD to `{prefix}-{batch_time_ms}/part-N` files.

```rust
ssc.save_as_text_files(stream, "/data/output/batch");
```

---

## Checkpointing

```rust
// Set checkpoint directory before start
ssc.checkpoint_to("/data/checkpoints");

// Start normally
ssc.start()?;

// ...later, restore from checkpoint after a failure
let ssc = StreamingContext::from_checkpoint("/data/checkpoints")?;
ssc.start()?;
```

Checkpoint state is serialized with `bincode` and written atomically
(`.tmp` → rename) after each configured checkpoint interval.

---

## Lifecycle

```rust
ssc.start()?;                                           // start batch loop
ssc.await_termination()?;                               // block until stopped
ssc.await_termination_or_timeout(Duration::from_secs(60))?; // with timeout
```

`state()` returns `StreamingContextState::Initialized`, `Active`, or `Stopped`.

---

## Known limitations

- **Driver-local sources.** All input sources (socket, file, Kafka) run a background thread in the driver process. Distributed receiver placement (running consumers on worker nodes) depends on distributed streaming execution, which is not yet implemented.
- **Local execution only.** All streaming computation runs on the driver's
  `Arc<Context>`. Distributed worker dispatch for streaming stages is not wired.
- **No event-time watermarking.** All windowing is processing-time based. For
  event-time windows with watermark and late-data handling, use `atomic-structured`.
- **At-least-once delivery.** Kafka offsets are auto-committed; exactly-once requires idempotent sinks or transactional producers (not implemented).

---

## Running tests

```bash
cargo test -p atomic-streaming
```
