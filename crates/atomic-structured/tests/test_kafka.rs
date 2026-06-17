//! 4c — end-to-end Kafka round-trip. Requires a broker, so it is `#[ignore]`d;
//! run with `cargo test -p atomic-structured --features kafka -- --ignored`.
#![cfg(feature = "kafka")]

use std::sync::Arc;
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_streaming::context::StreamingContext;
use atomic_structured::kafka::{KafkaSink, KafkaSource};
use atomic_structured::sink::Sink;
use atomic_structured::{Agg, OutputMode, StreamingDataFrame, Trigger};

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("user", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

#[test]
#[ignore = "requires a Kafka broker at localhost:9092"]
fn kafka_roundtrip() {
    const BROKERS: &str = "localhost:9092";

    // Produce a few input rows to a topic via the KafkaSink.
    let producer = KafkaSink::new(BROKERS, "structured-in").unwrap();
    let input = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![100, 200, 1500])),
            Arc::new(StringArray::from(vec!["a", "a", "b"])),
            Arc::new(Int64Array::from(vec![10, 20, 5])),
        ],
    )
    .unwrap();
    producer.add_batch(0, &[input]).unwrap();

    // Read from the topic, aggregate per window, write results to another topic.
    let source = Arc::new(KafkaSource::new(
        schema(),
        BROKERS,
        "structured-group",
        &["structured-in"],
    ));
    let out = Arc::new(KafkaSink::new(BROKERS, "structured-out").unwrap());
    let ssc = StreamingContext::new(Context::local().unwrap(), Duration::from_millis(200));

    let q = StreamingDataFrame::read_stream(source)
        .window("ts", Duration::from_millis(1000))
        .group_by(&["user"])
        .aggregate(vec![Agg::count("cnt"), Agg::sum("amount", "total")])
        .write_stream()
        .output_mode(OutputMode::Update)
        .trigger(Trigger::ProcessingTime(Duration::from_millis(200)))
        .format(out)
        .start(&ssc)
        .unwrap();

    std::thread::sleep(Duration::from_secs(2));
    q.stop();
    // Assertion is manual: inspect the `structured-out` topic.
}

#[test]
#[ignore = "requires a Kafka broker at localhost:9092"]
fn transactional_sink_commits_atomically() {
    const BROKERS: &str = "localhost:9092";

    // A transactional sink: each `add_batch` is one Kafka transaction. The rows of
    // a batch become visible to read-committed consumers only on commit, and a
    // batch retried after a crash (same `transactional.id`) fences the previous
    // producer rather than double-writing.
    let sink =
        KafkaSink::transactional(BROKERS, "structured-txn-out", "atomic-structured-q1").unwrap();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();

    sink.add_batch(0, &[batch]).unwrap();
    // Assertion is manual: a read-committed consumer of `structured-txn-out` sees
    // exactly two rows, and re-running this test does not add duplicates.
}

#[test]
#[ignore = "requires a Kafka broker at localhost:9092 (exactly-once end-to-end)"]
fn exactly_once_kafka_to_kafka() {
    const BROKERS: &str = "localhost:9092";
    const IN_TOPIC: &str = "eo-in";
    const OUT_TOPIC: &str = "eo-out";
    const TXN_ID: &str = "atomic-eo-q1";
    const GROUP: &str = "atomic-eo-consumer";

    // Pre-seed the input topic with two rows via a non-transactional producer.
    let seed = KafkaSink::new(BROKERS, IN_TOPIC).unwrap();
    let input = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    seed.add_batch(0, &[input]).unwrap();

    // Build a KafkaSource (manual-commit, exactly-once capable) and a
    // transactional KafkaSink. The QueryRunner detects both and routes
    // the batch through `add_batch_with_offsets`: begin → produce →
    // send_offsets_to_transaction → commit.
    let source = Arc::new(KafkaSource::new(schema(), BROKERS, GROUP, &[IN_TOPIC]));
    let sink = Arc::new(KafkaSink::transactional(BROKERS, OUT_TOPIC, TXN_ID).unwrap());

    let ssc = StreamingContext::new(Context::local().unwrap(), Duration::from_millis(500));
    let q = StreamingDataFrame::read_stream(source)
        .sql("SELECT * FROM input")
        .output_mode(atomic_structured::OutputMode::Append)
        .trigger(atomic_structured::Trigger::Once)
        .format(sink)
        .start(&ssc)
        .unwrap();
    q.await_termination().unwrap();
    // Assertions are manual: a read-committed consumer of `eo-out` must see exactly
    // two rows. Re-running this test (same transactional.id) must not produce
    // duplicates — the fenced producer from the prior run is aborted on init.
}
