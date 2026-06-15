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
