//! Kafka source and sink (4c) — gated behind the `kafka` feature.
//!
//! [`KafkaSource`] consumes JSON-object messages from topics and converts them to
//! Arrow batches per a declared schema; [`KafkaSink`] serializes emitted rows back
//! to JSON and produces them to a topic. Delivery is at-least-once: for the source,
//! consumer-group offsets auto-commit; for the sink, rows are produced then flushed
//! per batch.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use parking_lot::Mutex;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

use crate::errors::{StructuredError, StructuredResult};
use crate::sink::Sink;
use crate::source::StreamSource;

/// A structured-streaming source over Kafka topics carrying JSON-object messages.
pub struct KafkaSource {
    schema: SchemaRef,
    brokers: String,
    group_id: String,
    topics: Vec<String>,
    buffer: Arc<Mutex<Vec<serde_json::Value>>>,
    handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    stop: Arc<AtomicBool>,
}

impl KafkaSource {
    /// Build a source. Each message payload must be a JSON object whose fields
    /// match `schema`.
    pub fn new(schema: SchemaRef, brokers: &str, group_id: &str, topics: &[&str]) -> Self {
        KafkaSource {
            schema,
            brokers: brokers.to_string(),
            group_id: group_id.to_string(),
            topics: topics.iter().map(|s| s.to_string()).collect(),
            buffer: Arc::new(Mutex::new(Vec::new())),
            handle: Mutex::new(None),
            stop: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl StreamSource for KafkaSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&self, _time_ms: u64) -> Vec<RecordBatch> {
        let rows: Vec<serde_json::Value> = self.buffer.lock().drain(..).collect();
        match json_to_batch(&self.schema, &rows) {
            Ok(Some(b)) => vec![b],
            _ => vec![],
        }
    }

    fn start(&self) {
        let brokers = self.brokers.clone();
        let group_id = self.group_id.clone();
        let topics = self.topics.clone();
        let buffer = self.buffer.clone();
        let stop = self.stop.clone();

        let handle = std::thread::Builder::new()
            .name("structured-kafka-source".into())
            .spawn(move || {
                let consumer: BaseConsumer = match ClientConfig::new()
                    .set("bootstrap.servers", &brokers)
                    .set("group.id", &group_id)
                    .set("enable.auto.commit", "true")
                    .set("auto.offset.reset", "earliest")
                    .create()
                {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!("KafkaSource: consumer create failed: {e}");
                        return;
                    }
                };
                let refs: Vec<&str> = topics.iter().map(String::as_str).collect();
                if let Err(e) = consumer.subscribe(&refs) {
                    log::error!("KafkaSource: subscribe failed: {e}");
                    return;
                }
                while !stop.load(Ordering::SeqCst) {
                    match consumer.poll(Duration::from_millis(200)) {
                        Some(Ok(msg)) => {
                            if let Some(v) = msg
                                .payload()
                                .and_then(|p| serde_json::from_slice::<serde_json::Value>(p).ok())
                                .filter(serde_json::Value::is_object)
                            {
                                buffer.lock().push(v);
                            }
                        }
                        Some(Err(e)) => log::warn!("KafkaSource: poll error: {e}"),
                        None => {}
                    }
                }
            })
            .expect("spawn kafka source thread");
        *self.handle.lock() = Some(handle);
    }

    fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.lock().take() {
            let _ = h.join();
        }
    }
}

/// A sink that produces each emitted row as a JSON-object message to a topic.
pub struct KafkaSink {
    topic: String,
    producer: BaseProducer,
}

impl KafkaSink {
    pub fn new(brokers: &str, topic: &str) -> StructuredResult<Self> {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .map_err(|e| StructuredError::Sink(e.to_string()))?;
        Ok(KafkaSink {
            topic: topic.to_string(),
            producer,
        })
    }
}

impl Sink for KafkaSink {
    fn add_batch(&self, _epoch: u64, batches: &[RecordBatch]) -> StructuredResult<()> {
        for b in batches {
            for row in 0..b.num_rows() {
                let json = row_to_json(b, row);
                let payload = serde_json::to_string(&json)
                    .map_err(|e| StructuredError::Sink(e.to_string()))?;
                let record = BaseRecord::<(), str>::to(&self.topic).payload(&payload);
                if let Err((e, _)) = self.producer.send(record) {
                    return Err(StructuredError::Sink(e.to_string()));
                }
            }
        }
        self.producer
            .flush(Duration::from_secs(5))
            .map_err(|e| StructuredError::Sink(e.to_string()))?;
        Ok(())
    }
}

// ── JSON ⇄ Arrow ──────────────────────────────────────────────────────────────

/// Convert JSON-object `rows` into one Arrow batch per `schema`. Returns `None`
/// when there are no rows. Supports Int64 / Float64 / Utf8 / Boolean columns.
fn json_to_batch(
    schema: &SchemaRef,
    rows: &[serde_json::Value],
) -> StructuredResult<Option<RecordBatch>> {
    if rows.is_empty() {
        return Ok(None);
    }
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let name = field.name();
        let array: Arc<dyn Array> = match field.data_type() {
            DataType::Int64 => Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| r.get(name).and_then(serde_json::Value::as_i64))
                    .collect::<Vec<_>>(),
            )),
            DataType::Float64 => Arc::new(Float64Array::from(
                rows.iter()
                    .map(|r| r.get(name).and_then(serde_json::Value::as_f64))
                    .collect::<Vec<_>>(),
            )),
            DataType::Boolean => Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|r| r.get(name).and_then(serde_json::Value::as_bool))
                    .collect::<Vec<_>>(),
            )),
            _ => Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.get(name).and_then(|v| v.as_str().map(str::to_string)))
                    .collect::<Vec<_>>(),
            )),
        };
        columns.push(array);
    }
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| StructuredError::Source(e.to_string()))?;
    Ok(Some(batch))
}

/// Convert one row of a batch into a JSON object.
fn row_to_json(batch: &RecordBatch, row: usize) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(i);
        let value = if col.is_null(row) {
            serde_json::Value::Null
        } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            serde_json::Value::from(a.value(row))
        } else if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
            serde_json::Value::from(a.value(row))
        } else if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
            serde_json::Value::from(a.value(row))
        } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
            serde_json::Value::from(a.value(row))
        } else {
            serde_json::Value::Null
        };
        obj.insert(field.name().clone(), value);
    }
    serde_json::Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};

    // The consumer/producer are thin rdkafka wrappers; the conversion is the
    // real logic — verify it round-trips without a broker.
    #[test]
    fn json_arrow_roundtrip() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("user", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let original = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let rows: Vec<serde_json::Value> = (0..original.num_rows())
            .map(|r| row_to_json(&original, r))
            .collect();
        let back = json_to_batch(&schema, &rows).unwrap().unwrap();

        assert_eq!(back.num_rows(), 2);
        let users = back
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ns = back
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!((users.value(0), ns.value(0)), ("a", 1));
        assert_eq!((users.value(1), ns.value(1)), ("b", 2));
    }
}
