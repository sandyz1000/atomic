//! Kafka source and sink (4c) — gated behind the `kafka` feature.
//!
//! [`KafkaSource`] consumes JSON-object messages from topics and converts them to
//! Arrow batches per a declared schema; [`KafkaSink`] serializes emitted rows back
//! to JSON and produces them to a topic.
//!
//! Delivery semantics:
//! - **At-least-once** (default): sink produces + flushes; source commits offsets
//!   in `post_batch_commit` after the sink confirms delivery.
//! - **Exactly-once** (transactional [`KafkaSink`] + [`KafkaSource`]): the source
//!   exposes `pending_offsets()` carrying the consumed [`TopicPartitionList`] and the
//!   consumer's [`ConsumerGroupMetadata`]; the sink's
//!   `add_batch_with_offsets` wraps produce + `send_offsets_to_transaction` in one
//!   Kafka transaction, so a crash-restart replays neither the source nor the output.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_streaming::dstream::kafka_direct::{OffsetRange, OffsetTracker, build_staged_pipeline};
use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use parking_lot::Mutex;
use rdkafka::TopicPartitionList;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerGroupMetadata};
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

use crate::errors::{StructuredError, StructuredResult};
use crate::sink::Sink;
use crate::source::StreamSource;

/// Consumed offsets for a single micro-batch — carries everything the Kafka producer
/// needs to commit source offsets inside a transaction (`send_offsets_to_transaction`).
pub struct OffsetCommit {
    /// Topic-partition-to-next-offset map for messages returned by the last `next_batch`.
    pub tpl: TopicPartitionList,
    /// Consumer group metadata required by `send_offsets_to_transaction`.
    pub group_metadata: Arc<ConsumerGroupMetadata>,
}

/// Shared buffer + polled-offset tracker updated atomically by the background thread.
struct SourceShared {
    buffer: Vec<serde_json::Value>,
    /// Offsets polled since the last `next_batch` drain. Swapped out atomically by
    /// `next_batch` so that `pending_tpl` covers exactly the messages returned.
    polled_tpl: TopicPartitionList,
}

/// A structured-streaming source over Kafka topics carrying JSON-object messages.
///
/// Delivery contract:
/// - With a non-transactional sink: commit consumer offsets in `post_batch_commit`
///   (at-least-once — offsets advance only after the sink confirms the batch).
/// - With [`KafkaSink::transactional`]: call `pending_offsets()` to get the batch's
///   [`OffsetCommit`] and pass it to [`KafkaSink::add_batch_with_offsets`], which commits
///   source offsets inside the producer transaction (exactly-once).
pub struct KafkaSource {
    schema: SchemaRef,
    brokers: String,
    group_id: String,
    topics: Vec<String>,
    shared: Arc<Mutex<SourceShared>>,
    /// Shared consumer — created in `start()`, accessible from the main thread for
    /// `group_metadata()` (exactly-once) and `commit()` (at-least-once).
    consumer: Arc<Mutex<Option<Arc<BaseConsumer>>>>,
    handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    stop: Arc<AtomicBool>,
    /// Offsets for the last batch returned by `next_batch`; consumed by `pending_offsets`.
    pending_tpl: Mutex<TopicPartitionList>,
}

impl KafkaSource {
    /// Build a source. Each message payload must be a JSON object whose fields
    /// match `schema`. Offsets are committed manually (at-least-once via
    /// `post_batch_commit`, or exactly-once via `pending_offsets` + the sink transaction).
    pub fn new(schema: SchemaRef, brokers: &str, group_id: &str, topics: &[&str]) -> Self {
        KafkaSource {
            schema,
            brokers: brokers.to_string(),
            group_id: group_id.to_string(),
            topics: topics.iter().map(|s| s.to_string()).collect(),
            shared: Arc::new(Mutex::new(SourceShared {
                buffer: Vec::new(),
                polled_tpl: TopicPartitionList::new(),
            })),
            consumer: Arc::new(Mutex::new(None)),
            handle: Mutex::new(None),
            stop: Arc::new(AtomicBool::new(false)),
            pending_tpl: Mutex::new(TopicPartitionList::new()),
        }
    }

    /// The consumed offsets for the last batch, together with the consumer group metadata
    /// required by `send_offsets_to_transaction`. Returns `None` before the first batch
    /// or when no messages were consumed.
    ///
    /// Used by the exactly-once path in `QueryRunner` — the [`OffsetCommit`] is passed to
    /// [`KafkaSink::add_batch_with_offsets`] so the source offsets are committed inside the
    /// producer transaction instead of separately.
    pub fn pending_offsets(&self) -> Option<OffsetCommit> {
        let tpl = {
            let mut guard = self.pending_tpl.lock();
            let tpl = std::mem::replace(&mut *guard, TopicPartitionList::new());
            tpl
        };
        if tpl.elements().is_empty() {
            return None;
        }
        let consumer_guard = self.consumer.lock();
        let consumer = consumer_guard.as_ref()?;
        let metadata = consumer.group_metadata()?;
        Some(OffsetCommit {
            tpl,
            group_metadata: Arc::new(metadata),
        })
    }
}

impl StreamSource for KafkaSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&self, _time_ms: u64) -> Vec<RecordBatch> {
        // Atomically drain buffer and swap the polled-offset tracker so that
        // `pending_tpl` covers exactly the messages we're about to return.
        let (rows, tpl) = {
            let mut state = self.shared.lock();
            let rows = std::mem::take(&mut state.buffer);
            let tpl = std::mem::replace(&mut state.polled_tpl, TopicPartitionList::new());
            (rows, tpl)
        };
        *self.pending_tpl.lock() = tpl;
        match json_to_batch(&self.schema, &rows) {
            Ok(Some(b)) => vec![b],
            _ => vec![],
        }
    }

    fn start(&self) {
        let brokers = self.brokers.clone();
        let group_id = self.group_id.clone();
        let topics = self.topics.clone();
        let shared = self.shared.clone();
        let stop = self.stop.clone();

        let consumer: BaseConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
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
        let consumer = Arc::new(consumer);
        *self.consumer.lock() = Some(Arc::clone(&consumer));

        let handle = std::thread::Builder::new()
            .name("structured-kafka-source".into())
            .spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    match consumer.poll(Duration::from_millis(200)) {
                        Some(Ok(msg)) => {
                            let mut state = shared.lock();
                            // Track offset+1 (the next-to-consume offset) per Kafka convention.
                            use rdkafka::Offset;
                            let _ = state.polled_tpl.add_partition_offset(
                                msg.topic(),
                                msg.partition(),
                                Offset::Offset(msg.offset() + 1),
                            );
                            if let Some(v) = msg
                                .payload()
                                .and_then(|p| serde_json::from_slice::<serde_json::Value>(p).ok())
                                .filter(serde_json::Value::is_object)
                            {
                                state.buffer.push(v);
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
        *self.consumer.lock() = None;
    }

    /// At-least-once offset commit: advance the consumer group's committed position
    /// for all partitions consumed in the last batch. Called by `QueryRunner` after
    /// the sink successfully writes the batch (so offsets only advance on confirmed
    /// delivery). For exactly-once delivery, use [`pending_offsets`] + the
    /// transactional sink path instead — that path skips `post_batch_commit`.
    fn post_batch_commit(&self, _epoch: u64) {
        // Take the pending TPL. If pending_offsets() was already called (exactly-once
        // path), this will be empty — that's correct since the transaction already
        // committed the offsets.
        let tpl = {
            let mut guard = self.pending_tpl.lock();
            std::mem::replace(&mut *guard, TopicPartitionList::new())
        };
        if tpl.elements().is_empty() {
            return;
        }
        let consumer_guard = self.consumer.lock();
        if let Some(consumer) = consumer_guard.as_ref() {
            if let Err(e) = consumer.commit(&tpl, CommitMode::Sync) {
                log::warn!("KafkaSource: offset commit failed: {e}");
            }
        }
    }
}

/// A sink that produces each emitted row as a JSON-object message to a topic.
///
/// Two delivery modes:
/// - [`KafkaSink::new`] — at-least-once: rows are produced then flushed each batch.
/// - [`KafkaSink::transactional`] — each batch is produced inside one Kafka
///   transaction (begin → produce → commit), so a batch's rows appear atomically
///   and a batch retried after a crash does not double-write to the topic.
pub struct KafkaSink {
    topic: String,
    producer: BaseProducer,
    transactional: bool,
}

impl KafkaSink {
    /// At-least-once sink.
    pub fn new(brokers: &str, topic: &str) -> StructuredResult<Self> {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .map_err(|e| StructuredError::Sink(e.to_string()))?;
        Ok(KafkaSink {
            topic: topic.to_string(),
            producer,
            transactional: false,
        })
    }

    /// Exactly-once-capable sink. The producer is configured with idempotence and a
    /// stable `transactional.id` (derive it from the query id so a restart resumes
    /// the same producer identity and fences the previous one), and each batch is
    /// committed as one Kafka transaction. The transaction is aborted if any row in
    /// the batch fails to produce.
    pub fn transactional(
        brokers: &str,
        topic: &str,
        transactional_id: &str,
    ) -> StructuredResult<Self> {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("enable.idempotence", "true")
            .set("transactional.id", transactional_id)
            .create()
            .map_err(|e| StructuredError::Sink(e.to_string()))?;
        producer
            .init_transactions(Duration::from_secs(30))
            .map_err(|e| StructuredError::Sink(e.to_string()))?;
        Ok(KafkaSink {
            topic: topic.to_string(),
            producer,
            transactional: true,
        })
    }

    /// Produce every row of every batch as a JSON message. Does not flush/commit —
    /// the caller decides between a transaction commit and a plain flush.
    fn produce_rows(&self, batches: &[RecordBatch]) -> StructuredResult<()> {
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
        Ok(())
    }
}

impl Sink for KafkaSink {
    fn add_batch(&self, _epoch: u64, batches: &[RecordBatch]) -> StructuredResult<()> {
        if !self.transactional {
            self.produce_rows(batches)?;
            return self
                .producer
                .flush(Duration::from_secs(5))
                .map_err(|e| StructuredError::Sink(e.to_string()));
        }

        self.producer
            .begin_transaction()
            .map_err(|e| StructuredError::Sink(e.to_string()))?;
        match self.produce_rows(batches) {
            Ok(()) => self
                .producer
                .commit_transaction(Duration::from_secs(30))
                .map_err(|e| StructuredError::Sink(e.to_string())),
            Err(e) => {
                // Best-effort abort so the partial batch is never visible to readers.
                let _ = self.producer.abort_transaction(Duration::from_secs(30));
                Err(e)
            }
        }
    }

    /// Exactly-once batch: begin → produce → commit source offsets inside the
    /// transaction → commit. The source offsets and output rows become visible to
    /// read-committed consumers atomically.
    ///
    /// Falls back to `add_batch` (ignoring offsets) when this sink is not transactional.
    fn add_batch_with_offsets(
        &self,
        epoch: u64,
        batches: &[RecordBatch],
        offsets: &OffsetCommit,
    ) -> StructuredResult<()> {
        if !self.transactional {
            return self.add_batch(epoch, batches);
        }

        self.producer
            .begin_transaction()
            .map_err(|e| StructuredError::Sink(e.to_string()))?;

        match self.produce_rows(batches) {
            Ok(()) => {
                self.producer
                    .send_offsets_to_transaction(
                        &offsets.tpl,
                        offsets.group_metadata.as_ref(),
                        Duration::from_secs(30),
                    )
                    .map_err(|e| StructuredError::Sink(e.to_string()))?;
                self.producer
                    .commit_transaction(Duration::from_secs(30))
                    .map_err(|e| StructuredError::Sink(e.to_string()))
            }
            Err(e) => {
                let _ = self.producer.abort_transaction(Duration::from_secs(30));
                Err(e)
            }
        }
    }
}

/// A structured-streaming source over Kafka using the Direct (pull-based) model.
///
/// Each `next_batch` call polls Kafka metadata for high-water marks, builds
/// per-partition `OffsetRange`s, and — in distributed mode — dispatches one
/// `KafkaConsume` task per range to a worker via `Context::dispatch_pipeline`
/// (the same staged-pipeline mechanism as `DirectKafkaInputDStream`). In local
/// mode it consumes directly on the calling thread.
///
/// Offsets advance only in `post_batch_commit`, which the engine calls after
/// the sink has successfully written the batch (at-least-once delivery).
pub struct KafkaDirectSource {
    schema: SchemaRef,
    brokers: String,
    topics: Vec<String>,
    max_records_per_partition: usize,
    /// `Some` to dispatch consume tasks through the distributed scheduler;
    /// `None` to consume locally on the calling thread.
    ctx: Option<Arc<Context>>,
    offset_tracker: Mutex<OffsetTracker>,
    /// Ranges consumed by the most recent `next_batch`, pending commit.
    pending_ranges: Mutex<Vec<OffsetRange>>,
}

impl KafkaDirectSource {
    /// Build a Direct-model source. Each message payload must be a JSON object
    /// whose fields match `schema`.
    ///
    /// `ctx`: pass the streaming `Context` to dispatch consume tasks to workers
    /// when it is distributed; `next_batch` consumes locally when `ctx` is local
    /// or `None`.
    pub fn new(
        schema: SchemaRef,
        brokers: &str,
        topics: &[&str],
        max_records_per_partition: usize,
        ctx: Option<Arc<Context>>,
    ) -> Self {
        KafkaDirectSource {
            schema,
            brokers: brokers.to_string(),
            topics: topics.iter().map(|s| s.to_string()).collect(),
            max_records_per_partition,
            ctx,
            offset_tracker: Mutex::new(OffsetTracker::default()),
            pending_ranges: Mutex::new(Vec::new()),
        }
    }

    /// Restore offset state from a previous checkpoint map (called on recovery).
    pub fn restore_offsets(&self, saved: std::collections::HashMap<(String, i32), i64>) {
        self.offset_tracker.lock().restore(saved);
    }

    /// Snapshot current committed offsets for checkpointing.
    pub fn offset_snapshot(&self) -> std::collections::HashMap<(String, i32), i64> {
        self.offset_tracker.lock().snapshot()
    }

    fn fetch_offset_ranges(&self) -> Vec<OffsetRange> {
        let consumer: BaseConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", "atomic-structured-direct-meta")
            .set("enable.auto.commit", "false")
            .create()
        {
            Ok(c) => c,
            Err(e) => {
                log::error!("KafkaDirectSource: metadata consumer create failed: {e}");
                return vec![];
            }
        };

        let mut ranges = Vec::new();
        let tracker = self.offset_tracker.lock();

        for topic in &self.topics {
            let metadata =
                match consumer.fetch_metadata(Some(topic.as_str()), Duration::from_secs(10)) {
                    Ok(m) => m,
                    Err(e) => {
                        log::error!("KafkaDirectSource: fetch_metadata({topic}) failed: {e}");
                        continue;
                    }
                };

            for tm in metadata.topics() {
                for pm in tm.partitions() {
                    let part = pm.id();
                    let start = tracker.start_for(topic, part);

                    let hwm = match consumer.fetch_watermarks(topic, part, Duration::from_secs(5)) {
                        Ok((_lo, hi)) => hi,
                        Err(e) => {
                            log::warn!(
                                "KafkaDirectSource: fetch_watermarks({topic}/{part}) failed: {e}"
                            );
                            continue;
                        }
                    };

                    if hwm <= start {
                        continue;
                    }

                    let end = (start + self.max_records_per_partition as i64).min(hwm);
                    ranges.push(OffsetRange {
                        topic: topic.clone(),
                        partition: part,
                        start_offset: start,
                        end_offset: end,
                    });
                }
            }
        }
        ranges
    }

    /// Consume one offset range on the calling thread (local-mode path).
    fn consume_range_local(&self, range: &OffsetRange) -> Vec<String> {
        let consumer: BaseConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", "atomic-structured-direct-local")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "none")
            .create()
        {
            Ok(c) => c,
            Err(e) => {
                log::error!("KafkaDirectSource local consume: consumer create failed: {e}");
                return vec![];
            }
        };

        let mut tpl = rdkafka::TopicPartitionList::new();
        if tpl
            .add_partition_offset(
                &range.topic,
                range.partition,
                rdkafka::Offset::Offset(range.start_offset),
            )
            .is_err()
            || consumer.assign(&tpl).is_err()
        {
            return vec![];
        }

        let mut msgs = Vec::new();
        let poll_timeout = Duration::from_millis(500);
        loop {
            if msgs.len() >= self.max_records_per_partition {
                break;
            }
            match consumer.poll(poll_timeout) {
                Some(Ok(msg)) => {
                    if msg.offset() >= range.end_offset {
                        break;
                    }
                    let text = msg
                        .payload()
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                        .unwrap_or_default();
                    msgs.push(text);
                }
                Some(Err(e)) => log::warn!("KafkaDirectSource local poll: {e}"),
                None => break,
            }
        }
        msgs
    }

    /// Dispatch one consume task per range to the distributed scheduler and
    /// collect the resulting messages.
    fn consume_ranges_distributed(
        &self,
        ctx: &Arc<Context>,
        ranges: &[OffsetRange],
    ) -> Vec<String> {
        use atomic_data::distributed::WireDecode;

        let (source_partitions, ops) =
            build_staged_pipeline(&self.brokers, ranges, self.max_records_per_partition);
        let raw = match ctx.dispatch_pipeline(source_partitions, ops) {
            Ok(r) => r,
            Err(e) => {
                log::error!("KafkaDirectSource: dispatch_pipeline failed: {e}");
                return vec![];
            }
        };

        let mut messages = Vec::new();
        for bytes in raw {
            match Vec::<String>::decode_wire(&bytes) {
                Ok(msgs) => messages.extend(msgs),
                Err(e) => log::error!("KafkaDirectSource: result decode failed: {e}"),
            }
        }
        messages
    }
}

impl StreamSource for KafkaDirectSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn next_batch(&self, _time_ms: u64) -> Vec<RecordBatch> {
        let ranges = self.fetch_offset_ranges();
        if ranges.is_empty() {
            return vec![];
        }

        let messages = match &self.ctx {
            Some(ctx) if ctx.is_distributed() => self.consume_ranges_distributed(ctx, &ranges),
            _ => ranges
                .iter()
                .flat_map(|r| self.consume_range_local(r))
                .collect(),
        };

        *self.pending_ranges.lock() = ranges;

        let rows: Vec<serde_json::Value> = messages
            .iter()
            .filter_map(|m| serde_json::from_str::<serde_json::Value>(m).ok())
            .filter(serde_json::Value::is_object)
            .collect();

        match json_to_batch(&self.schema, &rows) {
            Ok(Some(b)) => vec![b],
            _ => vec![],
        }
    }

    fn post_batch_commit(&self, _epoch: u64) {
        let ranges = self.pending_ranges.lock().split_off(0);
        if ranges.is_empty() {
            return;
        }
        let mut tracker = self.offset_tracker.lock();
        for r in &ranges {
            tracker.commit(&r.topic, r.partition, r.end_offset);
        }
    }
}

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
