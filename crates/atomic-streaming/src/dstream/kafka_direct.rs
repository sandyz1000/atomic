//! Direct-pull Kafka input DStream.
//!
//! Per batch, the driver polls Kafka for high-water marks, computes per-partition
//! offset ranges, and dispatches one-shot consume tasks to workers (one per Kafka
//! partition). No long-running background receiver threads run on workers; the driver
//! controls exactly which offsets each batch reads.
//!
//! Advantages over the receiver model:
//! - Replayable: offsets are checkpointed; on restart or worker loss the same range
//!   is re-consumed (at-least-once delivery).
//! - Consistent hashing pins each Kafka partition to a preferred worker so the
//!   rdkafka metadata cache stays warm across batches.
//! - No block manager — consume is an ordinary `TaskEnvelope` pipeline.
//!
//! # Usage
//! ```ignore
//! let ssc = StreamingContext::new(ctx, Duration::from_secs(2));
//! let stream = ssc.direct_kafka_stream(
//!     "localhost:9092",
//!     &["events"],
//!     Some(500),  // max_records_per_partition_per_batch
//! );
//! ssc.foreach_rdd(stream, |rdd, _t| { /* process rdd */ });
//! ssc.start()?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use atomic_compute::context::Context;
use atomic_compute::rdd::ParallelCollection;
use atomic_data::data::Data;
use atomic_data::dependency::Dependency;
use atomic_data::distributed::{KafkaConsumePayload, StepKind, Step, EngineStep, TaskRuntime};
use atomic_data::error::BaseError;
use atomic_data::rdd::{Rdd, RddBase};
use atomic_data::split::{Split, SplitStruct};
use parking_lot::Mutex;

use rdkafka::Offset as RdOffset;
use rdkafka::TopicPartitionList;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};

use crate::context::StreamingContext;
use crate::dstream::input::InputDStreamState;
use crate::dstream::{DStream, DStreamBase, InputStreamBase};

/// One Kafka partition's offset range for a micro-batch.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OffsetRange {
    pub topic: String,
    pub partition: i32,
    pub start_offset: i64,
    pub end_offset: i64,
}

/// Tracks committed (last consumed) offsets per `(topic, partition)`.
#[derive(Debug, Default)]
pub struct OffsetTracker {
    /// `(topic, partition)` → last-committed offset (exclusive end of the last batch).
    committed: HashMap<(String, i32), i64>,
}

impl OffsetTracker {
    /// Set the committed offset for a partition (called after a batch sinks successfully).
    pub fn commit(&mut self, topic: &str, partition: i32, end_offset: i64) {
        self.committed
            .insert((topic.to_string(), partition), end_offset);
    }

    /// Restore committed offsets from a checkpoint map (on recovery).
    pub fn restore(&mut self, saved: HashMap<(String, i32), i64>) {
        self.committed = saved;
    }

    /// Committed offset for `(topic, partition)`, or 0 if never consumed.
    pub fn start_for(&self, topic: &str, partition: i32) -> i64 {
        self.committed
            .get(&(topic.to_string(), partition))
            .copied()
            .unwrap_or(0)
    }

    /// Snapshot for checkpointing.
    pub fn snapshot(&self) -> HashMap<(String, i32), i64> {
        self.committed.clone()
    }
}

// ── DirectKafkaInputDStream ───────────────────────────────────────────────────

/// DStream that reads from Kafka using the Direct model.
///
/// Each micro-batch computes `OffsetRange`s from high-water marks and dispatches
/// one `TaskEnvelope` per Kafka partition (via the distributed scheduler when in
/// distributed mode, or a local rdkafka call in local mode).
pub struct DirectKafkaInputDStream {
    state: InputDStreamState,
    brokers: String,
    topics: Vec<String>,
    /// Upper bound on records consumed per Kafka partition per batch.
    max_records_per_partition: usize,
    offset_tracker: Mutex<OffsetTracker>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = String>>>>,
}

impl DirectKafkaInputDStream {
    pub fn new(
        ssc: Arc<StreamingContext>,
        stream_id: usize,
        brokers: &str,
        topics: &[&str],
        max_records_per_partition: usize,
    ) -> Self {
        DirectKafkaInputDStream {
            state: InputDStreamState::new(ssc, stream_id),
            brokers: brokers.to_string(),
            topics: topics.iter().map(|s| s.to_string()).collect(),
            max_records_per_partition,
            offset_tracker: Mutex::new(OffsetTracker::default()),
            generated: Mutex::new(HashMap::new()),
        }
    }

    /// Restore offset state from a previous checkpoint map (called on recovery).
    pub fn restore_offsets(&self, saved: HashMap<(String, i32), i64>) {
        self.offset_tracker.lock().restore(saved);
    }

    /// Snapshot current committed offsets for checkpointing.
    pub fn offset_snapshot(&self) -> HashMap<(String, i32), i64> {
        self.offset_tracker.lock().snapshot()
    }

    /// Commit offsets for the given ranges (called after a batch sinks successfully).
    pub fn commit_offsets(&self, ranges: &[OffsetRange]) {
        let mut tracker = self.offset_tracker.lock();
        for r in ranges {
            tracker.commit(&r.topic, r.partition, r.end_offset);
        }
    }

    /// Poll Kafka metadata to find the current high-water marks for all configured
    /// topic partitions, then build per-partition offset ranges for the next batch.
    ///
    /// Caps each range at `max_records_per_partition` records (backpressure).
    fn fetch_offset_ranges(&self) -> Vec<OffsetRange> {
        let consumer: BaseConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", "atomic-direct-meta")
            .set("enable.auto.commit", "false")
            .create()
        {
            Ok(c) => c,
            Err(e) => {
                log::error!("DirectKafka: metadata consumer create failed: {e}");
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
                        log::error!("DirectKafka: fetch_metadata({topic}) failed: {e}");
                        continue;
                    }
                };

            for tm in metadata.topics() {
                for pm in tm.partitions() {
                    let part = pm.id();
                    let start = tracker.start_for(topic, part);

                    // Fetch the high-water mark (latest committed offset on the broker).
                    let hwm = match consumer.fetch_watermarks(topic, part, Duration::from_secs(5)) {
                        Ok((_lo, hi)) => hi,
                        Err(e) => {
                            log::warn!("DirectKafka: fetch_watermarks({topic}/{part}) failed: {e}");
                            continue;
                        }
                    };

                    if hwm <= start {
                        continue; // no new messages
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

    /// Build the batch RDD.
    ///
    /// - **Distributed mode**: creates a `DirectKafkaBatchRdd` whose `extract_staged_pipeline`
    ///   returns the pre-built `KafkaConsume` op chain; `Context::collect_rdd` detects this and
    ///   dispatches the pipeline to workers (one task per Kafka partition).
    /// - **Local mode**: falls back to a `ParallelCollection<String>` built by consuming
    ///   from Kafka directly on the driver thread.
    pub fn build_rdd(
        &self,
        sc: &Arc<Context>,
        ranges: Vec<OffsetRange>,
    ) -> Option<Arc<dyn Rdd<Item = String>>> {
        if ranges.is_empty() {
            return None;
        }

        if sc.is_distributed() {
            let id = sc.new_rdd_id();
            let (source_partitions, steps) =
                build_staged_pipeline(&self.brokers, &ranges, self.max_records_per_partition);
            Some(Arc::new(DirectKafkaBatchRdd {
                id,
                brokers: self.brokers.clone(),
                ranges,
                max_records: self.max_records_per_partition,
                source_partitions,
                steps,
            }))
        } else {
            // Local mode: consume all ranges on the driver and hand back a ParallelCollection.
            let mut all_msgs: Vec<String> = Vec::new();
            for r in &ranges {
                all_msgs.extend(consume_range_local(
                    &self.brokers,
                    r,
                    self.max_records_per_partition,
                ));
            }
            if all_msgs.is_empty() {
                return None;
            }
            let id = sc.new_rdd_id();
            let n = ranges.len().max(1);
            Some(Arc::new(ParallelCollection::new(id, all_msgs, n)))
        }
    }
}

impl DStreamBase for DirectKafkaInputDStream {
    fn slide_duration(&self) -> Duration {
        self.state.batch_duration
    }
    fn id(&self) -> usize {
        self.state.stream_id
    }
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>> {
        vec![]
    }
    fn is_receiver_input(&self) -> bool {
        false // Direct model: no long-running receiver
    }
    fn start(&self) {}
    fn stop(&self) {}
}

impl DStream<String> for DirectKafkaInputDStream {
    fn compute(&self, _valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = String>>> {
        let ranges = self.fetch_offset_ranges();
        let sc = &self.state.ssc.sc;
        self.build_rdd(sc, ranges)
    }

    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = String>>> {
        {
            let cache = self.generated.lock();
            if let Some(rdd) = cache.get(&valid_time_ms) {
                return Some(rdd.clone());
            }
        }
        let rdd = self.compute(valid_time_ms)?;
        self.generated.lock().insert(valid_time_ms, rdd.clone());
        Some(rdd)
    }
}

impl InputStreamBase for DirectKafkaInputDStream {}

// ── DirectKafkaBatchRdd ───────────────────────────────────────────────────────

/// An RDD where each partition corresponds to one Kafka `OffsetRange`.
///
/// In distributed mode, `extract_staged_pipeline()` returns the pre-built
/// `KafkaConsume` op chain so `Context::collect_rdd` dispatches it to workers.
/// In local mode, `compute(part)` fetches directly from Kafka.
pub struct DirectKafkaBatchRdd {
    id: usize,
    brokers: String,
    ranges: Vec<OffsetRange>,
    max_records: usize,
    /// Pre-built source bytes for the distributed staged pipeline.
    /// `source_partitions[i]` = bincode-encoded `KafkaConsumePayload` for partition `i`.
    source_partitions: Vec<Vec<u8>>,
    /// Singleton op: `[Step { action: KafkaConsume, payload: [] }]`.
    steps: Vec<Step>,
}

impl RddBase for DirectKafkaBatchRdd {
    fn get_rdd_id(&self) -> usize {
        self.id
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        vec![]
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        (0..self.ranges.len())
            .map(|i| Box::new(SplitStruct { index: i }) as Box<dyn Split>)
            .collect()
    }

    fn number_of_splits(&self) -> usize {
        self.ranges.len()
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> atomic_data::error::BaseResult<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        Ok(Box::new(
            self.compute(split)?.map(|s| Box::new(s) as Box<dyn Data>),
        ))
    }

    /// Expose the pre-built staged pipeline for distributed dispatch.
    ///
    /// `Context::collect_rdd` checks this and routes to `dispatch_pipeline`
    /// instead of running the compute path locally.
    fn extract_staged_pipeline(
        &self,
    ) -> Option<(Vec<Vec<u8>>, Vec<atomic_data::distributed::Step>)> {
        if self.source_partitions.is_empty() {
            None
        } else {
            Some((self.source_partitions.clone(), self.steps.clone()))
        }
    }
}

impl Rdd for DirectKafkaBatchRdd {
    type Item = String;

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        unimplemented!("DirectKafkaBatchRdd is not cloneable as Arc; use collect_rdd")
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        unimplemented!("DirectKafkaBatchRdd: use get_rdd_id directly")
    }

    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = String>>, BaseError> {
        let idx = split.get_index();
        if idx >= self.ranges.len() {
            return Ok(Box::new(std::iter::empty()));
        }
        let r = &self.ranges[idx];
        let msgs = consume_range_local(&self.brokers, r, self.max_records);
        Ok(Box::new(msgs.into_iter()))
    }
}

// ── Local consume helper ───────────────────────────────────────────────────────

/// Consume one offset range on the calling thread (used in local mode and as
/// fallback in `DirectKafkaBatchRdd::compute`).
fn consume_range_local(brokers: &str, range: &OffsetRange, max_records: usize) -> Vec<String> {
    use rdkafka::message::Message;

    let consumer: BaseConsumer = match ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "atomic-direct-local")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "none")
        .create()
    {
        Ok(c) => c,
        Err(e) => {
            log::error!("DirectKafka local consume: consumer create failed: {e}");
            return vec![];
        }
    };

    let mut tpl = TopicPartitionList::new();
    if tpl
        .add_partition_offset(
            &range.topic,
            range.partition,
            RdOffset::Offset(range.start_offset),
        )
        .is_err()
        || consumer.assign(&tpl).is_err()
    {
        return vec![];
    }

    let mut msgs = Vec::new();
    let poll_timeout = Duration::from_millis(500);
    loop {
        if msgs.len() >= max_records {
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
            Some(Err(e)) => log::warn!("DirectKafka local poll: {e}"),
            None => break,
        }
    }
    msgs
}

// ── StagedPipeline builder for distributed dispatch ───────────────────────────

/// Build the staged-pipeline representation of a batch: one partition per range,
/// each encoded as a `KafkaConsumePayload` (the `KafkaConsume` op reads it from
/// the partition bytes, not from `op.payload`).
pub fn build_staged_pipeline(
    brokers: &str,
    ranges: &[OffsetRange],
    max_records: usize,
) -> (Vec<Vec<u8>>, Vec<Step>) {
    let steps = vec![Step {
        op_id: String::new(), // KafkaConsume dispatched by action variant, not op_id
        kind: StepKind::Engine(EngineStep::KafkaConsume),
        runtime: TaskRuntime::Native,
        payload: vec![], // config is per-partition in source_partitions (data)
    }];

    let source_partitions: Vec<Vec<u8>> = ranges
        .iter()
        .map(|r| {
            let p = KafkaConsumePayload {
                brokers: brokers.to_string(),
                topic: r.topic.clone(),
                partition: r.partition,
                start_offset: r.start_offset,
                end_offset: r.end_offset,
                max_records,
            };
            bincode::encode_to_vec(&p, bincode::config::standard()).unwrap_or_default()
        })
        .collect();

    (source_partitions, steps)
}

// ── DistributedSource impl for DirectKafkaInputDStream ───────────────────────

impl crate::dstream::distributed_source::DistributedSource for DirectKafkaInputDStream {
    type Item = String;

    fn plan_batch(
        &self,
        _batch_time_ms: u64,
    ) -> Vec<crate::dstream::distributed_source::SourcePartitionTask> {
        use crate::dstream::distributed_source::SourcePartitionTask;
        use atomic_data::distributed::{StepKind, Step, EngineStep, TaskRuntime};

        self.fetch_offset_ranges()
            .into_iter()
            .map(|r| {
                let payload = KafkaConsumePayload {
                    brokers: self.brokers.clone(),
                    topic: r.topic.clone(),
                    partition: r.partition,
                    start_offset: r.start_offset,
                    end_offset: r.end_offset,
                    max_records: self.max_records_per_partition,
                };
                let partition_bytes = bincode::encode_to_vec(&payload, bincode::config::standard())
                    .unwrap_or_default();
                SourcePartitionTask {
                    op: Step {
                        op_id: String::new(),
                        kind: StepKind::Engine(EngineStep::KafkaConsume),
                        runtime: TaskRuntime::Native,
                        payload: vec![],
                    },
                    partition_bytes,
                    descriptor: format!("{}/{}", r.topic, r.partition),
                }
            })
            .collect()
    }

    fn commit(&self, tasks: &[crate::dstream::distributed_source::SourcePartitionTask]) {
        let mut tracker = self.offset_tracker.lock();
        for task in tasks {
            if let Ok((payload, _)) = bincode::decode_from_slice::<KafkaConsumePayload, _>(
                &task.partition_bytes,
                bincode::config::standard(),
            ) {
                tracker.commit(&payload.topic, payload.partition, payload.end_offset);
            }
        }
    }

    fn decode_results(&self, raw_batches: Vec<Vec<u8>>) -> Vec<String> {
        use atomic_data::distributed::WireDecode;
        raw_batches
            .into_iter()
            .flat_map(|bytes| {
                Vec::<String>::decode_wire(&bytes)
                    .inspect_err(|e| log::warn!("KafkaSource decode: {e}"))
                    .unwrap_or_default()
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomic_compute::context::Context;

    // Broker-free: the offset tracker must initialize with offset 0.
    #[test]
    fn offset_tracker_default_start() {
        let mut tracker = OffsetTracker::default();
        assert_eq!(tracker.start_for("events", 0), 0);
        tracker.commit("events", 0, 100);
        assert_eq!(tracker.start_for("events", 0), 100);
    }

    // Broker-free: snapshot/restore round-trip.
    #[test]
    fn offset_tracker_snapshot_restore() {
        let mut t1 = OffsetTracker::default();
        t1.commit("t", 0, 50);
        t1.commit("t", 1, 75);
        let snap = t1.snapshot();

        let mut t2 = OffsetTracker::default();
        t2.restore(snap);
        assert_eq!(t2.start_for("t", 0), 50);
        assert_eq!(t2.start_for("t", 1), 75);
    }

    // Broker-free: empty ranges → no RDD.
    #[test]
    fn build_rdd_empty_ranges_returns_none() {
        let sc = Context::local().unwrap();
        let ssc = StreamingContext::new(sc.clone(), Duration::from_millis(100));
        let stream = ssc.direct_kafka_stream("localhost:9092", &["t"], None);
        let rdd = stream.build_rdd(&sc, vec![]);
        assert!(rdd.is_none());
    }
}
