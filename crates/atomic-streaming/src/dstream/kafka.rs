//! Kafka input source — `KafkaInputDStream`.
//!
//! Gated behind the `kafka` feature (pulls in `librdkafka`). It mirrors
//! [`SocketInputDStream`](super::input::SocketInputDStream): a background thread
//! polls a Kafka topic and buffers message payloads as `String`; each batch
//! `compute()` drains the buffer into a `ParallelCollection<String>` RDD.
//!
//! ```ignore
//! let ssc = StreamingContext::new(ctx, Duration::from_secs(1));
//! let stream = ssc.kafka_stream("localhost:9092", "my-group", &["events"]);
//! ssc.foreach_rdd(stream, |rdd, _t| { /* ... */ });
//! ```
//!
//! Offsets are committed by the consumer group (auto-commit). Distributed
//! receiver placement (running the consumer on a worker rather than the driver)
//! depends on distributed streaming, which is not yet implemented; today the
//! consumer runs in the driver process like the socket/file sources.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use atomic_compute::rdd::ParallelCollection;
use atomic_data::rdd::Rdd;
use parking_lot::Mutex;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;

use crate::context::StreamingContext;
use crate::dstream::input::InputDStreamState;
use crate::dstream::{DStream, DStreamBase, InputStreamBase};

/// A DStream whose elements are payloads read from a Kafka topic.
pub struct KafkaInputDStream {
    state: InputDStreamState,
    brokers: String,
    group_id: String,
    topics: Vec<String>,
    buffer: Arc<Mutex<Vec<String>>>,
    consumer_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    stop_flag: Arc<AtomicBool>,
    generated: Mutex<HashMap<u64, Arc<dyn Rdd<Item = String>>>>,
}

impl KafkaInputDStream {
    pub fn new(
        ssc: Arc<StreamingContext>,
        stream_id: usize,
        brokers: &str,
        group_id: &str,
        topics: &[&str],
    ) -> Self {
        KafkaInputDStream {
            state: InputDStreamState::new(ssc, stream_id),
            brokers: brokers.to_string(),
            group_id: group_id.to_string(),
            topics: topics.iter().map(|s| s.to_string()).collect(),
            buffer: Arc::new(Mutex::new(Vec::new())),
            consumer_handle: Mutex::new(None),
            stop_flag: Arc::new(AtomicBool::new(false)),
            generated: Mutex::new(HashMap::new()),
        }
    }
}

impl DStreamBase for KafkaInputDStream {
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
        true
    }

    fn start(&self) {
        let brokers = self.brokers.clone();
        let group_id = self.group_id.clone();
        let topics = self.topics.clone();
        let buffer = self.buffer.clone();
        let stop = self.stop_flag.clone();

        let handle = std::thread::Builder::new()
            .name(format!("kafka-receiver-{}", self.state.stream_id))
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
                        log::error!("KafkaInputDStream: failed to create consumer: {e}");
                        return;
                    }
                };
                let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
                if let Err(e) = consumer.subscribe(&topic_refs) {
                    log::error!("KafkaInputDStream: subscribe failed: {e}");
                    return;
                }

                while !stop.load(Ordering::SeqCst) {
                    match consumer.poll(Duration::from_millis(200)) {
                        Some(Ok(msg)) => {
                            if let Some(Ok(s)) = msg.payload().map(std::str::from_utf8) {
                                buffer.lock().push(s.to_string());
                            }
                        }
                        Some(Err(e)) => log::warn!("KafkaInputDStream: poll error: {e}"),
                        None => {} // poll timeout — loop and re-check stop flag
                    }
                }
            })
            .expect("failed to spawn kafka receiver thread");

        *self.consumer_handle.lock() = Some(handle);
    }

    fn stop(&self) {
        self.stop_flag.store(true, Ordering::SeqCst);
        if let Some(h) = self.consumer_handle.lock().take() {
            let _ = h.join();
        }
    }
}

impl DStream<String> for KafkaInputDStream {
    fn compute(&self, _valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = String>>> {
        let msgs: Vec<String> = self.buffer.lock().drain(..).collect();
        let ctx = &self.state.ssc.sc;
        let id = ctx.new_rdd_id();
        let nparts = msgs.len().max(1).min(num_cpus::get());
        Some(Arc::new(ParallelCollection::new(id, msgs, nparts)))
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

impl InputStreamBase for KafkaInputDStream {}

#[cfg(test)]
mod tests {
    use super::*;
    use atomic_compute::context::Context;

    // Broker-free: construct the stream and drain an empty buffer. The consumer
    // thread is never started, so no network is touched.
    #[test]
    fn empty_compute_yields_empty_rdd() {
        let sc = Context::local().unwrap();
        let ssc = StreamingContext::new(sc.clone(), Duration::from_millis(100));
        let stream = ssc.kafka_stream("localhost:9092", "g", &["t"]);

        let rdd = stream.compute(0).expect("compute should yield an RDD");
        let parts = sc.run_job(rdd, |it| it.count()).unwrap();
        assert_eq!(parts.iter().sum::<usize>(), 0);
    }
}
