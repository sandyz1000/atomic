use crate::dstream::distributed_source::{DistributedFileSource, DistributedInputDStream};
use crate::dstream::input::{FileInputDStream, QueueInputDStream, SocketInputDStream};
use crate::dstream::mapped::ForEachDStream;
use crate::dstream::{DStream, DStreamGraph, InputStreamBase, OutputOperation};
use crate::errors::{StreamingError, StreamingResult};
use crate::scheduler::job::JobScheduler;
use atomic_compute::context::Context;
use atomic_data::data::Data;
use atomic_data::rdd::Rdd;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

// StreamingContextState

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamingContextState {
    /// Created but not yet started.
    Initialized,
    /// Running — batch loop is active.
    Active,
    /// Stopped.
    Stopped,
}

// StreamingContext

/// The entry point for a streaming application.
///
/// ```rust,ignore
/// let ctx = Context::local()?;
/// let ssc = StreamingContext::new(ctx, Duration::from_secs(1));
/// let queue = Arc::new(Mutex::new(VecDeque::new()));
/// let stream = ssc.queue_stream(queue.clone(), true);
/// ssc.foreach_rdd(stream, |rdd, t| { /* ... */ });
/// ssc.start()?;
/// ssc.await_termination()?;
/// ```
pub struct StreamingContext {
    pub sc: Arc<Context>,
    pub batch_duration: Duration,
    pub graph: Mutex<DStreamGraph>,
    pub checkpoint_dir: Mutex<Option<PathBuf>>,
    /// Must be a multiple of `batch_duration`.
    pub checkpoint_duration: Option<Duration>,
    state: Mutex<StreamingContextState>,
    /// Set when `start()` is called.
    scheduler: Mutex<Option<Arc<JobScheduler>>>,
    next_stream_id: AtomicUsize,
}

impl StreamingContext {
    /// Create a new StreamingContext wrapping `sc` with the given batch interval.
    pub fn new(sc: Arc<Context>, batch_duration: Duration) -> Arc<Self> {
        let mut graph = DStreamGraph::new();
        graph.set_batch_duration(batch_duration);
        Arc::new(StreamingContext {
            sc,
            batch_duration,
            graph: Mutex::new(graph),
            checkpoint_dir: Mutex::new(None),
            checkpoint_duration: None,
            state: Mutex::new(StreamingContextState::Initialized),
            scheduler: Mutex::new(None),
            next_stream_id: AtomicUsize::new(0),
        })
    }

    fn next_stream_id(&self) -> usize {
        self.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }

    // Input stream factories

    /// Create a DStream from a queue of pre-built RDDs.
    pub fn queue_stream<T: Data + Clone>(
        self: &Arc<Self>,
        queue: Arc<Mutex<VecDeque<Arc<dyn Rdd<Item = T>>>>>,
        one_at_a_time: bool,
    ) -> Arc<QueueInputDStream<T>> {
        let id = self.next_stream_id();
        let stream = Arc::new(QueueInputDStream::new(
            self.clone(),
            id,
            queue,
            one_at_a_time,
        ));
        self.graph
            .lock()
            .add_input_stream(stream.clone() as Arc<dyn InputStreamBase>);
        stream
    }

    /// Create a DStream that reads text lines from a TCP socket.
    pub fn socket_text_stream(self: &Arc<Self>, host: &str, port: u16) -> Arc<SocketInputDStream> {
        let id = self.next_stream_id();
        let stream = Arc::new(SocketInputDStream::new(self.clone(), id, host, port));
        self.graph
            .lock()
            .add_input_stream(stream.clone() as Arc<dyn InputStreamBase>);
        stream
    }

    /// Create a DStream that consumes message payloads (as UTF-8 strings) from
    /// Kafka topics. Requires the `kafka` feature.
    #[cfg(feature = "kafka")]
    pub fn kafka_stream(
        self: &Arc<Self>,
        brokers: &str,
        group_id: &str,
        topics: &[&str],
    ) -> Arc<crate::dstream::kafka::KafkaInputDStream> {
        let id = self.next_stream_id();
        let stream = Arc::new(crate::dstream::kafka::KafkaInputDStream::new(
            self.clone(),
            id,
            brokers,
            group_id,
            topics,
        ));
        self.graph
            .lock()
            .add_input_stream(stream.clone() as Arc<dyn InputStreamBase>);
        stream
    }

    /// Create a Direct Kafka DStream using the pull-based Direct model (not receivers).
    ///
    /// Per batch the driver polls Kafka metadata for high-water marks, builds per-partition
    /// offset ranges, and dispatches one-shot consume tasks to workers (or consumes locally).
    ///
    /// * `max_records_per_partition` — cap on records per Kafka partition per batch for
    ///   backpressure. `None` uses the default (10 000).
    ///
    /// Requires the `kafka` feature.
    #[cfg(feature = "kafka")]
    pub fn direct_kafka_stream(
        self: &Arc<Self>,
        brokers: &str,
        topics: &[&str],
        max_records_per_partition: Option<usize>,
    ) -> Arc<crate::dstream::kafka_direct::DirectKafkaInputDStream> {
        let id = self.next_stream_id();
        let max = max_records_per_partition.unwrap_or(10_000);
        let stream = Arc::new(crate::dstream::kafka_direct::DirectKafkaInputDStream::new(
            self.clone(),
            id,
            brokers,
            topics,
            max,
        ));
        self.graph
            .lock()
            .add_input_stream(stream.clone() as Arc<dyn InputStreamBase>);
        stream
    }

    /// Create a DStream that watches a local directory for new text files.
    pub fn text_file_stream(
        self: &Arc<Self>,
        directory: impl Into<PathBuf>,
    ) -> Arc<FileInputDStream> {
        let id = self.next_stream_id();
        let stream = Arc::new(FileInputDStream::new(self.clone(), id, directory, true));
        self.graph
            .lock()
            .add_input_stream(stream.clone() as Arc<dyn InputStreamBase>);
        stream
    }

    /// Create a DStream that reads files from a directory using the Direct pull model.
    ///
    /// Each file in `directory` becomes one partition task dispatched to a worker (or
    /// executed in-process in local mode).  New files appearing in subsequent batches are
    /// picked up automatically.  If a dispatch fails (e.g. worker death), the uncommitted
    /// files are automatically re-planned on the next batch — at-least-once delivery.
    ///
    /// Unlike [`text_file_stream`] (which runs on the driver thread), this uses
    /// `Context::dispatch_pipeline` so it integrates with the distributed scheduler.
    pub fn distributed_file_stream(
        self: &Arc<Self>,
        directory: impl Into<PathBuf>,
    ) -> Arc<DistributedInputDStream<DistributedFileSource>> {
        let id = self.next_stream_id();
        let source = DistributedFileSource::new(directory.into());
        let stream = Arc::new(DistributedInputDStream::new(self.clone(), id, source));
        self.graph
            .lock()
            .add_input_stream(stream.clone() as Arc<dyn InputStreamBase>);
        stream
    }

    // Output operation registration

    /// Register a `foreach_rdd` output operation on `stream`.
    ///
    /// `func` is called with the RDD and batch time (ms) for each batch.
    pub fn foreach_rdd<T, F>(self: &Arc<Self>, stream: Arc<dyn DStream<T>>, func: F)
    where
        T: Data + Clone,
        F: Fn(Arc<dyn Rdd<Item = T>>, u64) + Send + Sync + 'static,
    {
        let id = self.next_stream_id();
        let op = ForEachDStream::new(id, stream, func, self.clone());
        self.graph
            .lock()
            .add_output_stream(Arc::new(op) as Arc<dyn OutputOperation>);
    }

    /// Print the first `num` elements of each batch to stdout.
    ///
    /// In distributed mode, uses `Context::collect_rdd` which routes through
    /// `dispatch_pipeline` / `run_pending_shuffle_stages` as appropriate.
    pub fn print<T>(self: &Arc<Self>, stream: Arc<dyn DStream<T>>, num: usize)
    where
        T: Data + Clone + std::fmt::Debug + atomic_data::distributed::WireDecode,
        Vec<T>: atomic_data::distributed::WireDecode,
    {
        let sc = self.sc.clone();
        self.foreach_rdd(stream, move |rdd, time_ms| {
            println!("-------------------------------------------");
            println!("Time: {}ms", time_ms);
            println!("-------------------------------------------");
            match sc.collect_rdd(rdd) {
                Ok(items) => {
                    for item in items.into_iter().take(num) {
                        println!("{:?}", item);
                    }
                }
                Err(e) => log::error!("print: collect_rdd failed: {}", e),
            }
            println!();
        });
    }

    /// Save each batch RDD as text files with `<prefix>-<time_ms>` directories.
    ///
    /// In distributed mode, uses `Context::collect_rdd` for distribution-aware collection.
    pub fn save_as_text_files<T>(
        self: &Arc<Self>,
        stream: Arc<dyn DStream<T>>,
        prefix: impl Into<String>,
        suffix: impl Into<String>,
    ) where
        T: Data + Clone + std::fmt::Debug + atomic_data::distributed::WireDecode,
        Vec<T>: atomic_data::distributed::WireDecode,
    {
        let prefix = prefix.into();
        let suffix = suffix.into();
        let sc = self.sc.clone();
        self.foreach_rdd(stream, move |rdd, time_ms| {
            let dir = format!("{}-{}{}", prefix, time_ms, suffix);
            if let Err(e) = std::fs::create_dir_all(&dir) {
                log::error!("save_as_text_files: failed to create dir {}: {}", dir, e);
                return;
            }
            match sc.collect_rdd(rdd) {
                Ok(items) => {
                    let path = format!("{}/part-00000", dir);
                    if let Ok(mut f) = std::fs::File::create(&path) {
                        use std::io::Write;
                        for item in items {
                            let _ = writeln!(f, "{:?}", item);
                        }
                    }
                }
                Err(e) => log::error!("save_as_text_files: collect_rdd failed: {}", e),
            }
        });
    }

    // Recovery

    /// Create a new `StreamingContext` by restoring from the latest checkpoint in `dir`.
    ///
    /// Returns `None` if no checkpoint exists in `dir`. The caller must re-register
    /// DStreams and output operations before calling `start()` — checkpointing does not
    /// yet serialise the DStream graph itself, only timing metadata.
    ///
    /// # Example
    /// ```rust,ignore
    /// let ssc = StreamingContext::from_checkpoint(sc, "/tmp/my-stream-checkpoint")
    ///     .expect("checkpoint exists")
    ///     .expect("checkpoint readable");
    /// // Re-register streams and output ops here...
    /// ssc.start()?;
    /// ```
    pub fn from_checkpoint(
        sc: Arc<Context>,
        dir: impl Into<PathBuf>,
    ) -> std::io::Result<Option<Arc<Self>>> {
        use crate::checkpoint::Checkpoint;
        let dir = dir.into();
        let cp = match Checkpoint::read_latest(&dir)? {
            Some(c) => c,
            None => return Ok(None),
        };
        let batch_duration = Duration::from_millis(cp.batch_duration_ms);
        let ssc = Self::new(sc, batch_duration);
        *ssc.checkpoint_dir.lock() = Some(dir);
        log::info!(
            "Restored StreamingContext from checkpoint (batch={}ms, last_completed={:?}ms)",
            cp.batch_duration_ms,
            cp.last_completed_batch_time_ms
        );
        Ok(Some(ssc))
    }

    // Lifecycle

    /// Enable checkpointing. `dir` is created if it does not exist.
    pub fn checkpoint(self: &Arc<Self>, dir: impl Into<PathBuf>) {
        let path = dir.into();
        let state = self.state.lock();
        if *state != StreamingContextState::Initialized {
            log::warn!("checkpoint() called after start() — has no effect");
            return;
        }
        drop(state);
        let _ = std::fs::create_dir_all(&path);
        *self.checkpoint_dir.lock() = Some(path.clone());
        log::info!("Checkpointing enabled at {:?}", path);
    }

    /// Start the streaming computation.
    pub fn start(self: &Arc<Self>) -> StreamingResult<()> {
        let mut state = self.state.lock();
        match *state {
            StreamingContextState::Active => return Err(StreamingError::AlreadyStarted),
            StreamingContextState::Stopped => return Err(StreamingError::AlreadyStopped),
            StreamingContextState::Initialized => {}
        }
        self.graph.lock().validate()?;
        let scheduler = JobScheduler::new(self.clone());
        scheduler.start()?;
        *self.scheduler.lock() = Some(scheduler);
        *state = StreamingContextState::Active;
        log::info!(
            "StreamingContext started (batch interval: {:?})",
            self.batch_duration
        );
        Ok(())
    }

    /// Stop the streaming computation.
    ///
    /// If `stop_sc` is `true`, also shuts down the underlying compute `Context`.
    /// If `gracefully` is `true`, waits for the current in-progress batch to
    /// complete before stopping (the batch loop already joins on stop, so this
    /// parameter is effectively always honored).
    pub fn stop(self: &Arc<Self>, stop_sc: bool, _gracefully: bool) {
        let mut state = self.state.lock();
        if *state == StreamingContextState::Stopped {
            return;
        }
        *state = StreamingContextState::Stopped;
        drop(state);

        if let Some(sched) = self.scheduler.lock().take() {
            sched.stop(); // sets stop flag + joins batch-loop thread (graceful by default)
        }
        self.graph.lock().stop();

        if stop_sc {
            self.sc.shutdown();
        }

        log::info!("StreamingContext stopped");
    }

    /// Block until `stop()` is called (e.g. from a signal handler).
    pub fn await_termination(self: &Arc<Self>) -> StreamingResult<()> {
        loop {
            {
                if *self.state.lock() == StreamingContextState::Stopped {
                    return Ok(());
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Block until `stop()` is called or `timeout` elapses.
    /// Returns `true` if stopped, `false` if timed out.
    pub fn await_termination_or_timeout(
        self: &Arc<Self>,
        timeout: Duration,
    ) -> StreamingResult<bool> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if std::time::Instant::now() >= deadline {
                return Ok(false);
            }
            {
                if *self.state.lock() == StreamingContextState::Stopped {
                    return Ok(true);
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn state(&self) -> StreamingContextState {
        self.state.lock().clone()
    }
}
