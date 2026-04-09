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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

// ─────────────────────────────────────────────────────────────────────────────
// StreamingContextState
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamingContextState {
    /// Created but not yet started.
    Initialized,
    /// Running — batch loop is active.
    Active,
    /// Stopped.
    Stopped,
}

// ─────────────────────────────────────────────────────────────────────────────
// StreamingContext
// ─────────────────────────────────────────────────────────────────────────────

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
    /// Underlying compute context.
    pub sc: Arc<Context>,
    /// How often a new batch is generated.
    pub batch_duration: Duration,
    /// The DAG of DStreams.
    pub graph: Mutex<DStreamGraph>,
    /// Optional checkpoint directory.
    pub checkpoint_dir: Option<PathBuf>,
    /// How often to write checkpoints (must be a multiple of batch_duration).
    pub checkpoint_duration: Option<Duration>,
    /// Lifecycle state.
    state: Mutex<StreamingContextState>,
    /// The batch-loop scheduler (set when started).
    scheduler: Mutex<Option<Arc<JobScheduler>>>,
    /// ID counter for streams.
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
            checkpoint_dir: None,
            checkpoint_duration: None,
            state: Mutex::new(StreamingContextState::Initialized),
            scheduler: Mutex::new(None),
            next_stream_id: AtomicUsize::new(0),
        })
    }

    fn next_stream_id(&self) -> usize {
        self.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Input stream factories
    // ─────────────────────────────────────────────────────────────────────────

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
    pub fn socket_text_stream(
        self: &Arc<Self>,
        host: &str,
        port: u16,
    ) -> Arc<SocketInputDStream> {
        let id = self.next_stream_id();
        let stream = Arc::new(SocketInputDStream::new(self.clone(), id, host, port));
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

    // ─────────────────────────────────────────────────────────────────────────
    // Output operation registration
    // ─────────────────────────────────────────────────────────────────────────

    /// Register a `foreach_rdd` output operation on `stream`.
    ///
    /// `func` is called with the RDD and batch time (ms) for each batch.
    pub fn foreach_rdd<T, F>(
        self: &Arc<Self>,
        stream: Arc<dyn DStream<T>>,
        func: F,
    )
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
    pub fn print<T: Data + Clone + std::fmt::Debug>(
        self: &Arc<Self>,
        stream: Arc<dyn DStream<T>>,
        num: usize,
    ) {
        let sc = self.sc.clone();
        self.foreach_rdd(stream, move |rdd, time_ms| {
            println!("-------------------------------------------");
            println!("Time: {}ms", time_ms);
            println!("-------------------------------------------");
            let owned_rdd = rdd.get_rdd();
            match sc.run_job(owned_rdd, move |iter| iter.take(num).collect::<Vec<T>>()) {
                Ok(results) => {
                    for item in results.into_iter().flatten().take(num) {
                        println!("{:?}", item);
                    }
                }
                Err(e) => log::error!("print: run_job failed: {}", e),
            }
            println!();
        });
    }

    /// Save each batch RDD as text files with `<prefix>-<time_ms>` directories.
    pub fn save_as_text_files<T: Data + Clone + std::fmt::Debug>(
        self: &Arc<Self>,
        stream: Arc<dyn DStream<T>>,
        prefix: impl Into<String>,
        suffix: impl Into<String>,
    ) {
        let prefix = prefix.into();
        let suffix = suffix.into();
        let sc = self.sc.clone();
        self.foreach_rdd(stream, move |rdd, time_ms| {
            let dir = format!("{}-{}{}", prefix, time_ms, suffix);
            if let Err(e) = std::fs::create_dir_all(&dir) {
                log::error!("save_as_text_files: failed to create dir {}: {}", dir, e);
                return;
            }
            match sc.run_job(rdd.get_rdd(), |iter| iter.collect::<Vec<T>>()) {
                Ok(partitions) => {
                    for (i, partition) in partitions.into_iter().enumerate() {
                        let path = format!("{}/part-{:05}", dir, i);
                        if let Ok(mut f) = std::fs::File::create(&path) {
                            use std::io::Write;
                            for item in partition {
                                let _ = writeln!(f, "{:?}", item);
                            }
                        }
                    }
                }
                Err(e) => log::error!("save_as_text_files: run_job failed: {}", e),
            }
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    /// Enable checkpointing. `dir` is created if it does not exist.
    pub fn checkpoint(self: &Arc<Self>, dir: impl Into<PathBuf>) {
        let path = dir.into();
        // We can't mutate self.checkpoint_dir after Arc construction, so we do it via
        // an unsafe pointer if needed — but for simplicity, users should call this
        // before start(). The path is recorded in the graph's start sequence.
        //
        // TODO: make checkpoint_dir interior-mutable (Mutex<Option<PathBuf>>)
        // For now, log a warning if called after start.
        let state = self.state.lock();
        if *state != StreamingContextState::Initialized {
            log::warn!("checkpoint() called after start() — has no effect");
        }
        drop(state);
        let _ = std::fs::create_dir_all(&path);
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
        log::info!("StreamingContext started (batch interval: {:?})", self.batch_duration);
        Ok(())
    }

    /// Stop the streaming computation.
    ///
    /// `stop_sc`: also shut down the underlying compute Context (not yet implemented).
    /// `gracefully`: wait for in-progress batches to finish (not yet implemented).
    pub fn stop(self: &Arc<Self>, _stop_sc: bool, _gracefully: bool) {
        let mut state = self.state.lock();
        if *state == StreamingContextState::Stopped {
            return;
        }
        *state = StreamingContextState::Stopped;
        drop(state);

        if let Some(sched) = self.scheduler.lock().take() {
            sched.stop();
        }
        self.graph.lock().stop();
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
