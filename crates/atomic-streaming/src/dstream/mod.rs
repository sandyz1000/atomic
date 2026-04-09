pub mod input;
pub mod mapped;
pub mod pair;
pub mod shuffle;
pub mod transformed;
pub mod windowed;

use crate::errors::StreamingResult;
use atomic_data::data::Data;
use atomic_data::rdd::Rdd;
use std::sync::Arc;
use std::time::Duration;

// ─────────────────────────────────────────────────────────────────────────────
// Core trait hierarchy
// ─────────────────────────────────────────────────────────────────────────────

/// Untyped, object-safe base for all DStreams.
pub trait DStreamBase: Send + Sync + 'static {
    /// How often this DStream produces RDDs.
    fn slide_duration(&self) -> Duration;
    /// Unique stream ID assigned by StreamingContext.
    fn id(&self) -> usize;
    /// Parent DStreams in the lineage graph.
    fn base_dependencies(&self) -> Vec<Arc<dyn DStreamBase>>;
    /// Returns true if this stream is backed by a network receiver.
    fn is_receiver_input(&self) -> bool {
        false
    }
    /// Called once when the streaming context starts.
    fn initialize(&self, _zero_time_ms: u64) {}
    /// Validate configuration before starting.
    fn validate_at_start(&self) -> StreamingResult<()> {
        Ok(())
    }
    /// Start any background workers (e.g. network listeners).
    fn start(&self) {}
    /// Stop background workers.
    fn stop(&self) {}
    /// Clear cached RDDs older than `time_ms` to free memory.
    fn clear_metadata(&self, _time_ms: u64) {}
}

/// Typed DStream — produces one RDD per batch interval.
pub trait DStream<T: Data + Clone>: DStreamBase {
    /// Produce the RDD for the given batch time. Returns `None` if there is
    /// no data for this batch (e.g. the input queue is empty).
    fn compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = T>>>;

    /// Return the cached RDD for `valid_time_ms`, computing it if needed.
    fn get_or_compute(&self, valid_time_ms: u64) -> Option<Arc<dyn Rdd<Item = T>>>;
}

// ─────────────────────────────────────────────────────────────────────────────
// Output operation — type-erased so the graph can hold mixed output ops
// ─────────────────────────────────────────────────────────────────────────────

/// A single runnable batch job produced by an output operation.
pub struct StreamingJob {
    pub time_ms: u64,
    pub output_op_id: usize,
    func: Box<dyn FnOnce() -> StreamingResult<()> + Send>,
}

impl StreamingJob {
    pub fn new<F>(time_ms: u64, output_op_id: usize, f: F) -> Self
    where
        F: FnOnce() -> StreamingResult<()> + Send + 'static,
    {
        StreamingJob {
            time_ms,
            output_op_id,
            func: Box::new(f),
        }
    }

    /// Execute the job. Consumes self.
    pub fn run(self) -> StreamingResult<()> {
        (self.func)()
    }
}

/// Object-safe trait for DStream output operations.
pub trait OutputOperation: Send + Sync + 'static {
    /// Generate a job for the given batch time, or `None` if this operation
    /// has nothing to do for this batch.
    fn generate_job(&self, time_ms: u64) -> Option<StreamingJob>;
    /// Called once when the streaming context starts.
    fn initialize(&self, _zero_time_ms: u64) {}
    /// Validate before start.
    fn validate_at_start(&self) -> StreamingResult<()> {
        Ok(())
    }
}

/// Marker trait for the untyped input stream side of the graph.
pub trait InputStreamBase: DStreamBase {}

// ─────────────────────────────────────────────────────────────────────────────
// DStreamGraph
// ─────────────────────────────────────────────────────────────────────────────

/// The DAG of DStreams for a single streaming application.
pub struct DStreamGraph {
    pub batch_duration: Option<Duration>,
    pub zero_time_ms: Option<u64>,
    pub start_time_ms: Option<u64>,
    pub remember_duration: Option<Duration>,
    output_streams: Vec<Arc<dyn OutputOperation>>,
    input_streams: Vec<Arc<dyn InputStreamBase>>,
}

impl DStreamGraph {
    pub fn new() -> Self {
        DStreamGraph {
            batch_duration: None,
            zero_time_ms: None,
            start_time_ms: None,
            remember_duration: None,
            output_streams: Vec::new(),
            input_streams: Vec::new(),
        }
    }

    pub fn set_batch_duration(&mut self, duration: Duration) {
        self.batch_duration = Some(duration);
    }

    pub fn add_output_stream(&mut self, op: Arc<dyn OutputOperation>) {
        self.output_streams.push(op);
    }

    pub fn add_input_stream(&mut self, stream: Arc<dyn InputStreamBase>) {
        self.input_streams.push(stream);
    }

    /// Validate the graph before starting.
    pub fn validate(&self) -> StreamingResult<()> {
        use crate::errors::StreamingError;
        if self.batch_duration.is_none() {
            return Err(StreamingError::NoBatchDuration);
        }
        if self.output_streams.is_empty() {
            return Err(StreamingError::NoOutputOperations);
        }
        Ok(())
    }

    /// Start the graph: record timing, initialize output ops, start input streams.
    pub fn start(&mut self, zero_time_ms: u64) {
        self.zero_time_ms = Some(zero_time_ms);
        self.start_time_ms = Some(zero_time_ms);
        for op in &self.output_streams {
            op.initialize(zero_time_ms);
        }
        for is in &self.input_streams {
            is.start();
        }
    }

    pub fn stop(&mut self) {
        for is in &self.input_streams {
            is.stop();
        }
    }

    /// Generate all batch jobs for the given batch time.
    pub fn generate_jobs(&self, time_ms: u64) -> Vec<StreamingJob> {
        self.output_streams
            .iter()
            .enumerate()
            .filter_map(|(id, op)| {
                let mut job = op.generate_job(time_ms)?;
                job.output_op_id = id;
                Some(job)
            })
            .collect()
    }

    pub fn num_receivers(&self) -> usize {
        self.input_streams.iter().filter(|is| is.is_receiver_input()).count()
    }

    pub fn input_stream_ids(&self) -> Vec<usize> {
        self.input_streams.iter().map(|is| is.id()).collect()
    }
}

impl Default for DStreamGraph {
    fn default() -> Self {
        Self::new()
    }
}
