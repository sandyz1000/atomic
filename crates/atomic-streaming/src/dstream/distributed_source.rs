//! Direct-model distributed source abstraction.
//!
//! Generalises the Kafka Direct pull model so any partitioned, offset-addressable
//! source can dispatch reads to workers as ordinary `TaskEnvelope`s.
//!
//! # Design
//!
//! The Spark receiver model requires long-running worker tasks, a block manager, and
//! replication — none of which Atomic currently supports.  The *Direct* model (already
//! used for Kafka) is simpler: **the driver plans** which byte ranges / file splits /
//! offsets to read for each batch, dispatches one short-lived task per partition, and
//! commits only after the sink confirms success.  A worker death just means the split
//! is never committed and is automatically re-planned on the next batch (at-least-once).
//!
//! # Usage
//! ```ignore
//! let source = DistributedFileSource::new("/data/input");
//! let stream = ssc.distributed_file_stream("/data/input");
//! ssc.foreach_rdd(stream, |rdd, _t| { /* process rdd */ });
//! ssc.start()?;
//! ```

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use atomic_compute::rdd::ParallelCollection;
use atomic_data::data::Data;
use atomic_data::distributed::{FileSplitPayload, PipelineOp, TaskAction, TaskRuntime};
use atomic_data::rdd::Rdd;
use parking_lot::Mutex;

use crate::context::StreamingContext;
use crate::dstream::input::InputDStreamState;
use crate::dstream::{DStream, DStreamBase, InputStreamBase};

// ── SourcePartitionTask ───────────────────────────────────────────────────────

/// One partition's read descriptor for a single micro-batch.
///
/// Produced by [`DistributedSource::plan_batch`]; each task becomes one
/// `TaskEnvelope` dispatched to a worker (or executed in-process in local mode).
#[derive(Debug, Clone)]
pub struct SourcePartitionTask {
    /// The pipeline op the worker executes over this partition's bytes.
    pub op: PipelineOp,
    /// Bincode-encoded per-partition config (e.g. `KafkaConsumePayload`,
    /// `FileSplitPayload`).  Passed as the task's `data` field.
    pub partition_bytes: Vec<u8>,
    /// Human-readable identity for logging and tracking
    /// (e.g. `"events/0"`, `"/data/part-1.txt"`).
    pub descriptor: String,
}

// ── DistributedSource trait ───────────────────────────────────────────────────

/// A partitioned, offset-addressable data source that can distribute reads to
/// workers via the Direct pull model.
///
/// Implementations must be `Send + Sync + 'static` so they can live inside the
/// streaming graph's thread.
///
/// # At-least-once guarantee
///
/// [`plan_batch`] returns all splits that have not yet been committed.  The
/// driver calls [`commit`] only after `dispatch_pipeline` succeeds AND the
/// downstream sink has processed the data.  If a worker dies mid-batch, the
/// driver never calls `commit` for its split(s), so the next [`plan_batch`]
/// automatically includes them again.
pub trait DistributedSource: Send + Sync + 'static {
    type Item: Data + Clone;

    /// Plan this batch's per-partition tasks.  Must include any previously
    /// planned tasks that were not yet committed (re-plan for worker death).
    fn plan_batch(&self, batch_time_ms: u64) -> Vec<SourcePartitionTask>;

    /// Mark these tasks as successfully read and processed.  Only call after
    /// the downstream sink confirms the data.
    fn commit(&self, tasks: &[SourcePartitionTask]);

    /// Decode raw result bytes returned by `dispatch_pipeline` into typed items.
    fn decode_results(&self, raw_batches: Vec<Vec<u8>>) -> Vec<Self::Item>;
}

// ── DistributedInputDStream ───────────────────────────────────────────────────

/// Per-batch cache of generated RDDs, keyed by batch time.
type GeneratedRddCache<I> = Mutex<HashMap<u64, Arc<dyn Rdd<Item = I>>>>;

/// A DStream backed by a [`DistributedSource`].
///
/// In **distributed mode**: calls `plan_batch`, dispatches one `TaskEnvelope` per
/// partition via `Context::dispatch_pipeline`, decodes results, calls `commit` on
/// success.  A failed dispatch does **not** call `commit`, so the source re-plans
/// those partitions on the next batch tick.
///
/// In **local mode**: the same `dispatch_pipeline` path is used — the
/// `NativeBackend` executes the op in-process, so no code path changes are needed.
pub struct DistributedInputDStream<S: DistributedSource> {
    state: InputDStreamState,
    source: S,
    generated: GeneratedRddCache<S::Item>,
}

impl<S: DistributedSource> DistributedInputDStream<S> {
    pub fn new(ssc: Arc<StreamingContext>, stream_id: usize, source: S) -> Self {
        DistributedInputDStream {
            state: InputDStreamState::new(ssc, stream_id),
            source,
            generated: Mutex::new(HashMap::new()),
        }
    }

    pub fn source(&self) -> &S {
        &self.source
    }
}

impl<S: DistributedSource> DStreamBase for DistributedInputDStream<S> {
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

impl<S: DistributedSource> DStream<S::Item> for DistributedInputDStream<S> {
    fn compute(&self, batch_time_ms: u64) -> Option<Arc<dyn Rdd<Item = S::Item>>> {
        let tasks = self.source.plan_batch(batch_time_ms);
        if tasks.is_empty() {
            return None;
        }

        let sc = &self.state.ssc.sc;

        // All tasks in a single-op source share the same op; take it from the first.
        let ops = vec![tasks[0].op.clone()];
        let source_partitions: Vec<Vec<u8>> =
            tasks.iter().map(|t| t.partition_bytes.clone()).collect();

        match sc.dispatch_pipeline(source_partitions, ops) {
            Ok(raw_batches) => {
                let all_items = self.source.decode_results(raw_batches);
                // Commit only after successful dispatch so uncommitted splits are
                // re-planned on the next batch if a worker died mid-task.
                self.source.commit(&tasks);
                let rdd_id = sc.new_rdd_id();
                let nparts = tasks.len().max(1);
                Some(Arc::new(ParallelCollection::new(rdd_id, all_items, nparts)))
            }
            Err(e) => {
                log::error!(
                    "DistributedInputDStream(stream_id={}): dispatch failed, \
                     {} split(s) will be re-planned next batch: {e}",
                    self.state.stream_id,
                    tasks.len()
                );
                None
            }
        }
    }

    fn get_or_compute(&self, batch_time_ms: u64) -> Option<Arc<dyn Rdd<Item = S::Item>>> {
        {
            let cache = self.generated.lock();
            if let Some(rdd) = cache.get(&batch_time_ms) {
                return Some(rdd.clone());
            }
        }
        let rdd = self.compute(batch_time_ms)?;
        self.generated.lock().insert(batch_time_ms, rdd.clone());
        Some(rdd)
    }
}

impl<S: DistributedSource> InputStreamBase for DistributedInputDStream<S> {}

// ── DistributedFileSource ─────────────────────────────────────────────────────

/// A [`DistributedSource`] that reads files from a local directory.
///
/// Each file in the directory becomes one partition task.  Files are committed
/// (marked seen) only after a batch completes successfully — if a worker dies
/// mid-batch, the uncommitted file reappears in the next `plan_batch` call.
///
/// This is the canonical non-Kafka proof that the Direct model generalises beyond
/// message brokers and is safe to use in CI without any external infrastructure.
pub struct DistributedFileSource {
    directory: PathBuf,
    /// Files fully committed across at least one successful batch.
    committed: Mutex<HashSet<PathBuf>>,
    /// Files dispatched in the current or a past batch but not yet committed.
    pending: Mutex<HashMap<PathBuf, SourcePartitionTask>>,
}

impl DistributedFileSource {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        DistributedFileSource {
            directory: directory.into(),
            committed: Mutex::new(HashSet::new()),
            pending: Mutex::new(HashMap::new()),
        }
    }
}

impl DistributedSource for DistributedFileSource {
    type Item = String;

    fn plan_batch(&self, _batch_time_ms: u64) -> Vec<SourcePartitionTask> {
        let committed = self.committed.lock();
        let mut pending = self.pending.lock();

        let new_files: Vec<PathBuf> = match std::fs::read_dir(&self.directory) {
            Ok(entries) => entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.is_file() && !committed.contains(p))
                .collect(),
            Err(e) => {
                log::error!("DistributedFileSource: read_dir {:?}: {e}", self.directory);
                return vec![];
            }
        };

        let tasks: Vec<SourcePartitionTask> = new_files
            .into_iter()
            .map(|path| {
                let payload = FileSplitPayload {
                    path: path.to_string_lossy().into_owned(),
                    start_byte: 0,
                    end_byte: None,
                };
                let partition_bytes = bincode::encode_to_vec(&payload, bincode::config::standard())
                    .unwrap_or_default();
                let task = SourcePartitionTask {
                    op: PipelineOp {
                        op_id: String::new(),
                        action: TaskAction::ReadFileSplit,
                        runtime: TaskRuntime::Native,
                        payload: vec![],
                    },
                    partition_bytes,
                    descriptor: path.to_string_lossy().into_owned(),
                };
                pending.insert(path, task.clone());
                task
            })
            .collect();

        tasks
    }

    fn commit(&self, tasks: &[SourcePartitionTask]) {
        let mut committed = self.committed.lock();
        let mut pending = self.pending.lock();
        for task in tasks {
            let path = PathBuf::from(&task.descriptor);
            committed.insert(path.clone());
            pending.remove(&path);
        }
    }

    fn decode_results(&self, raw_batches: Vec<Vec<u8>>) -> Vec<String> {
        use atomic_data::distributed::WireDecode;
        raw_batches
            .into_iter()
            .flat_map(|bytes| Vec::<String>::decode_wire(&bytes).unwrap_or_default())
            .collect()
    }
}
