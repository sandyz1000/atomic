use std::fmt::Display;
use std::net::Ipv4Addr;

use crate::data::Data;
use crate::distributed::{EngineAction, Step, StepKind, TaskEnvelope};
use crate::task::{Task, TaskBase, TaskMeta};

/// Everything a `PipelineTask` run produces, beyond the per-partition result bytes
/// themselves: accumulator deltas to merge, and — when the pipeline's steps end in a
/// `ShuffleMap` engine step — the worker's shuffle server URI, which needs registering
/// with `MapOutputTracker` before the reduce side can fetch it.
#[derive(Clone, Default)]
pub struct PipelineTaskOutput {
    pub data: Vec<u8>,
    pub accumulator_deltas: Vec<(usize, Vec<u8>)>,
    pub shuffle_server_uri: Option<String>,
}

/// Executes a `TaskEnvelope`'s `Vec<Step>` pipeline.
///
/// Implemented by `atomic-compute`'s `ComputeEngine` and installed once at `Context` init
/// via `atomic_data::env::set_pipeline_executor` — `atomic-data` can't depend on
/// `atomic-compute` directly, so this is a process-global hook, the same pattern
/// `MapOutputTracker`/`ShuffleCache` already use for driver-set, task-read infrastructure.
pub trait PipelineExecutor: Send + Sync {
    fn execute(&self, envelope: &TaskEnvelope) -> Result<PipelineTaskOutput, String>;
}

/// A `Stage`'s unit of work for a Step-pipeline job — the `_task`-method counterpart to
/// [`ShuffleMapTask`](super::ShuffleMapTask) (`.compute()`-driven) and
/// [`ResultTask`](super::ResultTask) (closure-driven). Carries the partition's source bytes
/// and the `Vec<Step>` to run over them; `Task::run` dispatches through the installed
/// [`PipelineExecutor`] hook rather than calling `.compute()` or invoking a closure, so both
/// `LocalScheduler`'s blocking-thread `submit_task` and `DistributedScheduler`'s worker-RPC
/// `submit_task` drive it identically — the only difference between the two is which thread
/// or machine ends up running the same `TaskEnvelope`.
#[derive(Clone)]
pub struct PipelineTask {
    pub meta: TaskMeta,
    pub source: Vec<u8>,
    pub steps: Vec<Step>,
    pub broadcasts: Vec<(usize, Vec<u8>)>,
    pub output_id: usize,
}

impl PipelineTask {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: usize,
        run_id: usize,
        stage_id: usize,
        partition: usize,
        locs: Vec<Ipv4Addr>,
        source: Vec<u8>,
        steps: Vec<Step>,
        broadcasts: Vec<(usize, Vec<u8>)>,
        output_id: usize,
    ) -> Self {
        PipelineTask {
            meta: TaskMeta::new(task_id, run_id, stage_id, partition, locs, false),
            source,
            steps,
            broadcasts,
            output_id,
        }
    }

    /// Build the wire `TaskEnvelope` for this task — shared by both the in-process
    /// (`LocalScheduler`) and worker-RPC (`DistributedScheduler`) dispatch paths, so the
    /// bytes a local thread runs and the bytes shipped to a remote worker are constructed
    /// identically.
    pub fn to_envelope(&self, attempt_id: usize) -> TaskEnvelope {
        TaskEnvelope::new(
            self.meta.run_id,
            self.meta.stage_id,
            self.meta.task_id,
            attempt_id,
            self.meta.partition,
            format!("pipeline-{}-{}", self.meta.stage_id, self.meta.partition),
            self.steps.clone(),
            self.source.clone(),
        )
        .with_broadcasts(self.broadcasts.clone())
    }

    /// `(shuffle_id, num_output_partitions)` if `self.steps` ends in a `ShuffleMap` engine
    /// step — i.e. this task's completion needs `MapOutputTracker::register_map_output`,
    /// keyed by this task's own partition as the map id. `None` for a task with no shuffle
    /// write (the common case: a plain map/filter/reduce/collect pipeline).
    pub fn shuffle_dep(&self) -> Option<(usize, usize)> {
        self.steps.iter().find_map(|step| match step.kind {
            StepKind::Engine(EngineAction::ShuffleMap {
                shuffle_id,
                num_output_partitions,
            }) => Some((shuffle_id, num_output_partitions)),
            _ => None,
        })
    }
}

impl Display for PipelineTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PipelineTask({}, {})",
            self.meta.stage_id, self.meta.partition
        )
    }
}

impl TaskBase for PipelineTask {
    fn meta(&self) -> &TaskMeta {
        &self.meta
    }
}

impl Task for PipelineTask {
    /// Boxes `(result_bytes, accumulator_deltas)` rather than just the bytes — the only
    /// consumer is `LocalScheduler::run_task`, which unpacks the tuple, merges the deltas
    /// via its own accumulator sink, and re-boxes just the bytes before building the
    /// `CompletionEvent`, so `on_event_success`'s generic downcast (`U = Vec<u8>` for a
    /// pipeline job) never has to know this wrapping exists. The distributed dispatch path
    /// doesn't go through `Task::run` at all (it ships the envelope directly and reads
    /// `accumulator_deltas` off the RPC response), so this only matters for local runs.
    ///
    /// Shuffle-map output registration is handled right here, as a side effect, rather than
    /// threaded through the boxed result: if `self.steps` ends in a `ShuffleMap` step and the
    /// executor reports a `shuffle_server_uri`, register it with `MapOutputTracker`
    /// immediately (keyed by this task's own partition as the map id) so the reduce side can
    /// find it — mirrors what `ShuffleMapTask`'s `on_event_success` arm does for the older
    /// RDD-lineage shuffle path, just registered eagerly per-task instead of once the whole
    /// stage's pending tasks empty out, since `register_map_output` is already incremental.
    fn run(&self, id: usize) -> Result<Box<dyn Data>, Box<dyn std::error::Error>> {
        let executor = crate::env::get_pipeline_executor()
            .ok_or("PipelineTask: no pipeline executor installed")?;
        let envelope = self.to_envelope(id);
        let output = executor.execute(&envelope)?;

        if let Some(uri) = &output.shuffle_server_uri
            && let Some((shuffle_id, num_output_partitions)) = self.shuffle_dep()
            && let Some(tracker) = crate::env::get_map_output_tracker()
        {
            tracker.register_shuffle(shuffle_id, num_output_partitions);
            if let Err(e) =
                tracker.register_map_output(shuffle_id, self.meta.partition, uri.clone())
            {
                log::error!("PipelineTask: register_map_output failed: {e}");
            }
        }

        Ok(Box::new((output.data, output.accumulator_deltas)))
    }
}
