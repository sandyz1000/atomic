use std::clone::Clone;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::option::Option;
use std::sync::Arc;

use crate::Rdd;
use crate::base::NativeScheduler;
use crate::error::LibResult;
use crate::listener::JobListener;
use crate::stage::Stage;

use atomic_data::data::Data;
use atomic_data::distributed::Step;
use atomic_data::task::TaskOption;
use atomic_data::task_context::PartitionTask;
use parking_lot::Mutex;

/// Per-partition data for a `Vec<Step>` pipeline job — set on `JobTracker::pipeline` instead
/// of relying on `func`/`final_rdd` (the closure-job fields), which a pipeline job doesn't use.
/// `submit_missing_tasks`'s final-stage branch checks this to build `PipelineTask`s instead of
/// `ResultTask`s, without needing `supports_closure_tasks()` — a pipeline job has no closure to
/// reject in the first place.
pub struct PipelineJobData {
    pub result_steps: Vec<Step>,
    pub source_partitions: Vec<Vec<u8>>,
    pub broadcasts: Vec<(usize, Vec<u8>)>,
}

#[derive(Clone, Debug)]
pub struct Job {
    pub run_id: usize,
    pub job_id: usize,
}

impl Job {
    pub fn new(run_id: usize, job_id: usize) -> Self {
        Job { run_id, job_id }
    }
}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Job) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Job {
    fn eq(&self, other: &Job) -> bool {
        self.job_id == other.job_id
    }
}

impl Eq for Job {}

impl Ord for Job {
    fn cmp(&self, other: &Job) -> Ordering {
        other.job_id.cmp(&self.job_id)
    }
}

type PendingTasks = BTreeMap<Stage, BTreeSet<TaskOption>>;

pub struct JobTracker<F, U: Data, T: Data, L>
where
    F: PartitionTask<T, U>,
    L: JobListener,
{
    pub output_parts: Vec<usize>,
    pub num_output_parts: usize,
    pub final_stage: Stage,
    pub func: Arc<F>,
    pub final_rdd: Arc<dyn Rdd<Item = T>>,
    pub run_id: usize,
    pub waiting: Mutex<BTreeSet<Stage>>,
    pub running: Mutex<BTreeSet<Stage>>,
    pub failed: Mutex<BTreeSet<Stage>>,
    pub finished: Mutex<Vec<bool>>,
    pub pending_tasks: Mutex<PendingTasks>,
    pub listener: L,
    /// `Some` for a `Vec<Step>` pipeline job (built by `Context::dispatch_pipeline`); `None`
    /// for a closure job (`Context::run_job`/`run_job_with_partitions`). `final_rdd`/`func`
    /// above stay populated either way — for a pipeline job `final_rdd` is the (possibly
    /// placeholder) RDD `dispatch_pipeline` derived its shuffle-dependency DAG from, and
    /// `func` is never called — only used so `JobTracker` doesn't need two full generic
    /// shapes for what's otherwise identical stage-tracking machinery.
    pub pipeline: Option<PipelineJobData>,
    _marker_t: PhantomData<T>,
    _marker_u: PhantomData<U>,
}

impl<F, U: Data, T: Data, L> JobTracker<F, U, T, L>
where
    F: PartitionTask<T, U>,
    L: JobListener,
{
    pub async fn from_scheduler<S>(
        scheduler: &S,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        output_parts: Vec<usize>,
        listener: L,
    ) -> LibResult<Arc<JobTracker<F, U, T, L>>>
    where
        S: NativeScheduler,
    {
        let run_id = scheduler.state().get_next_job_id();
        let final_stage = scheduler
            .new_stage(final_rdd.clone().get_rdd_base(), None)
            .await?;
        Ok(JobTracker::new(
            run_id,
            final_stage,
            func,
            final_rdd,
            output_parts,
            listener,
            None,
        ))
    }

    /// Same as [`from_scheduler`](Self::from_scheduler), but for a `Vec<Step>` pipeline job:
    /// `submit_missing_tasks`'s final stage builds `PipelineTask`s from `pipeline_data` instead
    /// of `ResultTask`s from `func`/`final_rdd`. `func`/`final_rdd` are still required to keep
    /// `JobTracker`'s single generic shape (see the `pipeline` field's doc comment) — callers
    /// pass a placeholder (e.g. the pipeline's already-materialized source RDD) since neither
    /// is ever invoked for a pipeline job.
    pub async fn from_scheduler_pipeline<S>(
        scheduler: &S,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        output_parts: Vec<usize>,
        listener: L,
        pipeline_data: PipelineJobData,
    ) -> LibResult<Arc<JobTracker<F, U, T, L>>>
    where
        S: NativeScheduler,
    {
        let run_id = scheduler.state().get_next_job_id();
        let final_stage = scheduler
            .new_stage(final_rdd.clone().get_rdd_base(), None)
            .await?;
        Ok(JobTracker::new(
            run_id,
            final_stage,
            func,
            final_rdd,
            output_parts,
            listener,
            Some(pipeline_data),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        run_id: usize,
        final_stage: Stage,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        output_parts: Vec<usize>,
        listener: L,
        pipeline: Option<PipelineJobData>,
    ) -> Arc<JobTracker<F, U, T, L>> {
        let finished: Vec<bool> = (0..output_parts.len()).map(|_| false).collect();
        let pending_tasks: BTreeMap<Stage, BTreeSet<TaskOption>> = BTreeMap::new();
        Arc::new(JobTracker {
            num_output_parts: output_parts.len(),
            output_parts,
            final_stage,
            func,
            final_rdd,
            run_id,
            waiting: Mutex::new(BTreeSet::new()),
            running: Mutex::new(BTreeSet::new()),
            failed: Mutex::new(BTreeSet::new()),
            finished: Mutex::new(finished),
            pending_tasks: Mutex::new(pending_tasks),
            listener,
            pipeline,
            _marker_t: PhantomData,
            _marker_u: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sort_job() {
        let mut jobs = vec![Job::new(1, 2), Job::new(1, 1), Job::new(1, 3)];
        println!("{:?}", jobs);
        jobs.sort();
        println!("{:?}", jobs);
        assert_eq!(jobs, vec![Job::new(1, 3), Job::new(1, 2), Job::new(1, 1),])
    }
}
