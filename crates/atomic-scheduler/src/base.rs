use crate::dag::CompletionEvent;
use crate::dag::FetchFailedVals;
use crate::error::{LibResult, SchedulerError};
use crate::job::JobTracker;
use crate::listener::JobListener;
use crate::planner::StagePlanner;
use crate::stage::Stage;
use atomic_data::data::Data;
use atomic_data::dependency::Dependency;
use atomic_data::distributed::Step;
use atomic_data::rdd::RddBase;
use atomic_data::shuffle::MapOutputTracker;
use atomic_data::task::TaskOption;
use atomic_data::task::pipeline::PipelineTask;
use atomic_data::task::result::ResultTask;
use atomic_data::task::shuffle_map::ShuffleMapTask;
use atomic_data::task_context::{PartitionFn, PartitionTask, TaskContext};
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

pub type EventQueue = Arc<DashMap<usize, VecDeque<CompletionEvent>>>;

/// Functionality of the library built-in schedulers
#[async_trait::async_trait]
pub trait NativeScheduler: StagePlanner {
    /// Fast path for execution. Runs the DD in the driver main thread if possible.
    fn local_execution<T: Data, U: Data, F, L>(
        tracker: Arc<JobTracker<F, U, T, L>>,
    ) -> LibResult<Option<Vec<U>>>
    where
        F: PartitionTask<T, U>,
        L: JobListener,
    {
        if tracker.final_stage.parents.is_empty() && (tracker.num_output_parts == 1) {
            let split = (tracker.final_rdd.splits()[tracker.output_parts[0]]).clone();
            let task_context = TaskContext::new(tracker.final_stage.id, tracker.output_parts[0], 0);
            let iter = tracker
                .final_rdd
                .iterator(split)
                .map_err(|_| SchedulerError::Other)?;
            Ok(Some(vec![(tracker.func)((task_context, iter))]))
        } else {
            Ok(None)
        }
    }

    async fn on_event_failure<T: Data, U: Data, F, L>(
        &self,
        jt: Arc<JobTracker<F, U, T, L>>,
        failed_vals: FetchFailedVals,
        stage_id: usize,
    ) where
        F: PartitionTask<T, U>,
        L: JobListener,
    {
        let FetchFailedVals {
            server_uri,
            shuffle_id,
            map_id,
            ..
        } = failed_vals;

        // The reduce stage that hit the fetch failure: mark it failed so it is
        // re-run after the lost map output is recomputed.
        let m = self.state();
        let failed_stage = m.fetch_from_stage_cache(stage_id);
        jt.running.lock().remove(&failed_stage);
        jt.failed.lock().insert(failed_stage);
        log::warn!(
            "fetch failed: shuffle {shuffle_id} map {map_id} on {server_uri} — \
             invalidating the lost map output and resubmitting its stage for recompute"
        );
        // Drop the lost output from both the stage's output_locs and the
        // MapOutputTracker, then mark the producing map stage failed so
        // `submit_missing_tasks` recomputes exactly that partition.
        m.remove_stage_output_loc(shuffle_id, map_id, &server_uri);
        m.unregister_map_output(shuffle_id, map_id, server_uri);
        jt.failed.lock().insert(m.stage_for_shuffle(shuffle_id));
    }

    async fn on_event_success<T: Data, U: Data + Clone, F, L>(
        &self,
        mut completed_event: CompletionEvent,
        results: &mut Vec<Option<U>>,
        num_finished: &mut usize,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> LibResult<()>
    where
        F: PartitionTask<T, U>,
        L: JobListener,
    {
        match &completed_event.task {
            TaskOption::ResultTask(rt) => {
                let result = completed_event.result.take().ok_or(SchedulerError::Other)?;

                jt.listener.task_succeeded(rt.output_id, &*result).await?;

                let typed_result = result
                    .as_any()
                    .downcast_ref::<U>()
                    .ok_or_else(|| {
                        SchedulerError::DowncastFailure(
                            "Failed to downcast result to expected type U".to_string(),
                        )
                    })?
                    .clone();

                results[rt.output_id] = Some(typed_result);
                jt.finished.lock()[rt.output_id] = true;
                *num_finished += 1;
            }
            TaskOption::PipelineTask(pt) => {
                // Same shape as the `ResultTask` arm above — a `PipelineTask`'s result is
                // `Box<dyn Data>` wrapping the raw `Vec<u8>` a Step-pipeline produced, and
                // pipeline jobs always instantiate `JobTracker<F, U, T, L>` with `U = Vec<u8>`,
                // so downcasting to `U` here is exactly as valid as it is for a closure result.
                let result = completed_event.result.take().ok_or(SchedulerError::Other)?;

                jt.listener.task_succeeded(pt.output_id, &*result).await?;

                let typed_result = result
                    .as_any()
                    .downcast_ref::<U>()
                    .ok_or_else(|| {
                        SchedulerError::DowncastFailure(
                            "Failed to downcast pipeline result to expected type U".to_string(),
                        )
                    })?
                    .clone();

                results[pt.output_id] = Some(typed_result);
                jt.finished.lock()[pt.output_id] = true;
                *num_finished += 1;
            }
            TaskOption::ShuffleMapTask(smt) => {
                let shuffle_server_uri = completed_event
                    .result
                    .take()
                    .ok_or(SchedulerError::Other)?
                    .as_any()
                    .downcast_ref::<String>()
                    .ok_or_else(|| SchedulerError::DowncastFailure("String".to_string()))?
                    .clone();
                log::debug!(
                    "completed shuffle task server uri: {:?}",
                    shuffle_server_uri
                );
                let state = self.state();
                state.add_stage_output_loc(
                    smt.meta.stage_id,
                    smt.meta.partition,
                    shuffle_server_uri,
                );

                let stage = state.fetch_from_stage_cache(smt.meta.stage_id);
                log::debug!(
                    "pending stages: {:?}",
                    jt.pending_tasks
                        .lock()
                        .iter()
                        .map(|(x, y)| (x.id, y.iter().map(|k| k.get_task_id()).collect::<Vec<_>>()))
                        .collect::<Vec<_>>()
                );
                log::debug!(
                    "pending tasks: {:?}",
                    jt.pending_tasks
                        .lock()
                        .get(&stage)
                        .ok_or(SchedulerError::Other)?
                        .iter()
                        .map(|x| x.get_task_id())
                        .collect::<Vec<_>>()
                );
                log::debug!(
                    "running stages: {:?}",
                    jt.running.lock().iter().map(|x| x.id).collect::<Vec<_>>()
                );
                log::debug!(
                    "waiting stages: {:?}",
                    jt.waiting.lock().iter().map(|x| x.id).collect::<Vec<_>>()
                );

                if jt.running.lock().contains(&stage)
                    && jt
                        .pending_tasks
                        .lock()
                        .get(&stage)
                        .ok_or(SchedulerError::Other)?
                        .is_empty()
                {
                    log::debug!("started registering map outputs");
                    // TODO: logging
                    jt.running.lock().remove(&stage);
                    if let Some(dep) = stage.shuffle_dependency {
                        log::debug!(
                            "stage output locs before register mapoutput tracker: {:?}",
                            stage.output_locs
                        );
                        let locs = stage
                            .output_locs
                            .iter()
                            .map(|x| x.first().map(|s| s.to_owned()))
                            .collect();
                        let shuffle_id = dep.get_shuffle_id();
                        log::debug!("locs for shuffle id #{}: {:?}", shuffle_id, locs);
                        state.register_map_outputs(shuffle_id, locs);
                        log::debug!("finished registering map outputs");

                        // After the map stage completes, compute the optimal number
                        // of reduce partitions based on actual bucket byte sizes.
                        if state.coalesce_threshold_bytes > 0 {
                            state.compute_coalescing(shuffle_id, stage.num_partitions);
                        }
                    }
                    // TODO: Cache
                    self.update_cache_locs().await?;
                    let mut newly_runnable = Vec::new();
                    let waiting_stages: Vec<_> = jt.waiting.lock().iter().cloned().collect();
                    for stage in waiting_stages {
                        let missing_stages = self.get_missing_parent_stages(stage.clone()).await?;
                        log::debug!(
                            "waiting stage parent stages for stage #{} are: {:?}",
                            stage.id,
                            missing_stages.iter().map(|x| x.id).collect::<Vec<_>>()
                        );
                        if missing_stages.is_empty() {
                            newly_runnable.push(stage.clone())
                        }
                    }
                    for stage in &newly_runnable {
                        jt.waiting.lock().remove(stage);
                    }
                    for stage in &newly_runnable {
                        jt.running.lock().insert(stage.clone());
                    }
                    for stage in newly_runnable {
                        self.submit_missing_tasks(stage, jt.clone()).await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn submit_stage<T: Data, U: Data, F, L>(
        &self,
        stage: Stage,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> LibResult<()>
    where
        F: PartitionTask<T, U>,
        L: JobListener,
    {
        log::debug!("submitting stage #{}", stage.id);
        if !jt.waiting.lock().contains(&stage) && !jt.running.lock().contains(&stage) {
            let missing = self.get_missing_parent_stages(stage.clone()).await?;
            log::debug!(
                "while submitting stage #{}, missing stages: {:?}",
                stage.id,
                missing.iter().map(|x| x.id).collect::<Vec<_>>()
            );
            if missing.is_empty() {
                self.submit_missing_tasks(stage.clone(), jt.clone()).await?;
                jt.running.lock().insert(stage);
            } else {
                for parent in missing {
                    self.submit_stage(parent, jt.clone()).await?;
                }
                jt.waiting.lock().insert(stage);
            }
        }
        Ok(())
    }

    async fn submit_missing_tasks<T: Data, U: Data, F, L>(
        &self,
        stage: Stage,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> LibResult<()>
    where
        F: PartitionTask<T, U>,
        L: JobListener,
    {
        let m = self.state();
        let mut pending_tasks = jt.pending_tasks.lock();
        let my_pending = pending_tasks.entry(stage.clone()).or_default();
        if stage == jt.final_stage {
            log::debug!("final stage #{}", stage.id);
            if let Some(pipeline) = &jt.pipeline {
                // Pipeline job: no closure involved, so no supports_closure_tasks() gate —
                // a PipelineTask dispatches through the same TaskEnvelope/ComputeEngine path
                // regardless of which scheduler (thread vs. worker) runs it.
                for (id, part) in jt.output_parts.iter().enumerate().take(jt.num_output_parts) {
                    let locs = self.get_preferred_locs(jt.final_rdd.get_rdd_base(), *part);
                    let source = pipeline
                        .source_partitions
                        .get(*part)
                        .cloned()
                        .ok_or_else(|| {
                            SchedulerError::TaskFailed(format!(
                                "pipeline job: source partition {part} out of bounds ({} partitions)",
                                pipeline.source_partitions.len()
                            ))
                        })?;
                    // Distributed cache locality: if this partition is already cached on a
                    // live worker, pin the task there and serve it from cache (with a
                    // recompute fallback baked into the `PipelineTask`). Local mode and the
                    // first, cache-populating run get `(None, None)` and recompute anywhere.
                    let (pinned, cache_serve) = self.pipeline_partition_plan(
                        &pipeline.result_steps,
                        *part,
                        jt.num_output_parts,
                    );
                    let pipeline_task = PipelineTask::new(
                        m.get_next_task_id(),
                        jt.run_id,
                        jt.final_stage.id,
                        *part,
                        locs,
                        source,
                        pipeline.result_steps.clone(),
                        pipeline.broadcasts.clone(),
                        id,
                        cache_serve,
                    );
                    let task_option = TaskOption::PipelineTask(pipeline_task);
                    let executor =
                        pinned.unwrap_or_else(|| self.next_executor_server(&task_option));
                    my_pending.insert(task_option.clone());
                    self.submit_task::<T, U, F>(task_option, executor)
                }
            } else {
                if !self.supports_closure_tasks() {
                    return Err(SchedulerError::UnsupportedOperation(
                        "final-stage closure ResultTask is unsupported on this scheduler; use task-op APIs (task_fn!/map_task/filter_task/flat_map_task/fold_task/reduce_task) for distributed execution",
                    ));
                }
                for (id, part) in jt.output_parts.iter().enumerate().take(jt.num_output_parts) {
                    let locs = self.get_preferred_locs(jt.final_rdd.get_rdd_base(), *part);
                    let result_task = ResultTask::new(
                        m.get_next_task_id(),
                        jt.run_id,
                        jt.final_stage.id,
                        jt.final_rdd.clone(),
                        jt.func.clone(),
                        *part,
                        locs.clone(),
                        id,
                    );
                    let task_option = TaskOption::ResultTask(result_task.into());
                    let executor = self.next_executor_server(&task_option);
                    my_pending.insert(task_option.clone());
                    self.submit_task::<T, U, F>(task_option, executor)
                }
            }
        } else {
            for p in 0..stage.num_partitions {
                log::debug!("shuffle stage #{}", stage.id);
                if stage.output_locs[p].is_empty() {
                    let locs = self.get_preferred_locs(stage.get_rdd(), p);
                    log::debug!("creating task for stage #{} partition #{}", stage.id, p);
                    let shuffle_dep = Arc::new(Dependency::Shuffle(
                        stage
                            .shuffle_dependency
                            .clone()
                            .ok_or(SchedulerError::Other)?,
                    ));
                    let shuffle_map_task = ShuffleMapTask::new(
                        m.get_next_task_id(),
                        jt.run_id,
                        stage.id,
                        stage.rdd.clone(),
                        shuffle_dep,
                        p,
                        locs,
                    );
                    log::debug!(
                        "creating task for stage #{}, partition #{} and shuffle id #{:?}",
                        stage.id,
                        p,
                        shuffle_map_task.dep.get_shuffle_id()
                    );
                    let task_option = TaskOption::ShuffleMapTask(shuffle_map_task);
                    let executor = self.next_executor_server(&task_option);
                    my_pending.insert(task_option.clone());
                    self.submit_task::<T, U, F>(task_option, executor);
                }
            }
        }
        Ok(())
    }

    fn wait_for_event(&self, run_id: usize, timeout: u64) -> Option<CompletionEvent> {
        let end = Instant::now() + Duration::from_millis(timeout);
        let state = self.state();
        let event_queue = state.get_event_queue();
        while event_queue.get(&run_id)?.is_empty() {
            if Instant::now() > end {
                return None;
            }
            thread::sleep(Duration::from_millis(10));
        }
        event_queue.get_mut(&run_id)?.pop_front()
    }

    /// Failed-task retry cap before a job gives up (`SchedulerError::MaxTaskFailures`).
    /// `LocalScheduler` returns its configured value; this default (matching
    /// `LocalScheduler::new_with_coalesce`'s typical `20`) covers any scheduler that hasn't
    /// overridden it.
    fn max_failures(&self) -> usize {
        20
    }

    /// Debounce window (ms) before a batch of failed stages is resubmitted — not a
    /// per-task backoff, a single fixed wait so multiple failures in flight get resubmitted
    /// together rather than one at a time.
    fn resubmit_timeout(&self) -> u128 {
        2000
    }

    /// Poll interval (ms) `wait_for_event` sleeps between empty-queue checks.
    fn poll_timeout(&self) -> u64 {
        50
    }

    /// Drive one job's `Stage` DAG to completion: submit the final stage (which recursively
    /// submits any missing parent — e.g. shuffle-map — stages first), then loop processing
    /// `CompletionEvent`s until every output partition is finished, retrying failed stages
    /// after `resubmit_timeout()`. Shared by every scheduler — local (`submit_task` runs a
    /// task on a blocking thread) and distributed (`submit_task` ships it to a worker) alike;
    /// this loop itself has no notion of which.
    async fn event_process_loop<T: Data, U: Data + Clone, F, L>(
        self: Arc<Self>,
        allow_local: bool,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> LibResult<Vec<U>>
    where
        Self: Sized,
        F: PartitionTask<T, U>,
        L: JobListener,
    {
        if allow_local && let Some(result) = Self::local_execution(jt.clone())? {
            return Ok(result);
        }

        self.state().event_queues.insert(jt.run_id, VecDeque::new());

        let mut results: Vec<Option<U>> = (0..jt.num_output_parts).map(|_| None).collect();
        // Timestamp of the oldest unresubmitted failure; None when jt.failed is empty.
        let mut fetch_failure_start: Option<Instant> = None;
        let mut task_failure_counts: HashMap<(usize, usize), usize> = HashMap::new();

        self.submit_stage(jt.final_stage.clone(), jt.clone())
            .await?;

        let mut num_finished = 0;
        while num_finished != jt.num_output_parts {
            if let Some(reason) = self.state().take_job_abort(jt.run_id) {
                self.state().event_queues.remove(&jt.run_id);
                return Err(SchedulerError::JobAborted(reason));
            }
            let event_option = self.wait_for_event(jt.run_id, self.poll_timeout());

            if let Some(evt) = event_option {
                let stage = self
                    .state()
                    .stage_cache
                    .get(&evt.task.get_stage_id())
                    .unwrap()
                    .clone();
                jt.pending_tasks
                    .lock()
                    .get_mut(&stage)
                    .unwrap()
                    .remove(&evt.task);
                use crate::dag::TaskEndReason::*;
                match evt.reason {
                    Success => {
                        self.on_event_success(evt, &mut results, &mut num_finished, jt.clone())
                            .await?;
                    }
                    FetchFailed(failed_vals) => {
                        self.on_event_failure(jt.clone(), failed_vals, evt.task.get_stage_id())
                            .await;
                        fetch_failure_start.get_or_insert_with(Instant::now);
                    }
                    Error(error) => {
                        let key = (evt.task.get_stage_id(), evt.task.get_task_id());
                        let count = task_failure_counts.entry(key).or_insert(0);
                        *count += 1;
                        if *count >= self.max_failures() {
                            return Err(SchedulerError::MaxTaskFailures(error.to_string()));
                        }
                        log::warn!(
                            "task {}/{} error (attempt {}/{}): retrying stage",
                            evt.task.get_stage_id(),
                            evt.task.get_task_id(),
                            count,
                            self.max_failures(),
                        );
                        let m = self.state();
                        let failed_stage = m.fetch_from_stage_cache(evt.task.get_stage_id());
                        // submit_stage() no-ops for a stage still marked running.
                        jt.running.lock().remove(&failed_stage);
                        jt.failed.lock().insert(failed_stage);
                        fetch_failure_start.get_or_insert_with(Instant::now);
                    }
                    OtherFailure(msg) => {
                        let key = (evt.task.get_stage_id(), evt.task.get_task_id());
                        let count = task_failure_counts.entry(key).or_insert(0);
                        *count += 1;
                        if *count >= self.max_failures() {
                            return Err(SchedulerError::MaxTaskFailures(msg));
                        }
                        log::warn!(
                            "task {}/{} failure (attempt {}/{}): retrying stage",
                            evt.task.get_stage_id(),
                            evt.task.get_task_id(),
                            count,
                            self.max_failures(),
                        );
                        let m = self.state();
                        let failed_stage = m.fetch_from_stage_cache(evt.task.get_stage_id());
                        jt.running.lock().remove(&failed_stage);
                        jt.failed.lock().insert(failed_stage);
                        fetch_failure_start.get_or_insert_with(Instant::now);
                    }
                }
            }

            // Checked every iteration, not just on fresh events, so a failure with no
            // further events to wake the loop still gets resubmitted.
            if let Some(since) = fetch_failure_start
                && !jt.failed.lock().is_empty()
                && since.elapsed().as_millis() > self.resubmit_timeout()
            {
                self.update_cache_locs().await?;
                let failed_stages: Vec<Stage> = jt.failed.lock().iter().cloned().collect();
                for stage in failed_stages {
                    self.submit_stage(stage, jt.clone()).await?;
                }
                jt.failed.lock().clear();
                fetch_failure_start = None;
            }
        }

        self.state().event_queues.remove(&jt.run_id);
        Ok(results
            .into_iter()
            .map(|s| match s {
                Some(v) => v,
                None => panic!("some results still missing"),
            })
            .collect())
    }

    fn submit_task<T: Data, U: Data, F>(&self, task: TaskOption, target_executor: SocketAddrV4)
    where
        F: PartitionFn<T, U>;

    /// Whether this scheduler can execute closure-backed `ResultTask`s.
    ///
    /// Local scheduler returns `true` (default). Distributed scheduler returns `false` and
    /// requires op-envelope task APIs — the project's task model (`#[task]`/`task_fn!`,
    /// `atomic_compute::task_traits`) dispatches by compile-time-registered name, not by
    /// shipping a closure. `LocalScheduler` returning `true` is legacy, not a model to
    /// extend: it exists only because the built-in RDD actions (`.collect()`, `.reduce()`,
    /// ...) predate the task model and haven't been migrated off `PartitionFn`/
    /// `PartitionTask` (`atomic_data::task_context`) yet. New driver-facing ops should be
    /// `#[task]`/`task_fn!`-wrapped even when they only ever run in-process.
    fn supports_closure_tasks(&self) -> bool {
        true
    }

    async fn update_cache_locs(&self) -> LibResult<()>;

    fn next_executor_server(&self, task: &TaskOption) -> SocketAddrV4;

    /// Per-partition dispatch hint for a pipeline task: an optional worker to pin to (the one
    /// holding this partition's cache) and the cache-serve `(rdd_id, post_ops)` that turns the
    /// task into a cache read. Default recomputes anywhere — `(None, None)` — which is correct
    /// for local mode (its cache is process-global, no worker routing needed) and for the
    /// first, cache-populating run. `DistributedScheduler` overrides it to consult
    /// `plan_cache_dispatch`.
    fn pipeline_partition_plan(
        &self,
        _steps: &[Step],
        _partition: usize,
        _num_partitions: usize,
    ) -> (Option<SocketAddrV4>, Option<(usize, Vec<Step>)>) {
        (None, None)
    }
}

#[derive(Clone, Default)]
pub struct SchedulerState {
    pub stage_cache: Arc<DashMap<usize, Stage>>,
    pub map_output_tracker: Option<Arc<MapOutputTracker>>,
    pub shuffle_to_map_stage: Arc<DashMap<usize, Stage>>,
    pub event_queues: EventQueue,
    /// Per-RDD cached partition locations. Cleared by `update_cache_locs` when
    /// shuffle stages complete or fetch failures are detected.
    pub cache_locs: Arc<DashMap<usize, Vec<Vec<Ipv4Addr>>>>,
    /// Monotonically increasing counter used to allocate unique job IDs.
    pub next_job_id: Arc<AtomicUsize>,
    /// Monotonically increasing counter used to allocate unique task IDs.
    pub next_task_id: Arc<AtomicUsize>,
    /// Monotonically increasing counter used to allocate unique stage IDs.
    pub next_stage_id: Arc<AtomicUsize>,
    /// Adaptive coalescing threshold in bytes (0 = disabled).
    /// Copied from `Config::coalesce_shuffle_threshold_bytes` at context init.
    pub coalesce_threshold_bytes: u64,
    /// Per-job abort reasons (`run_id → reason`). Set by failure handlers that
    /// cannot recover (e.g. lost distributed map output with no live recompute
    /// path); drained by the event loop, which fails the job loudly instead of
    /// completing with silently missing partitions.
    pub job_aborts: Arc<DashMap<usize, String>>,
}

impl SchedulerState {
    pub fn new() -> Self {
        Self {
            stage_cache: Arc::new(DashMap::new()),
            map_output_tracker: atomic_data::env::get_map_output_tracker(),
            shuffle_to_map_stage: Arc::new(DashMap::new()),
            event_queues: Arc::new(DashMap::new()),
            cache_locs: Arc::new(DashMap::new()),
            next_job_id: Arc::new(AtomicUsize::new(0)),
            next_task_id: Arc::new(AtomicUsize::new(0)),
            next_stage_id: Arc::new(AtomicUsize::new(0)),
            coalesce_threshold_bytes: 0,
            job_aborts: Arc::new(DashMap::new()),
        }
    }

    pub fn with_coalesce_threshold(mut self, bytes: u64) -> Self {
        self.coalesce_threshold_bytes = bytes;
        self
    }

    #[inline]
    pub fn add_stage_output_loc(&self, stage_id: usize, partition: usize, host: String) {
        self.stage_cache
            .get_mut(&stage_id)
            .expect("stage must exist in stage_cache before add_output_loc is called")
            .add_output_loc(partition, host);
    }

    #[inline]
    pub fn insert_into_stage_cache(&self, id: usize, stage: Stage) {
        self.stage_cache.insert(id, stage.clone());
    }

    #[inline]
    pub fn fetch_from_stage_cache(&self, id: usize) -> Stage {
        self.stage_cache
            .get(&id)
            .expect("stage must exist in stage_cache")
            .clone()
    }

    #[inline]
    pub fn stage_for_shuffle(&self, id: usize) -> Stage {
        self.shuffle_to_map_stage
            .get(&id)
            .expect("stage must exist in shuffle_to_map_stage")
            .clone()
    }

    #[inline]
    pub fn unregister_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String) {
        if let Some(tracker) = &self.map_output_tracker {
            tracker.unregister_map_output(shuffle_id, map_id, server_uri)
        }
    }

    /// Register a single recovered map-output slot. Returns `false` when no
    /// tracker is installed or the slot is unknown.
    #[inline]
    pub fn register_map_output(
        &self,
        shuffle_id: usize,
        map_id: usize,
        server_uri: String,
    ) -> bool {
        match &self.map_output_tracker {
            Some(tracker) => tracker
                .register_map_output(shuffle_id, map_id, server_uri)
                .is_ok(),
            None => false,
        }
    }

    /// Current URI registered for one map output, if any.
    #[inline]
    pub fn get_map_output_uri(&self, shuffle_id: usize, map_id: usize) -> Option<String> {
        self.map_output_tracker
            .as_ref()?
            .map_output_uris
            .get(&shuffle_id)?
            .get(map_id)?
            .clone()
    }

    /// Mark `run_id` for loud failure; the event loop returns `JobAborted`.
    #[inline]
    pub fn abort_job(&self, run_id: usize, reason: String) {
        self.job_aborts.insert(run_id, reason);
    }

    /// Take (and clear) the abort reason for `run_id`, if one was set.
    #[inline]
    pub fn take_job_abort(&self, run_id: usize) -> Option<String> {
        self.job_aborts.remove(&run_id).map(|(_, reason)| reason)
    }

    #[inline]
    pub fn register_shuffle(&self, shuffle_id: usize, num_maps: usize) {
        if let Some(tracker) = &self.map_output_tracker {
            tracker.register_shuffle(shuffle_id, num_maps)
        }
    }

    #[inline]
    pub fn register_map_outputs(&self, shuffle_id: usize, locs: Vec<Option<String>>) {
        if let Some(tracker) = &self.map_output_tracker {
            tracker.register_map_outputs(shuffle_id, locs)
        }
    }

    /// Compute the optimal coalesced reduce partition count for a completed shuffle-map
    /// stage and store it in the `MapOutputTracker`.
    ///
    /// Uses the global `SHUFFLE_CACHE` to measure the total bytes written per reduce
    /// partition (summed across all map tasks). Merges adjacent small partitions until
    /// each coalesced partition holds at least `coalesce_threshold_bytes / original_n`
    /// average bytes.
    pub fn compute_coalescing(&self, shuffle_id: usize, num_map_partitions: usize) {
        let tracker = match &self.map_output_tracker {
            Some(t) => t.clone(),
            None => return,
        };
        let cache = match atomic_data::env::get_shuffle_cache() {
            Some(c) => c,
            None => return,
        };

        // map_output_uris[shuffle_id].len() gives the original number of reduce partitions.
        let num_reduce_partitions = tracker
            .map_output_uris
            .get(&shuffle_id)
            .map(|v| v.len())
            .unwrap_or(0);
        if num_reduce_partitions <= 1 {
            return; // nothing to coalesce
        }

        // Compute total bytes for each reduce partition across all map tasks.
        let bucket_bytes: Vec<u64> = (0..num_reduce_partitions)
            .map(|reduce_id| {
                cache.bytes_for_reduce_partition(shuffle_id, num_map_partitions, reduce_id)
            })
            .collect();

        let total_bytes: u64 = bucket_bytes.iter().sum();
        if total_bytes == 0 {
            return; // empty shuffle — no coalescing needed
        }

        // Target: each coalesced partition should hold at least `threshold / original_n` bytes.
        // Greedily merge adjacent partitions until each meets the target.
        let target_bytes_per_partition =
            (self.coalesce_threshold_bytes / num_reduce_partitions as u64).max(1);

        let mut coalesced_count = 0usize;
        let mut running = 0u64;
        for &bytes in &bucket_bytes {
            running += bytes;
            if running >= target_bytes_per_partition {
                coalesced_count += 1;
                running = 0;
            }
        }
        // Any remaining bytes form the last coalesced partition.
        if running > 0 {
            coalesced_count += 1;
        }

        let coalesced_count = coalesced_count.max(1).min(num_reduce_partitions);
        if coalesced_count < num_reduce_partitions {
            log::info!(
                "adaptive coalescing: shuffle #{shuffle_id} coalesced {num_reduce_partitions} → \
                 {coalesced_count} partitions ({total_bytes} bytes total)"
            );
            tracker.coalesced_parts.insert(shuffle_id, coalesced_count);
        }
    }

    #[inline]
    pub fn remove_stage_output_loc(&self, shuffle_id: usize, map_id: usize, server_uri: &str) {
        self.shuffle_to_map_stage
            .get_mut(&shuffle_id)
            .expect("stage must exist in shuffle_to_map_stage before remove_output_loc is called")
            .remove_output_loc(map_id, server_uri);
    }

    #[inline]
    pub fn get_cache_locs(&self, rdd: Arc<dyn RddBase>) -> Option<Vec<Vec<Ipv4Addr>>> {
        let locs_opt = self.cache_locs.get(&rdd.get_rdd_id());
        locs_opt.map(|l| l.clone())
    }

    #[inline]
    pub fn get_event_queue(&self) -> &Arc<DashMap<usize, VecDeque<CompletionEvent>>> {
        &self.event_queues
    }

    #[inline]
    pub fn get_next_job_id(&self) -> usize {
        self.next_job_id.fetch_add(1, Ordering::SeqCst)
    }

    #[inline]
    pub fn get_next_stage_id(&self) -> usize {
        self.next_stage_id.fetch_add(1, Ordering::SeqCst)
    }

    #[inline]
    pub fn get_next_task_id(&self) -> usize {
        self.next_task_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Returns true if the distributed scheduler already completed a shuffle
    /// for `shuffle_id` — i.e., every map-partition slot in the tracker has a URI.
    pub fn is_shuffle_complete(&self, shuffle_id: usize) -> bool {
        if let Some(tracker) = &self.map_output_tracker {
            tracker
                .map_output_uris
                .get(&shuffle_id)
                .is_some_and(|arr| !arr.is_empty() && arr.iter().all(|s| s.is_some()))
        } else {
            false
        }
    }

    /// Returns the per-partition URIs for a completed shuffle (in partition order).
    pub fn get_shuffle_server_uris(&self, shuffle_id: usize) -> Vec<String> {
        if let Some(tracker) = &self.map_output_tracker {
            tracker
                .map_output_uris
                .get(&shuffle_id)
                .map(|arr| arr.iter().filter_map(|s| s.clone()).collect())
                .unwrap_or_default()
        } else {
            vec![]
        }
    }
}
