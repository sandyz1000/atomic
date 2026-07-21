use atomic_data::data::Data;
use atomic_data::dependency::ShuffleDependency;
use atomic_data::partial::result::PartialResult;
use atomic_data::partial::{ApproxListener, ApproximateEvaluator};
use atomic_data::rdd::Rdd;
use atomic_data::task::{TaskOption, TaskResult};
use atomic_data::task_context::TaskContext;
use std::clone::Clone;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::option::Option;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::base::{NativeScheduler, SchedulerState};
use crate::dag::{CompletionEvent, FetchFailedVals, TaskEndReason};
use crate::error::{LibResult, SchedulerError};
use crate::job::JobTracker;
use crate::listener::{
    JobEndListener, JobListener, JobStartListener, LiveListenerBus, NoOpListener,
};
use crate::planner::StagePlanner;
use crate::stage::Stage;

/// Recovery hook for lost distributed map outputs: `(shuffle_id, map_id) → recovered`.
///
/// Installed by a distributed driver context. On a reduce-side fetch failure the
/// scheduler clears the stale tracker slot and calls this hook, which recomputes
/// the lost map partition on a live worker and re-registers its fresh URI. When it
/// returns `true` only the fetching stage is retried; the map stage is never
/// recomputed on the driver (distributed lineage may be a staged placeholder with
/// no local data).
pub type MapOutputRecovery = Arc<dyn Fn(usize, usize) -> bool + Send + Sync>;

#[derive(Clone, Default)]
pub struct LocalScheduler {
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,
    resubmit_timeout: u128,
    poll_timeout: u64,
    /// Shared mutable state: stage cache, event queues, map-output tracker, and ID counters.
    state: SchedulerState,
    /// Scheduler role flag taken at construction; retained as config (driver vs. worker).
    #[allow(dead_code)]
    master: bool,
    scheduler_lock: Arc<Mutex<()>>,
    live_listener_bus: LiveListenerBus,
    /// Distributed map-output recovery hook; `None` in pure local mode.
    map_output_recovery: Arc<std::sync::OnceLock<MapOutputRecovery>>,
}

impl LocalScheduler {
    pub fn new(max_failures: usize, master: bool) -> Self {
        Self::new_with_coalesce(max_failures, master, 0)
    }

    pub fn new_with_coalesce(
        max_failures: usize,
        master: bool,
        coalesce_threshold_bytes: u64,
    ) -> Self {
        let mut live_listener_bus = LiveListenerBus::new();
        live_listener_bus.start().unwrap();
        LocalScheduler {
            state: SchedulerState::new().with_coalesce_threshold(coalesce_threshold_bytes),
            max_failures,
            attempt_id: Arc::new(AtomicUsize::new(0)),
            resubmit_timeout: 2000,
            poll_timeout: 50,
            master,
            scheduler_lock: Arc::new(Mutex::new(())),
            live_listener_bus,
            map_output_recovery: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Install the distributed map-output recovery hook. First call wins;
    /// later calls are ignored (the hook is process-lifetime, like the tracker).
    pub fn set_map_output_recovery(&self, hook: MapOutputRecovery) {
        let _ = self.map_output_recovery.set(hook);
    }

    /// Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
    /// as they arrive. Returns a partial result object from the evaluator.
    pub fn run_approximate_job<T: Data, U: Data + Clone, R, F, E>(
        self: Arc<Self>,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        evaluator: E,
        timeout: Duration,
    ) -> LibResult<PartialResult<R>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
        E: ApproximateEvaluator<U, R> + Send + Sync + 'static,
        R: Clone + Debug + Send + Sync + 'static,
    {
        // acquiring lock so that only one job can run at same time this lock is just
        // a temporary patch for preventing multiple jobs to update cache locks which affects
        // construction of dag task graph. dag task graph construction needs to be altered
        let sched = self.clone();
        let _lock = sched.scheduler_lock.lock();

        // Run async code directly - tokio runtime should already be available
        futures::executor::block_on(async move {
            let partitions: Vec<_> = (0..final_rdd.number_of_splits()).collect();
            let listener = ApproxListener::new(evaluator, timeout, partitions.len());
            let jt =
                JobTracker::from_scheduler(&*self, func, final_rdd.clone(), partitions, listener)
                    .await?;
            if final_rdd.number_of_splits() == 0 {
                let time = Instant::now();
                self.live_listener_bus.post(Box::new(JobStartListener {
                    job_id: jt.run_id,
                    time,
                    stage_infos: vec![],
                }));
                self.live_listener_bus.post(Box::new(JobEndListener {
                    job_id: jt.run_id,
                    time,
                    job_result: true,
                }));
                let res =
                    PartialResult::new(jt.listener.evaluator.lock().await.current_result(), true);
                return Ok(res);
            }
            tokio::spawn(self.event_process_loop(false, jt.clone()));
            Ok(jt.listener.get_result().await?)
        })
    }

    pub fn run_job<T: Data, U: Data + Clone, F>(
        self: Arc<Self>,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> LibResult<Vec<U>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
    {
        // acquiring lock so that only one job can run at same time this lock is just
        // a temporary patch for preventing multiple jobs to update cache locks which affects
        // construction of dag task graph. dag task graph construction needs to be altered
        let sched = self.clone();
        let _lock = sched.scheduler_lock.lock();

        // Run async code directly - tokio runtime should already be available
        futures::executor::block_on(async move {
            let jt = JobTracker::from_scheduler(
                &*self,
                func,
                final_rdd.clone(),
                partitions,
                NoOpListener,
            )
            .await?;
            self.event_process_loop(allow_local, jt).await
        })
    }

    /// Start the event processing loop for a given job.
    async fn event_process_loop<T: Data, U: Data + Clone, F, L>(
        self: Arc<Self>,
        allow_local: bool,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> LibResult<Vec<U>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
        L: JobListener,
    {
        // TODO: update cache

        if allow_local && let Some(result) = LocalScheduler::local_execution(jt.clone())? {
            return Ok(result);
        }

        self.state.event_queues.insert(jt.run_id, VecDeque::new());

        let mut results: Vec<Option<U>> = (0..jt.num_output_parts).map(|_| None).collect();
        // Timestamp of the oldest unresubmitted failure; None when jt.failed is empty.
        let mut fetch_failure_start: Option<Instant> = None;
        let mut task_failure_counts: HashMap<(usize, usize), usize> = HashMap::new();

        self.submit_stage(jt.final_stage.clone(), jt.clone())
            .await?;
        log::debug!(
            "pending stages and tasks: {:?}",
            jt.pending_tasks
                .lock()
                .iter()
                .map(|(k, v)| (k.id, v.iter().map(|x| x.get_task_id()).collect::<Vec<_>>()))
                .collect::<Vec<_>>()
        );

        let mut num_finished = 0;
        while num_finished != jt.num_output_parts {
            if let Some(reason) = self.state.take_job_abort(jt.run_id) {
                self.state.event_queues.remove(&jt.run_id);
                return Err(SchedulerError::JobAborted(reason));
            }
            let event_option = self.wait_for_event(jt.run_id, self.poll_timeout);

            if let Some(evt) = event_option {
                log::debug!("event starting");
                let stage = self
                    .state
                    .stage_cache
                    .get(&evt.task.get_stage_id())
                    .unwrap()
                    .clone();
                log::debug!(
                    "removing stage #{} task from pending task #{}",
                    stage.id,
                    evt.task.get_task_id()
                );
                jt.pending_tasks
                    .lock()
                    .get_mut(&stage)
                    .unwrap()
                    .remove(&evt.task);
                use super::dag::TaskEndReason::*;
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
                        if *count >= self.max_failures {
                            return Err(SchedulerError::MaxTaskFailures(error.to_string()));
                        }
                        log::warn!(
                            "task {}/{} error (attempt {}/{}): retrying stage",
                            evt.task.get_stage_id(),
                            evt.task.get_task_id(),
                            count,
                            self.max_failures,
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
                        if *count >= self.max_failures {
                            return Err(SchedulerError::MaxTaskFailures(msg));
                        }
                        log::warn!(
                            "task {}/{} failure (attempt {}/{}): retrying stage",
                            evt.task.get_stage_id(),
                            evt.task.get_task_id(),
                            count,
                            self.max_failures,
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
                && since.elapsed().as_millis() > self.resubmit_timeout
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

        self.state.event_queues.remove(&jt.run_id);
        Ok(results
            .into_iter()
            .map(|s| match s {
                Some(v) => v,
                None => panic!("some results still missing"),
            })
            .collect())
    }

    fn run_task(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: TaskOption,
        attempt_id: usize,
    ) {
        let result = task.run(attempt_id);
        match result {
            Ok(task_result) => {
                let result_data = match task_result {
                    TaskResult::ResultTask(data) => data,
                    TaskResult::ShuffleTask(data) => data,
                };
                LocalScheduler::handle_completion_event(
                    event_queues,
                    task,
                    TaskEndReason::Success,
                    result_data,
                );
            }
            Err(err) => {
                // A lost-map-output fetch failure must route to the FetchFailed path so the
                // scheduler recomputes that map partition from lineage; everything else is a
                // plain task failure that retries the same task.
                let reason = match err.downcast_ref::<atomic_data::error::DataError>() {
                    Some(atomic_data::error::DataError::FetchFailed {
                        shuffle_id,
                        map_id,
                        server_uri,
                    }) => TaskEndReason::FetchFailed(FetchFailedVals {
                        server_uri: server_uri.clone(),
                        shuffle_id: *shuffle_id,
                        map_id: *map_id,
                        reduce_id: 0,
                    }),
                    _ => TaskEndReason::OtherFailure(format!("Task execution failed: {}", err)),
                };
                LocalScheduler::handle_completion_event(
                    event_queues,
                    task,
                    reason,
                    Box::new(()) as Box<dyn Data>, // Placeholder for error case
                );
            }
        }
    }

    fn handle_completion_event(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: TaskOption,
        reason: TaskEndReason,
        result: Box<dyn Data>,
    ) {
        let result = Some(result);
        let run_id = task.get_run_id();
        if let Some(mut queue) = event_queues.get_mut(&run_id) {
            queue.push_back(CompletionEvent {
                task,
                reason,
                result,
            });
        } else {
            log::debug!("ignoring completion event for DAG Job");
        }
    }
}

#[async_trait::async_trait]
impl NativeScheduler for LocalScheduler {
    /// Overrides the base handler to route lost *distributed* map outputs to the
    /// installed [`MapOutputRecovery`] hook. The base behavior — recompute the map
    /// partition on the driver from lineage — is only correct in local mode: a
    /// distributed staged pipeline's parent RDD is an empty placeholder, so a
    /// driver-local recompute would register an empty bucket and silently drop data.
    async fn on_event_failure<T: Data, U: Data, F, L>(
        &self,
        jt: Arc<JobTracker<F, U, T, L>>,
        failed_vals: FetchFailedVals,
        stage_id: usize,
    ) where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync,
        L: JobListener,
    {
        let FetchFailedVals {
            server_uri,
            shuffle_id,
            map_id,
            ..
        } = failed_vals;

        let m = self.state();
        // The stage that hit the fetch failure re-runs once recovery is done.
        let failed_stage = m.fetch_from_stage_cache(stage_id);
        jt.running.lock().remove(&failed_stage);
        jt.failed.lock().insert(failed_stage);

        if let Some(recover) = self.map_output_recovery.get() {
            // Another reduce partition may have already recovered this map output.
            let current = m.get_map_output_uri(shuffle_id, map_id);
            if current.is_some() && current.as_deref() != Some(server_uri.as_str()) {
                log::info!(
                    "fetch failed: shuffle {shuffle_id} map {map_id} already re-registered \
                     at {current:?}; retrying the fetching stage only"
                );
                return;
            }
            m.unregister_map_output(shuffle_id, map_id, server_uri.clone());
            if recover(shuffle_id, map_id) {
                log::warn!(
                    "fetch failed: shuffle {shuffle_id} map {map_id} on {server_uri} — \
                     recomputed on a live worker; retrying the fetching stage"
                );
                return;
            }
            // No local fallback: an unfilled tracker slot is silently skipped by the
            // reduce fetch, so completing the job would drop that partition's data.
            m.abort_job(
                jt.run_id,
                format!(
                    "lost map output (shuffle {shuffle_id} map {map_id} on {server_uri}) \
                     could not be recomputed on any live worker"
                ),
            );
            return;
        }

        log::warn!(
            "fetch failed: shuffle {shuffle_id} map {map_id} on {server_uri} — \
             invalidating the lost map output and resubmitting its stage for recompute"
        );
        m.remove_output_loc_from_stage(shuffle_id, map_id, &server_uri);
        m.unregister_map_output(shuffle_id, map_id, server_uri);
        jt.failed
            .lock()
            .insert(m.fetch_from_shuffle_to_cache(shuffle_id));
    }

    /// Every single task is run in the local thread pool
    fn submit_task<T: Data, U: Data, F>(&self, task: TaskOption, _server_address: SocketAddrV4)
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        log::debug!("inside submit task");
        let my_attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
        let event_queues = self.state.event_queues.clone();

        // No need to serialize for local execution — runs on a Tokio blocking thread
        let task_id = task.get_task_id();
        log::info!(
            "[local-worker] spawning task_id={} attempt={} on blocking thread",
            task_id,
            my_attempt_id,
        );
        tokio::task::spawn_blocking(move || {
            log::info!(
                "[local-worker] executing task_id={} thread={:?}",
                task_id,
                std::thread::current().id(),
            );
            LocalScheduler::run_task(event_queues, task, my_attempt_id)
        });
    }

    fn next_executor_server(&self, _task: &TaskOption) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)
    }

    async fn update_cache_locs(&self) -> LibResult<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl StagePlanner for LocalScheduler {
    fn state(&self) -> SchedulerState {
        self.state.clone()
    }

    async fn get_shuffle_map_stage(&self, shuf: Arc<ShuffleDependency>) -> LibResult<Stage> {
        log::debug!("getting shuffle map stage");
        let stage_id = self
            .state
            .shuffle_to_map_stage
            .get(&shuf.get_shuffle_id())
            .map(|s| s.id);
        match stage_id {
            Some(id) => {
                // Return the up-to-date copy from stage_cache — shuffle_to_map_stage holds a
                // stale clone that never sees add_output_loc_to_stage updates.
                Ok(self.state.stage_cache.get(&id).unwrap().clone())
            }
            None => {
                log::debug!("started creating shuffle map stage before");
                let stage = self
                    .new_stage(shuf.get_rdd_base(), Some(shuf.clone()))
                    .await?;
                let shuffle_id = shuf.get_shuffle_id();
                // If the distributed scheduler already completed this shuffle, mark the
                // stage available so the local scheduler doesn't re-run it and overwrite
                // the worker URIs in MapOutputTracker.
                if self.state.is_shuffle_complete(shuffle_id) {
                    let uris = self.state.get_shuffle_server_uris(shuffle_id);
                    log::debug!(
                        "shuffle #{} already complete from distributed run; \
                         pre-populating stage #{} output_locs ({} partitions, {} uris)",
                        shuffle_id,
                        stage.id,
                        stage.num_partitions,
                        uris.len()
                    );
                    // Stage uses the placeholder RDD's partition count, which may be
                    // smaller than the number of distributed map outputs (2 vs 1 for
                    // staged pipelines). Only populate as many slots as the stage has.
                    for (partition, uri) in uris.into_iter().enumerate().take(stage.num_partitions)
                    {
                        self.state.add_output_loc_to_stage(stage.id, partition, uri);
                    }
                }
                // Re-fetch from stage_cache so output_locs mutations are reflected.
                let updated = self.state.stage_cache.get(&stage.id).unwrap().clone();
                self.state
                    .shuffle_to_map_stage
                    .insert(shuffle_id, updated.clone());
                log::debug!("finished inserting newly created shuffle stage");
                Ok(updated)
            }
        }
    }
}

impl Drop for LocalScheduler {
    fn drop(&mut self) {
        self.live_listener_bus.stop().unwrap();
    }
}

#[async_trait::async_trait]
impl<U, R, E> JobListener for ApproxListener<U, R, E>
where
    U: Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
    E: ApproximateEvaluator<U, R> + Send + Sync,
{
    async fn task_succeeded(&self, _index: usize, result: &dyn Data) -> LibResult<()> {
        // Downcast the result to U and merge it into the evaluator
        if let Some(typed_result) = result.as_any().downcast_ref::<U>() {
            let mut eval = self.evaluator.lock().await;
            eval.merge(_index, typed_result);
        }
        Ok(())
    }
}
