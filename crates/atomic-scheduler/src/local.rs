use atomic_data::data::Data;
use atomic_data::dependency::ShuffleDependency;
use atomic_data::partial::result::PartialResult;
use atomic_data::partial::{ApproxListener, ApproximateEvaluator};
use atomic_data::rdd::Rdd;
use atomic_data::task::{TaskOption, TaskResult};
use atomic_data::task_context::{PartitionFn, PartitionTask};
use std::clone::Clone;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::Mutex;

use crate::base::{NativeScheduler, SchedulerState};
use crate::dag::{CompletionEvent, FetchFailedVals, TaskEndReason};
use crate::error::LibResult;
use crate::job::JobTracker;
use crate::listener::{
    BusListener, JobEndListener, JobListener, JobStartListener, LiveListenerBus, NoOpListener,
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
    /// Driver-side merge of accumulator deltas produced by `PipelineTask` runs — the local
    /// counterpart to `DistributedScheduler::accumulator_sink`. `run_task` calls it directly
    /// after unpacking a `PipelineTask`'s result; nothing routes through it for `ResultTask`/
    /// `ShuffleMapTask` since those predate `#[task]`-level accumulator support.
    accumulator_sink: Arc<std::sync::OnceLock<crate::distributed::AccumulatorSink>>,
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
            accumulator_sink: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Install the distributed map-output recovery hook. First call wins;
    /// later calls are ignored (the hook is process-lifetime, like the tracker).
    pub fn set_map_output_recovery(&self, hook: MapOutputRecovery) {
        let _ = self.map_output_recovery.set(hook);
    }

    /// Install the driver-side accumulator-delta merge sink. First call wins.
    pub fn set_accumulator_sink(&self, sink: crate::distributed::AccumulatorSink) {
        let _ = self.accumulator_sink.set(sink);
    }

    /// Register a listener to observe `JobStartListener`/`JobEndListener` events
    /// posted around every job this scheduler runs.
    pub fn add_listener(&self, listener: Arc<dyn BusListener>) {
        self.live_listener_bus.add_listener(listener);
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
        F: PartitionTask<T, U>,
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

    /// Legacy closure-dispatch entry point — see [`NativeScheduler::supports_closure_tasks`]'s
    /// doc comment. `atomic-compute`'s built-in RDD actions are the only caller; new
    /// driver-facing ops should go through the `#[task]`/`task_fn!` op-envelope path instead.
    pub fn run_job<T: Data, U: Data + Clone, F>(
        self: Arc<Self>,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> LibResult<Vec<U>>
    where
        F: PartitionTask<T, U>,
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
            self.live_listener_bus.post(Box::new(JobStartListener {
                job_id: jt.run_id,
                time: Instant::now(),
                stage_infos: vec![],
            }));
            let result = self
                .clone()
                .event_process_loop(allow_local, jt.clone())
                .await;
            self.live_listener_bus.post(Box::new(JobEndListener {
                job_id: jt.run_id,
                time: Instant::now(),
                job_result: result.is_ok(),
            }));
            result
        })
    }

    /// Entry point for a `Vec<Step>` pipeline job (`Context::dispatch_pipeline`) — the
    /// `_task`-method counterpart to [`run_job`](Self::run_job)'s closure jobs. `final_rdd`
    /// is only ever used for its `RddBase`/split shape (stage planning, preferred locations);
    /// no closure is involved, so there's no `supports_closure_tasks()` question here.
    pub fn run_pipeline_job<T: Data>(
        self: Arc<Self>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        pipeline_data: crate::job::PipelineJobData,
        partitions: Vec<usize>,
    ) -> LibResult<Vec<Vec<u8>>> {
        let sched = self.clone();
        let _lock = sched.scheduler_lock.lock();

        futures::executor::block_on(async move {
            // Never invoked — `JobTracker::pipeline` being `Some` routes final-stage task
            // construction to `PipelineTask`, not this closure. Exists only so pipeline jobs
            // can reuse `JobTracker`'s single generic shape instead of a second one.
            let func = Arc::new(
                |(_ctx, _iter): (
                    atomic_data::task_context::TaskContext,
                    Box<dyn Iterator<Item = T>>,
                )|
                 -> Vec<u8> { Vec::new() },
            );
            let jt = JobTracker::from_scheduler_pipeline(
                &*self,
                func,
                final_rdd,
                partitions,
                NoOpListener,
                pipeline_data,
            )
            .await?;
            self.live_listener_bus.post(Box::new(JobStartListener {
                job_id: jt.run_id,
                time: Instant::now(),
                stage_infos: vec![],
            }));
            let result = self.clone().event_process_loop(false, jt.clone()).await;
            self.live_listener_bus.post(Box::new(JobEndListener {
                job_id: jt.run_id,
                time: Instant::now(),
                job_result: result.is_ok(),
            }));
            result
        })
    }

    fn run_task(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: TaskOption,
        attempt_id: usize,
        accumulator_sink: &std::sync::OnceLock<crate::distributed::AccumulatorSink>,
    ) {
        let result = task.run(attempt_id);
        match result {
            Ok(task_result) => {
                let result_data = match task_result {
                    TaskResult::ResultTask(data) => data,
                    TaskResult::ShuffleTask(data) => data,
                    TaskResult::PipelineTask(data) => {
                        // Unpack `(bytes, accumulator_deltas)` — see `PipelineTask::run`'s
                        // doc comment for why this wrapping exists only for the local path.
                        let (bytes, deltas) = data
                            .as_any()
                            .downcast_ref::<(Vec<u8>, Vec<(usize, Vec<u8>)>)>()
                            .cloned()
                            .unwrap_or_default();
                        if !deltas.is_empty()
                            && let Some(sink) = accumulator_sink.get()
                        {
                            sink(&deltas);
                        }
                        Box::new(bytes) as Box<dyn Data>
                    }
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
        F: PartitionTask<T, U>,
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

    fn max_failures(&self) -> usize {
        self.max_failures
    }

    fn resubmit_timeout(&self) -> u128 {
        self.resubmit_timeout
    }

    fn poll_timeout(&self) -> u64 {
        self.poll_timeout
    }

    /// Every single task is run in the local thread pool
    fn submit_task<T: Data, U: Data, F>(&self, task: TaskOption, _server_address: SocketAddrV4)
    where
        F: PartitionFn<T, U>,
    {
        log::debug!("inside submit task");
        let my_attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
        let event_queues = self.state.event_queues.clone();
        let accumulator_sink = self.accumulator_sink.clone();

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
            LocalScheduler::run_task(event_queues, task, my_attempt_id, &accumulator_sink)
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
        self.mark_task_finished();
        Ok(())
    }
}
