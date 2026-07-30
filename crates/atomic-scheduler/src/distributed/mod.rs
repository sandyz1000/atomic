use std::{
    collections::{HashSet, VecDeque},
    fmt::Debug,
    net::{Ipv4Addr, SocketAddrV4},
    sync::{
        Arc,
        atomic::{AtomicI16, AtomicUsize, Ordering},
    },
    time::Duration,
};

use atomic_data::{
    data::Data,
    dependency::ShuffleDependency,
    distributed::{EngineAction, Step, StepKind, TaskEnvelope, TaskRuntime, WorkerCapabilities},
    partial::{ApproximateEvaluator, result::PartialResult},
    rdd::Rdd,
    task::{ShuffleMapTask, TaskOption},
    task_context::{PartitionFn, PartitionTask},
};
use dashmap::DashMap;
use parking_lot::Mutex;

use crate::{
    base::{NativeScheduler, SchedulerState},
    dag::{CompletionEvent, TaskEndReason},
    error::{LibResult, SchedulerError},
    job::{Job, JobTracker},
    listener::LiveListenerBus,
    planner::StagePlanner,
    stage::Stage,
};

mod allocator;
mod cache_dispatch;
mod job_runner;
pub(crate) mod locality;
mod register_server;
pub(crate) mod retry;
mod shuffle_stage;
#[cfg(test)]
mod tests;
mod transport;
mod worker_pool;

pub use allocator::{
    AllocatorError, AllocatorResult, ResourceProfile, StaticAllocator, WorkerAllocator,
};
pub use cache_dispatch::CacheDispatch;
pub use register_server::{RegisterRequest, start_register_server};
pub use shuffle_stage::ActiveShuffleStage;
pub use worker_pool::InflightGuard;

/// Consecutive TCP-level failure count per worker before removal.
pub(crate) const MAX_WORKER_FAILURES: u32 = 3;

/// Default per-task timeout for `AgentStep` pipelines when `agent_step_timeout` is
/// unset. Multi-round LLM calls (with provider-side retry/backoff already happening
/// inside the agent runner) need far more headroom than the 5-minute CPU-task default.
pub(crate) const AGENT_STEP_DEFAULT_TIMEOUT: Duration = Duration::from_secs(1800);

/// Driver-side merge of `TaskResultEnvelope::accumulator_deltas`, installed by the
/// compute context. Called once per committed task result (duplicate speculative
/// results are dropped before the sink fires); a retried stage re-runs its tasks,
/// so accumulators in retried stages may over-count — same caveat as local mode.
pub type AccumulatorSink = Arc<dyn Fn(&[(usize, Vec<u8>)]) + Send + Sync>;

#[derive(Clone, Default)]
pub struct DistributedScheduler {
    pub(crate) state: SchedulerState,
    pub(crate) max_failures: usize,
    pub(crate) attempt_id: Arc<AtomicUsize>,

    /// Per-worker capability declarations — keyed by endpoint.
    pub(crate) worker_capabilities: Arc<DashMap<SocketAddrV4, WorkerCapabilities>>,
    /// Number of tasks currently in-flight to each worker.
    pub(crate) inflight: Arc<DashMap<SocketAddrV4, Arc<AtomicI16>>>,
    /// Consecutive TCP-level failure count per worker — reset on success, triggers removal at MAX_WORKER_FAILURES.
    pub(crate) worker_failures: Arc<DashMap<SocketAddrV4, u32>>,
    /// Broadcast ids already shipped to each worker. The driver sends a broadcast's
    /// bytes only on the first task that reaches a given endpoint; later tasks carry
    /// just the ids and the worker reads from its process-global cache. Cleared when a
    /// worker is removed or re-registers, so a restarted (cache-empty) worker is re-sent.
    pub(crate) broadcast_sent: Arc<DashMap<SocketAddrV4, HashSet<usize>>>,
    /// Per-task timeout. `None` means no timeout (useful in tests / local mode).
    pub(crate) task_timeout: Option<Duration>,
    /// Per-task timeout for pipelines containing an `AgentStep` op. Multi-round LLM
    /// calls run far longer than the cheap-CPU-task default `task_timeout`, so this
    /// is a separate, larger knob. Falls back to `AGENT_STEP_DEFAULT_TIMEOUT` when unset.
    pub(crate) agent_step_timeout: Option<Duration>,
    /// Speculative execution multiplier.
    pub(crate) speculation_multiplier: Option<f64>,

    /// Scheduler role flag taken at construction; retained as config (driver vs. worker).
    #[allow(dead_code)]
    pub(crate) master: bool,
    pub(crate) active_jobs: Arc<DashMap<usize, Job>>,
    pub(crate) active_job_queue: Arc<Mutex<VecDeque<Job>>>,
    pub(crate) taskid_to_jobid: Arc<DashMap<String, usize>>,
    pub(crate) taskid_to_slaveid: Arc<DashMap<String, String>>,
    pub(crate) job_tasks: Arc<DashMap<usize, HashSet<String>>>,
    /// Per-job cancellation tokens — cancelled when `cancel_job()` is called.
    pub(crate) job_cancel_tokens: Arc<DashMap<usize, tokio_util::sync::CancellationToken>>,

    /// Fingerprint of the driver's compiled task registry.
    pub(crate) driver_fingerprint: u64,

    /// Registered worker endpoints, round-robined for task dispatch.
    pub(crate) server_uris: Arc<Mutex<VecDeque<SocketAddrV4>>>,

    /// Per-RDD cached partition holders, keyed by full worker endpoint.
    pub(crate) cache_endpoints: Arc<DashMap<usize, Vec<Vec<SocketAddrV4>>>>,

    /// State shard → holding worker, built from `TaskResultEnvelope.held_state_ids`.
    /// Mirrors `cache_endpoints` for `MergeState` tasks: the driver routes each shard
    /// to its registered worker (the one that holds the shard's in-memory state), falling
    /// back to the modulo-based `pin_state_shard` for the first batch / cold shards.
    pub(crate) state_locs: Arc<DashMap<u64, SocketAddrV4>>,

    pub(crate) scheduler_lock: Arc<Mutex<bool>>,
    pub(crate) live_listener_bus: LiveListenerBus,

    /// Merges worker-reported accumulator deltas into the driver store; `None`
    /// until the compute context installs it.
    pub(crate) accumulator_sink: Arc<std::sync::OnceLock<AccumulatorSink>>,
}

impl DistributedScheduler {
    pub fn new(max_failures: usize, master: bool) -> Self {
        let mut live_listener_bus = LiveListenerBus::new();
        live_listener_bus
            .start()
            .expect("LiveListenerBus failed to start its event-dispatch thread");
        Self {
            state: SchedulerState::new(),
            max_failures,
            attempt_id: Arc::new(AtomicUsize::new(0)),
            worker_capabilities: Arc::new(DashMap::new()),
            inflight: Arc::new(DashMap::new()),
            worker_failures: Arc::new(DashMap::new()),
            broadcast_sent: Arc::new(DashMap::new()),
            task_timeout: Some(Duration::from_secs(300)),
            agent_step_timeout: None,
            speculation_multiplier: None,
            master,
            active_jobs: Arc::new(DashMap::new()),
            active_job_queue: Arc::new(Mutex::new(VecDeque::new())),
            taskid_to_jobid: Arc::new(DashMap::new()),
            taskid_to_slaveid: Arc::new(DashMap::new()),
            job_tasks: Arc::new(DashMap::new()),
            server_uris: Arc::new(Mutex::new(VecDeque::new())),
            cache_endpoints: Arc::new(DashMap::new()),
            state_locs: Arc::new(DashMap::new()),
            scheduler_lock: Arc::new(Mutex::new(false)),
            live_listener_bus,
            job_cancel_tokens: Arc::new(DashMap::new()),
            driver_fingerprint: 0,
            accumulator_sink: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Install the driver-side accumulator-delta merge. First call wins.
    pub fn set_accumulator_sink(&self, sink: AccumulatorSink) {
        let _ = self.accumulator_sink.set(sink);
    }

    /// Register a listener to observe `JobStartListener`/`JobEndListener` events
    /// posted around every job this scheduler dispatches.
    pub fn add_listener(&self, listener: Arc<dyn crate::listener::BusListener>) {
        self.live_listener_bus.add_listener(listener);
    }

    /// Forward non-empty accumulator deltas to the installed sink, if any.
    pub(crate) fn merge_accumulator_deltas(&self, deltas: &[(usize, Vec<u8>)]) {
        if !deltas.is_empty()
            && let Some(sink) = self.accumulator_sink.get()
        {
            sink(deltas);
        }
    }

    /// Cancel a running job by its `run_id`.
    pub fn cancel_job(&self, run_id: usize) -> Result<(), SchedulerError> {
        if let Some(token) = self.job_cancel_tokens.get(&run_id) {
            token.cancel();
            Ok(())
        } else {
            Err(SchedulerError::TaskFailed(format!(
                "job {run_id} not found or already completed"
            )))
        }
    }

    /// Set the driver's registry fingerprint for worker mismatch detection at registration.
    pub fn with_driver_fingerprint(mut self, fp: u64) -> Self {
        self.driver_fingerprint = fp;
        self
    }

    /// Build a job-scoped view whose task placement is restricted to `endpoints`.
    ///
    /// Every other piece of state — worker capabilities, in-flight counts, caches,
    /// trackers, heartbeat — is shared with `self` via `Arc`; only the round-robin
    /// placement queue (`server_uris`) is private to the returned view. This pins a
    /// job to a dedicated set of workers without changing any placement code, which
    /// all reads `self.server_uris`. The endpoints must already be registered in
    /// `worker_capabilities` (capacity-aware placement consults it).
    pub fn scoped_to(&self, endpoints: Vec<SocketAddrV4>) -> Self {
        let mut view = self.clone();
        view.server_uris = Arc::new(Mutex::new(allocator::placement_queue(endpoints)));
        view
    }

    /// Total number of tasks currently dispatched to workers and not yet completed.
    pub fn total_inflight(&self) -> i64 {
        self.inflight
            .iter()
            .map(|e| e.value().load(Ordering::SeqCst) as i64)
            .sum()
    }

    /// Block (with a bounded timeout) until no tasks are in-flight, polling every
    /// `poll_interval`. Returns `true` if drained, `false` if the timeout elapsed.
    pub fn drain(&self, timeout: Duration, poll_interval: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while self.total_inflight() > 0 {
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(poll_interval);
        }
        true
    }

    /// Enable speculative execution with the given multiplier.
    pub fn with_speculation(mut self, multiplier: f64) -> Self {
        self.speculation_multiplier = Some(multiplier);
        self
    }

    /// Override the per-task timeout used for pipelines containing an `AgentStep` op.
    pub fn with_agent_step_timeout(mut self, timeout: Duration) -> Self {
        self.agent_step_timeout = Some(timeout);
        self
    }

    /// Push a completion event into the DAG event queue for `run_id`.
    fn enqueue_completion_event(&self, run_id: usize, event: CompletionEvent) {
        if let Some(mut queue) = self.state.event_queues.get_mut(&run_id) {
            queue.push_back(event);
        } else {
            log::debug!(
                "dropping completion event for run_id={run_id} (event queue already removed)"
            );
        }
    }

    /// Convert one submitted [`TaskOption`] into a [`TaskEnvelope`] the distributed workers can run.
    ///
    /// Today only `ShuffleMapTask` has a complete op payload here. `ResultTask` still carries a
    /// driver closure and is rejected in distributed mode.
    fn build_shuffle_task_envelope(&self, task: &ShuffleMapTask) -> LibResult<TaskEnvelope> {
        let shuffle_dep = task.dep.get_shuffle_dep().ok_or_else(|| {
            SchedulerError::TaskFailed(
                "shuffle-map task missing shuffle dependency in task.dep".to_string(),
            )
        })?;
        let partitions = shuffle_dep.encode_partitions().map_err(|e| {
            SchedulerError::TaskFailed(format!("shuffle map partition encode: {e}"))
        })?;
        let data = partitions
            .get(task.meta.partition)
            .cloned()
            .ok_or_else(|| {
                SchedulerError::TaskFailed(format!(
                    "shuffle map partition {} out of bounds ({} partitions)",
                    task.meta.partition,
                    partitions.len()
                ))
            })?;

        let payload = bincode::encode_to_vec(
            atomic_data::distributed::ShuffleMapPayload {
                type_id: shuffle_dep.type_id.to_string(),
                partitioner_spec: shuffle_dep.partitioner_spec(),
            },
            bincode::config::standard(),
        )
        .map_err(|e| SchedulerError::TaskFailed(format!("shuffle-map payload encode: {e}")))?;

        let mut steps: Vec<Step> = shuffle_dep.preceding_steps.clone();
        steps.push(Step {
            task_name: format!("shuffle-map-{}", shuffle_dep.get_shuffle_id()),
            kind: StepKind::Engine(EngineAction::ShuffleMap {
                shuffle_id: shuffle_dep.get_shuffle_id(),
                num_output_partitions: shuffle_dep.get_num_output_partitions(),
            }),
            runtime: TaskRuntime::Native,
            payload,
        });

        let attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
        let trace = format!(
            "native-submit-shuffle-{}-{}",
            shuffle_dep.get_shuffle_id(),
            task.meta.partition
        );
        Ok(TaskEnvelope::new(
            task.meta.run_id,
            task.meta.stage_id,
            task.meta.task_id,
            attempt_id,
            task.meta.partition,
            trace,
            steps,
            data,
        ))
    }

    /// Distributed counterpart to `execute_distributed_shuffle_task` for a `PipelineTask`
    /// (a `Vec<Step>` `Stage`'s unit of work). Ships the same `TaskEnvelope`
    /// `PipelineTask::to_envelope` builds for local dispatch — the only difference between
    /// `LocalScheduler` and `DistributedScheduler` running this task is thread vs. worker.
    async fn execute_distributed_pipeline_task(
        &self,
        task_option: TaskOption,
        task: atomic_data::task::PipelineTask,
        target_executor: SocketAddrV4,
    ) -> CompletionEvent {
        let attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
        let envelope = task.to_envelope(attempt_id);

        match self
            .submit_native_task(&envelope, Some(target_executor))
            .await
        {
            Ok((result, _worker_addr)) => match result.status {
                atomic_data::distributed::ResultStatus::Success => {
                    self.merge_accumulator_deltas(&result.accumulator_deltas);
                    // Same registration `PipelineTask::run` does for the local path — see
                    // its doc comment. Here the worker's `shuffle_server_uri` arrives
                    // directly on the RPC response instead of a `PipelineExecutor` return.
                    if let Some(uri) = &result.shuffle_server_uri
                        && let Some((shuffle_id, num_output_partitions)) = task.shuffle_dep()
                    {
                        let state = self.state();
                        state.register_shuffle(shuffle_id, num_output_partitions);
                        state.register_map_output(shuffle_id, task.meta.partition, uri.clone());
                    }
                    CompletionEvent {
                        task: task_option,
                        reason: TaskEndReason::Success,
                        result: Some(Box::new(result.data) as Box<dyn Data>),
                    }
                }
                atomic_data::distributed::ResultStatus::RetryableFailure
                | atomic_data::distributed::ResultStatus::FatalFailure
                | atomic_data::distributed::ResultStatus::CacheMiss => CompletionEvent {
                    task: task_option,
                    reason: TaskEndReason::OtherFailure(
                        result
                            .error
                            .unwrap_or_else(|| "distributed pipeline task failed".to_string()),
                    ),
                    result: Some(Box::new(()) as Box<dyn Data>),
                },
            },
            Err(e) => CompletionEvent {
                task: task_option,
                reason: TaskEndReason::OtherFailure(e.to_string()),
                result: Some(Box::new(()) as Box<dyn Data>),
            },
        }
    }

    async fn execute_distributed_shuffle_task(
        &self,
        task_option: TaskOption,
        task: ShuffleMapTask,
        target_executor: SocketAddrV4,
    ) -> CompletionEvent {
        let envelope = match self.build_shuffle_task_envelope(&task) {
            Ok(env) => env,
            Err(e) => {
                return CompletionEvent {
                    task: task_option,
                    reason: TaskEndReason::OtherFailure(e.to_string()),
                    result: Some(Box::new(()) as Box<dyn Data>),
                };
            }
        };

        match self
            .submit_native_task(&envelope, Some(target_executor))
            .await
        {
            Ok((result, _worker_addr)) => match result.status {
                atomic_data::distributed::ResultStatus::Success => {
                    self.merge_accumulator_deltas(&result.accumulator_deltas);
                    match result.shuffle_server_uri {
                        Some(uri) => CompletionEvent {
                            task: task_option,
                            reason: TaskEndReason::Success,
                            result: Some(Box::new(uri) as Box<dyn Data>),
                        },
                        None => CompletionEvent {
                            task: task_option,
                            reason: TaskEndReason::OtherFailure(
                                "shuffle task succeeded but worker returned no shuffle_server_uri"
                                    .to_string(),
                            ),
                            result: Some(Box::new(()) as Box<dyn Data>),
                        },
                    }
                }
                atomic_data::distributed::ResultStatus::RetryableFailure
                | atomic_data::distributed::ResultStatus::FatalFailure
                | atomic_data::distributed::ResultStatus::CacheMiss => CompletionEvent {
                    task: task_option,
                    reason: TaskEndReason::OtherFailure(
                        result
                            .error
                            .unwrap_or_else(|| "distributed shuffle task failed".to_string()),
                    ),
                    result: Some(Box::new(()) as Box<dyn Data>),
                },
            },
            Err(e) => CompletionEvent {
                task: task_option,
                reason: TaskEndReason::OtherFailure(e.to_string()),
                result: Some(Box::new(()) as Box<dyn Data>),
            },
        }
    }

    async fn execute_submitted_task(
        &self,
        task: TaskOption,
        target_executor: SocketAddrV4,
    ) -> CompletionEvent {
        match task.clone() {
            TaskOption::ShuffleMapTask(shuffle_task) => {
                self.execute_distributed_shuffle_task(task, shuffle_task, target_executor)
                    .await
            }
            TaskOption::PipelineTask(pipeline_task) => {
                self.execute_distributed_pipeline_task(task, pipeline_task, target_executor)
                    .await
            }
            TaskOption::ResultTask(_) => CompletionEvent {
                task,
                reason: TaskEndReason::OtherFailure(
                    "distributed ResultTask is unsupported: closure-backed task IR cannot be shipped; use task_fn!/map_task-style op pipelines".to_string(),
                ),
                result: Some(Box::new(()) as Box<dyn Data>),
            },
        }
    }

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
        let _ = (self, func, final_rdd, evaluator, timeout);
        Err(SchedulerError::UnsupportedOperation(
            "distributed approximate jobs require the local scheduler",
        ))
    }

    /// Entry point for a `Vec<Step>` pipeline job (`Context::dispatch_pipeline`) — the
    /// distributed counterpart to `LocalScheduler::run_pipeline_job`. Drives the same
    /// `NativeScheduler::event_process_loop`; `submit_task`'s worker-RPC dispatch (not this
    /// function) is the only thing that differs from the local path.
    pub async fn run_pipeline_job<T: Data>(
        self: Arc<Self>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        pipeline_data: crate::job::PipelineJobData,
        partitions: Vec<usize>,
    ) -> LibResult<Vec<Vec<u8>>> {
        // Never invoked — see the identical note on `LocalScheduler::run_pipeline_job`.
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
            crate::listener::NoOpListener,
            pipeline_data,
        )
        .await?;
        self.event_process_loop(false, jt).await
    }
}

#[async_trait::async_trait]
impl NativeScheduler for DistributedScheduler {
    fn submit_task<T: Data, U: Data, F>(&self, task: TaskOption, target_executor: SocketAddrV4)
    where
        F: PartitionFn<T, U>,
    {
        let run_id = task.get_run_id();
        let scheduler = self.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            let event = match runtime {
                Ok(rt) => rt.block_on(scheduler.execute_submitted_task(task, target_executor)),
                Err(e) => CompletionEvent {
                    task,
                    reason: TaskEndReason::OtherFailure(format!(
                        "failed to initialize tokio runtime for submit_task: {e}"
                    )),
                    result: Some(Box::new(()) as Box<dyn Data>),
                },
            };
            scheduler.enqueue_completion_event(run_id, event);
        });
    }

    fn next_executor_server(&self, task: &TaskOption) -> SocketAddrV4 {
        let mut servers = self.server_uris.lock();
        if servers.is_empty() {
            log::warn!("next_executor_server called with an empty worker pool");
            return SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
        }

        let round_robin = |s: &mut VecDeque<SocketAddrV4>| {
            let addr = s.pop_back().expect("server list checked non-empty above");
            s.push_front(addr);
            addr
        };

        if !task.is_pinned() {
            return round_robin(&mut servers);
        }

        let preferred = match task.preferred_locations().first().copied() {
            Some(ip) => ip,
            None => return round_robin(&mut servers),
        };

        if let Some((pos, _)) = servers
            .iter()
            .enumerate()
            .find(|(_, endpoint)| *endpoint.ip() == preferred)
        {
            let target_host = servers
                .remove(pos)
                .expect("invariant: pos was just produced by find() above");
            servers.push_front(target_host);
            target_host
        } else {
            log::warn!("pinned preferred worker {preferred} not live; falling back to round-robin");
            round_robin(&mut servers)
        }
    }

    async fn update_cache_locs(&self) -> LibResult<()> {
        // Cache locations are populated incrementally as workers report cached
        // partitions (`register_cache_locs`); do not wipe them between jobs.
        Ok(())
    }

    fn supports_closure_tasks(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl StagePlanner for DistributedScheduler {
    async fn get_shuffle_map_stage(&self, shuf: Arc<ShuffleDependency>) -> LibResult<Stage> {
        let stage = self.state.shuffle_to_map_stage.get(&shuf.get_shuffle_id());
        match stage {
            Some(stage) => Ok(stage.clone()),
            None => {
                let stage = self
                    .new_stage(shuf.get_rdd_base(), Some(shuf.clone()))
                    .await?;
                self.state
                    .shuffle_to_map_stage
                    .insert(shuf.get_shuffle_id(), stage.clone());
                Ok(stage)
            }
        }
    }

    fn state(&self) -> SchedulerState {
        self.state.clone()
    }
}

impl Drop for DistributedScheduler {
    fn drop(&mut self) {
        if let Err(e) = self.live_listener_bus.stop() {
            log::warn!("failed to stop live listener bus during scheduler drop: {e}");
        }
    }
}
