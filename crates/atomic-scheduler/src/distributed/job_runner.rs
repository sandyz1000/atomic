use std::{
    collections::HashSet,
    net::SocketAddrV4,
    sync::{
        Arc,
        atomic::{AtomicI16, Ordering},
    },
    time::{Duration, Instant},
};

use atomic_data::distributed::{
    PipelineOp, StateMergePayload, TaskAction, TaskEnvelope, TaskResultEnvelope, decode_payload,
};
use parking_lot::Mutex;

use crate::{
    base::NativeScheduler,
    error::{LibResult, SchedulerError},
    job::Job,
};

use super::{
    CacheDispatch, DistributedScheduler, InflightGuard,
    locality::{CacheEndpointResolver, LocalityResolver, StateShardResolver},
};

/// A single partition's ready-to-dispatch task pair. `fallback` is set when the
/// primary is a cache-serve — it is the full recompute envelope used if the worker
/// reports a cache eviction miss.
pub(crate) struct PartitionTask {
    pub primary: TaskEnvelope,
    pub fallback: Option<TaskEnvelope>,
    /// Preferred worker for this partition (cache holder or state-shard affinity).
    pub holder_ip: Arc<Mutex<Option<SocketAddrV4>>>,
}

/// Job-level context shared across all partitions of one `run_native_job_inner` call.
struct DispatchContext<'a> {
    ops: &'a [PipelineOp],
    broadcasts: &'a [(usize, Vec<u8>)],
    plan: &'a CacheDispatch,
    state_ids: &'a [Option<u64>],
    is_merge_state: bool,
    run_id: usize,
    stage_id: usize,
    pipeline_label: &'a str,
}

/// A failed result whose error marks a lost broadcast cache on the worker — the driver
/// clears its sent record for that worker and retries so the bytes are re-sent.
fn is_broadcast_cache_miss(result: &TaskResultEnvelope) -> bool {
    use atomic_data::distributed::ResultStatus::{FatalFailure, RetryableFailure};
    matches!(result.status, RetryableFailure | FatalFailure)
        && result
            .error
            .as_deref()
            .is_some_and(|m| m.contains(atomic_data::broadcast::BROADCAST_CACHE_MISS))
}

impl DistributedScheduler {
    /// Store `result` into `slot` if the slot is not yet filled, and register
    /// cache/state locality for subsequent batches. Called by both the primary
    /// dispatch path and the speculative copy path.
    fn commit_result(
        &self,
        result: TaskResultEnvelope,
        worker_addr: SocketAddrV4,
        slot: &Arc<parking_lot::Mutex<Option<TaskResultEnvelope>>>,
        completed_durations: &Arc<parking_lot::Mutex<Vec<Duration>>>,
        elapsed: Duration,
        partition_id: usize,
    ) {
        let mut guard = slot.lock();
        if guard.is_none() {
            self.register_cache_locs(&result.cached_partitions, worker_addr);
            if !result.held_state_ids.is_empty() {
                self.register_state_locs(&result.held_state_ids, worker_addr);
            }
            self.merge_accumulator_deltas(&result.accumulator_deltas);
            *guard = Some(result);
            self.taskid_to_slaveid
                .insert(format!("{partition_id}"), worker_addr.to_string());
            completed_durations.lock().push(elapsed);
        }
    }

    /// Live endpoint for a cache-serve task, or a modulo-selected fallback if the
    /// holder is gone. Errors only when the live server list is empty.
    fn pick_preferred_worker(
        &self,
        endpoint: SocketAddrV4,
        shard: usize,
    ) -> LibResult<SocketAddrV4> {
        CacheEndpointResolver {
            endpoint,
            servers: &self.server_uris,
        }
        .resolve(shard)
        .ok_or_else(|| {
            SchedulerError::NoCompatibleWorker("no live workers for cache dispatch".to_string())
        })
    }

    /// Worker for stateful-streaming shard `state_id` / `shard` index.
    ///
    /// Report-back affinity: if `state_id` is registered in `state_locs` and that
    /// worker is still live, routes there; otherwise uses modulo placement so each
    /// shard has a stable assignment that survives worker-set changes.
    pub(crate) fn pin_state_shard(
        &self,
        shard: usize,
        state_id: Option<u64>,
    ) -> Option<SocketAddrV4> {
        StateShardResolver {
            state_id,
            state_locs: &self.state_locs,
            servers: &self.server_uris,
        }
        .resolve(shard)
    }

    /// Check that `target` supports every capability required by `task`'s ops.
    /// Returns the first unsupported capability as an error, or `Ok(())` if all pass.
    fn worker_accepts_task(
        &self,
        target: &SocketAddrV4,
        task: &TaskEnvelope,
    ) -> Result<(), SchedulerError> {
        for op in &task.ops {
            let cap = Self::required_capability(op);
            if !self.worker_has_capability(target, &cap) {
                log::warn!(
                    "worker {target} does not support capability '{cap}' — skipping to next worker"
                );
                return Err(SchedulerError::NoCompatibleWorker(format!(
                    "worker {target} does not support capability '{cap}'"
                )));
            }
        }
        Ok(())
    }

    /// The timeout to apply when dispatching `task`: `agent_step_timeout` (default
    /// [`super::AGENT_STEP_DEFAULT_TIMEOUT`]) when the pipeline contains an `AgentStep`
    /// op, since a multi-round LLM call runs far longer than a cheap CPU task —
    /// otherwise the regular `task_timeout`.
    pub(crate) fn effective_timeout(&self, task: &TaskEnvelope) -> Option<Duration> {
        let is_agent_step = task
            .ops
            .iter()
            .any(|o| matches!(o.action, TaskAction::AgentStep));
        if is_agent_step {
            Some(
                self.agent_step_timeout
                    .unwrap_or(super::AGENT_STEP_DEFAULT_TIMEOUT),
            )
        } else {
            self.task_timeout
        }
    }

    /// Send `task` to `target` exactly once, honouring [`Self::effective_timeout`].
    async fn send_task_once(
        &self,
        task: &TaskEnvelope,
        target: SocketAddrV4,
    ) -> LibResult<TaskResultEnvelope> {
        match self.effective_timeout(task) {
            Some(timeout) => {
                tokio::time::timeout(timeout, self.submit_task_to_worker(task, target))
                    .await
                    .unwrap_or_else(|_| {
                        Err(SchedulerError::Transport(format!(
                            "task timed out after {timeout:?}"
                        )))
                    })
            }
            None => self.submit_task_to_worker(task, target).await,
        }
    }

    /// Increment the consecutive TCP-failure counter for `target`. Removes the worker
    /// from the active pool when the counter reaches `MAX_WORKER_FAILURES`.
    fn record_worker_failure(&self, target: SocketAddrV4) {
        let fails = {
            let mut entry = self.worker_failures.entry(target).or_insert(0);
            *entry += 1;
            *entry
        };
        if fails >= super::MAX_WORKER_FAILURES {
            log::warn!(
                "removing dead worker {target} from pool after {fails} consecutive failures"
            );
            self.worker_capabilities.remove(&target);
            self.server_uris.lock().retain(|&ep| ep != target);
            self.inflight.remove(&target);
            self.worker_failures.remove(&target);
            self.broadcast_sent.remove(&target);
        }
    }

    /// Project `task` for `target`, dropping broadcast bytes the worker has already
    /// cached so they cross the wire only on the first task to reach it. Returns the
    /// projected envelope and the ids it newly carries, or `None` when the task has no
    /// broadcasts, in which case it is dispatched as-is with no envelope clone.
    fn project_broadcasts_for(
        &self,
        task: &TaskEnvelope,
        target: SocketAddrV4,
    ) -> Option<(TaskEnvelope, Vec<usize>)> {
        if task.broadcast_ids.is_empty() {
            return None;
        }
        let already = self
            .broadcast_sent
            .get(&target)
            .map(|s| s.clone())
            .unwrap_or_default();
        Some(task.project_broadcasts(&already))
    }

    /// Submit a single `TaskEnvelope` to a worker, retrying up to `max_failures` times.
    ///
    /// Features:
    /// - Tracks in-flight count per worker via `InflightGuard` (decrements on drop).
    /// - Per-task timeout via `task_timeout` (default 5 min).
    /// - Exponential backoff between retries: 100ms * min(2^attempt, 32).
    /// - Dead-worker removal after `MAX_WORKER_FAILURES` consecutive TCP errors.
    pub async fn submit_native_task(
        &self,
        task: &TaskEnvelope,
        preferred: Option<SocketAddrV4>,
    ) -> LibResult<(TaskResultEnvelope, SocketAddrV4)> {
        let mut last_err = None;
        'retry: for attempt in 0..=self.max_failures {
            // First attempt honours the locality hint; retries use capacity round-robin.
            let target = match (attempt, preferred) {
                (0, Some(endpoint)) => self.pick_preferred_worker(endpoint, task.partition_id)?,
                _ => self.next_executor_with_capacity()?,
            };

            if let Err(e) = self.worker_accepts_task(&target, task) {
                last_err = Some(e);
                continue 'retry;
            }

            let projected = self.project_broadcasts_for(task, target);
            let (dispatch_task, newly_sent): (&TaskEnvelope, &[usize]) = match &projected {
                Some((env, newly)) => (env, newly.as_slice()),
                None => (task, &[]),
            };

            // Track in-flight count; guard decrements on drop regardless of outcome.
            let counter = Arc::clone(
                &*self
                    .inflight
                    .entry(target)
                    .or_insert_with(|| Arc::new(AtomicI16::new(0))),
            );
            counter.fetch_add(1, Ordering::Relaxed);
            let _guard = InflightGuard(counter);

            match self.send_task_once(dispatch_task, target).await {
                Ok(result) => {
                    if is_broadcast_cache_miss(&result) {
                        self.broadcast_sent.remove(&target);
                        last_err = Some(SchedulerError::Transport(
                            "broadcast cache miss on worker; re-sending".to_string(),
                        ));
                        continue 'retry;
                    }
                    self.worker_failures.remove(&target);
                    if !newly_sent.is_empty() {
                        self.broadcast_sent
                            .entry(target)
                            .or_default()
                            .extend(newly_sent.iter().copied());
                    }
                    return Ok((result, target));
                }
                Err(e) => {
                    log::warn!(
                        "task {}/{} attempt {}/{} failed on {target}: {e}",
                        task.run_id,
                        task.task_id,
                        attempt + 1,
                        self.max_failures + 1,
                    );
                    if attempt < self.max_failures
                        && task
                            .ops
                            .iter()
                            .any(|o| matches!(o.action, TaskAction::AgentStep))
                    {
                        // No per-input checkpointing within a partition (by design — see
                        // notes/agentic-task-future-design.md): retrying re-runs every input
                        // in this partition's agent loop from scratch, including any that
                        // already produced a successful (and billed) finding.
                        log::warn!(
                            "task {}/{} is an AgentStep pipeline — retry will re-run the \
                             entire partition's agent loop (no per-input checkpointing), \
                             which re-incurs LLM cost for already-completed inputs",
                            task.run_id,
                            task.task_id,
                        );
                    }
                    self.record_worker_failure(target);
                    last_err = Some(e);
                }
            }

            // Exponential backoff: 100ms, 200ms, 400ms … 3.2s
            if attempt < self.max_failures {
                tokio::time::sleep(Duration::from_millis(100 * (1u64 << attempt).min(32))).await;
            }
        }
        Err(last_err.unwrap_or_else(|| {
            SchedulerError::NoCompatibleWorker(
                "no worker attempts were made (empty worker pool)".to_string(),
            )
        }))
    }

    /// Run a native job, attaching broadcast variable payloads to every `TaskEnvelope`.
    pub async fn run_native_job_with_broadcasts(
        &self,
        ops: Vec<PipelineOp>,
        partitions: Vec<Vec<u8>>,
        broadcasts: Vec<(usize, Vec<u8>)>,
    ) -> LibResult<Vec<Vec<u8>>> {
        if broadcasts.is_empty() {
            return self.run_native_job(ops, partitions).await;
        }
        self.run_native_job_inner(ops, partitions, broadcasts).await
    }

    pub async fn run_native_job(
        &self,
        ops: Vec<PipelineOp>,
        partitions: Vec<Vec<u8>>,
    ) -> LibResult<Vec<Vec<u8>>> {
        self.run_native_job_inner(ops, partitions, vec![]).await
    }

    /// Build one [`PartitionTask`] per partition: envelope(s), locality hint.
    fn plan_partition_dispatch(
        &self,
        ctx: &DispatchContext<'_>,
        partitions: Vec<Vec<u8>>,
    ) -> Vec<PartitionTask> {
        partitions
            .into_iter()
            .enumerate()
            .map(|(partition_id, partition_data)| {
                let task_id = self.get_mutators().get_next_task_id();
                let attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
                let task_key = format!("{}:{task_id}", ctx.run_id);
                self.taskid_to_jobid.insert(task_key.clone(), ctx.run_id);
                self.job_tasks
                    .entry(ctx.run_id)
                    .or_default()
                    .insert(task_key);
                let trace = format!("native-pipeline-{partition_id}-{}", ctx.pipeline_label);

                let holder_ip = match ctx.plan {
                    CacheDispatch::Serve { locs, .. } => {
                        Arc::new(Mutex::new(Some(locs[partition_id])))
                    }
                    CacheDispatch::Recompute => {
                        let pin = if ctx.is_merge_state {
                            self.pin_state_shard(partition_id, ctx.state_ids[partition_id])
                        } else {
                            None
                        };
                        Arc::new(Mutex::new(pin))
                    }
                };

                match ctx.plan {
                    CacheDispatch::Serve {
                        rdd_id, post_ops, ..
                    } => {
                        let primary = TaskEnvelope::new(
                            ctx.run_id,
                            ctx.stage_id,
                            task_id,
                            attempt_id,
                            partition_id,
                            trace.clone(),
                            post_ops.clone(),
                            Vec::new(),
                        )
                        .with_cache_source(*rdd_id)
                        .with_broadcasts(ctx.broadcasts.to_vec());
                        let fb_task_id = self.get_mutators().get_next_task_id();
                        let fb_attempt = self.attempt_id.fetch_add(1, Ordering::SeqCst);
                        let fallback = TaskEnvelope::new(
                            ctx.run_id,
                            ctx.stage_id,
                            fb_task_id,
                            fb_attempt,
                            partition_id,
                            format!("{trace}-recompute"),
                            ctx.ops.to_vec(),
                            partition_data,
                        )
                        .with_broadcasts(ctx.broadcasts.to_vec());
                        PartitionTask {
                            primary,
                            fallback: Some(fallback),
                            holder_ip,
                        }
                    }
                    CacheDispatch::Recompute => PartitionTask {
                        primary: TaskEnvelope::new(
                            ctx.run_id,
                            ctx.stage_id,
                            task_id,
                            attempt_id,
                            partition_id,
                            trace,
                            ctx.ops.to_vec(),
                            partition_data,
                        )
                        .with_broadcasts(ctx.broadcasts.to_vec()),
                        fallback: None,
                        holder_ip,
                    },
                }
            })
            .collect()
    }

    /// Spawn one Tokio task per partition that dispatches and retries until a result
    /// lands in its `slot`. Returns the join handles.
    fn dispatch_primary_tasks(
        &self,
        tasks: &[PartitionTask],
        slots: &[Arc<Mutex<Option<TaskResultEnvelope>>>],
        start_times: &Arc<Vec<Mutex<Option<Instant>>>>,
        completed_durations: &Arc<Mutex<Vec<Duration>>>,
        fallback_tasks: &Arc<Vec<Mutex<Option<TaskEnvelope>>>>,
        cancel_token: &tokio_util::sync::CancellationToken,
    ) -> Vec<tokio::task::JoinHandle<LibResult<()>>> {
        tasks
            .iter()
            .enumerate()
            .map(|(partition_id, pt)| {
                let mut task = pt.primary.clone();
                let slot = slots[partition_id].clone();
                let start_times = start_times.clone();
                let completed_durations = completed_durations.clone();
                let fallback_tasks = fallback_tasks.clone();
                let max_failures = self.max_failures;
                let sched = self.clone();
                let token = cancel_token.clone();
                let holder_ip_cell = pt.holder_ip.clone();

                tokio::spawn(async move {
                    *start_times[partition_id].lock() = Some(Instant::now());
                    let mut retry_count = 0usize;
                    loop {
                        if slot.lock().is_some() {
                            return Ok(());
                        }
                        if token.is_cancelled() {
                            return Err(SchedulerError::TaskFailed("job cancelled".to_string()));
                        }
                        let holder_ip = *holder_ip_cell.lock();
                        let dispatch_start = Instant::now();
                        let (result, worker_addr) = tokio::select! {
                            _ = token.cancelled() => {
                                return Err(SchedulerError::TaskFailed("job cancelled".to_string()));
                            }
                            r = sched.submit_native_task(&task, holder_ip) => r?,
                        };
                        let elapsed = dispatch_start.elapsed();

                        match result.status {
                            atomic_data::distributed::ResultStatus::FatalFailure => {
                                return Err(SchedulerError::TaskFailed(
                                    result.error.unwrap_or_else(|| "fatal failure".to_string()),
                                ));
                            }
                            atomic_data::distributed::ResultStatus::RetryableFailure => {
                                if retry_count < max_failures {
                                    retry_count += 1;
                                    tokio::time::sleep(Duration::from_millis(
                                        200 * (1u64 << retry_count).min(16),
                                    ))
                                    .await;
                                    continue;
                                }
                                return Err(SchedulerError::TaskFailed(
                                    result.error.unwrap_or_else(|| {
                                        "retryable failure exhausted".to_string()
                                    }),
                                ));
                            }
                            atomic_data::distributed::ResultStatus::CacheMiss => {
                                if let Some(fb) = fallback_tasks[partition_id].lock().take() {
                                    log::warn!(
                                        "cache evict-miss: rdd partition {partition_id}; \
                                         falling back to recompute"
                                    );
                                    sched.invalidate_worker_cache(worker_addr);
                                    *holder_ip_cell.lock() = None;
                                    task = fb;
                                    continue;
                                }
                                return Err(SchedulerError::TaskFailed(
                                    result
                                        .error
                                        .unwrap_or_else(|| "cache miss, no fallback".to_string()),
                                ));
                            }
                            atomic_data::distributed::ResultStatus::Success => {
                                sched.commit_result(
                                    result,
                                    worker_addr,
                                    &slot,
                                    &completed_durations,
                                    elapsed,
                                    partition_id,
                                );
                                return Ok(());
                            }
                        }
                    }
                })
            })
            .collect()
    }

    /// Spawn a background monitor that fires speculative copies of slow partitions.
    fn run_speculation_monitor(
        &self,
        multiplier: f64,
        tasks: &[PartitionTask],
        slots: &[Arc<Mutex<Option<TaskResultEnvelope>>>],
        start_times: &Arc<Vec<Mutex<Option<Instant>>>>,
        completed_durations: &Arc<Mutex<Vec<Duration>>>,
        num_partitions: usize,
    ) {
        let slots_ref: Vec<_> = slots.to_vec();
        let start_times_ref = start_times.clone();
        let completed_durations_ref = completed_durations.clone();
        let spec_tasks: Vec<TaskEnvelope> = tasks.iter().map(|pt| pt.primary.clone()).collect();
        let holder_ips: Vec<Arc<Mutex<Option<SocketAddrV4>>>> =
            tasks.iter().map(|pt| pt.holder_ip.clone()).collect();
        let sched = self.clone();

        tokio::spawn(async move {
            let mut speculated: HashSet<usize> = HashSet::new();
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;

                let done_count = slots_ref.iter().filter(|s| s.lock().is_some()).count();
                if done_count == num_partitions {
                    break;
                }
                if done_count * 2 < num_partitions {
                    continue;
                }

                let median = {
                    let mut durations = completed_durations_ref.lock().clone();
                    if durations.is_empty() {
                        continue;
                    }
                    durations.sort();
                    durations[durations.len() / 2]
                };
                let threshold = median.mul_f64(multiplier);

                for partition_id in 0..num_partitions {
                    if speculated.contains(&partition_id)
                        || slots_ref[partition_id].lock().is_some()
                    {
                        continue;
                    }
                    let elapsed = start_times_ref[partition_id]
                        .lock()
                        .map(|s| s.elapsed())
                        .unwrap_or(Duration::ZERO);

                    if elapsed > threshold {
                        speculated.insert(partition_id);
                        let task = spec_tasks[partition_id].clone();
                        let slot = slots_ref[partition_id].clone();
                        let completed_durations = completed_durations_ref.clone();
                        let holder_ip = *holder_ips[partition_id].lock();
                        let sched = sched.clone();

                        log::debug!(
                            "speculation: partition {partition_id} \
                             (elapsed={elapsed:?}, threshold={threshold:?})"
                        );
                        tokio::spawn(async move {
                            let start = Instant::now();
                            if let Ok((result, worker_addr)) =
                                sched.submit_native_task(&task, holder_ip).await
                                && matches!(
                                    result.status,
                                    atomic_data::distributed::ResultStatus::Success
                                )
                            {
                                sched.commit_result(
                                    result,
                                    worker_addr,
                                    &slot,
                                    &completed_durations,
                                    start.elapsed(),
                                    partition_id,
                                );
                            }
                        });
                    }
                }
            }
        });
    }

    /// Wait for all primary task handles to finish and assemble the ordered result vector.
    async fn collect_job_results(
        &self,
        handles: Vec<tokio::task::JoinHandle<LibResult<()>>>,
        slots: Vec<Arc<Mutex<Option<TaskResultEnvelope>>>>,
        run_id: usize,
    ) -> LibResult<Vec<Vec<u8>>> {
        for jr in futures::future::join_all(handles).await {
            match jr {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    self.cleanup_job(run_id);
                    return Err(e);
                }
                Err(e) => {
                    self.cleanup_job(run_id);
                    return Err(SchedulerError::TaskFailed(format!("task panicked: {e}")));
                }
            }
        }
        self.cleanup_job(run_id);

        let mut responses: Vec<TaskResultEnvelope> = slots
            .iter()
            .enumerate()
            .map(|(i, slot)| {
                slot.lock()
                    .take()
                    .unwrap_or_else(|| panic!("partition {i} slot empty after join"))
            })
            .collect();
        responses.sort_by_key(|r| r.partition_id);
        Ok(responses.into_iter().map(|r| r.data).collect())
    }

    async fn run_native_job_inner(
        &self,
        ops: Vec<PipelineOp>,
        partitions: Vec<Vec<u8>>,
        broadcasts: Vec<(usize, Vec<u8>)>,
    ) -> LibResult<Vec<Vec<u8>>> {
        let pipeline_label = ops
            .iter()
            .map(|o| o.op_id.as_str())
            .collect::<Vec<_>>()
            .join("→");
        let (run_id, stage_id) = {
            let _lock = self.scheduler_lock.lock();
            let run_id = self.get_mutators().get_next_job_id();
            let stage_id = self.get_mutators().get_next_stage_id();
            let job = Job::new(run_id, run_id);
            self.active_jobs.insert(run_id, job.clone());
            self.active_job_queue.lock().push_back(job);
            (run_id, stage_id)
        };

        let num_partitions = partitions.len();
        let cancel_token = tokio_util::sync::CancellationToken::new();
        self.job_cancel_tokens.insert(run_id, cancel_token.clone());

        // Distributed stateful-streaming merge: each partition is one state shard.
        // For report-back affinity (G1), decode the per-partition state_id before the
        // partitions vec is consumed, so `pin_state_shard` can look up registered locs.
        let is_merge_state = ops
            .iter()
            .any(|o| matches!(o.action, TaskAction::MergeState { .. }));
        let state_ids: Vec<Option<u64>> = if is_merge_state {
            partitions
                .iter()
                .map(|p| {
                    decode_payload::<StateMergePayload>(p)
                        .ok()
                        .map(|pl| pl.state_id)
                })
                .collect()
        } else {
            vec![None; num_partitions]
        };

        let plan = self.plan_cache_dispatch(&ops, num_partitions);
        let ctx = DispatchContext {
            ops: &ops,
            broadcasts: &broadcasts,
            plan: &plan,
            state_ids: &state_ids,
            is_merge_state,
            run_id,
            stage_id,
            pipeline_label: &pipeline_label,
        };
        let tasks = self.plan_partition_dispatch(&ctx, partitions);

        let slots: Vec<Arc<Mutex<Option<TaskResultEnvelope>>>> = (0..num_partitions)
            .map(|_| Arc::new(Mutex::new(None)))
            .collect();
        let start_times: Arc<Vec<Mutex<Option<Instant>>>> =
            Arc::new((0..num_partitions).map(|_| Mutex::new(None)).collect());
        let completed_durations: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::new()));
        let fallback_tasks: Arc<Vec<Mutex<Option<TaskEnvelope>>>> = Arc::new(
            tasks
                .iter()
                .map(|pt| Mutex::new(pt.fallback.clone()))
                .collect(),
        );

        let handles = self.dispatch_primary_tasks(
            &tasks,
            &slots,
            &start_times,
            &completed_durations,
            &fallback_tasks,
            &cancel_token,
        );

        // AgentStep stages are never speculated: each partition runs exactly once to
        // avoid duplicate LLM cost and non-deterministic "first winner" results.
        let has_agent_step = ops
            .iter()
            .any(|o| matches!(o.action, TaskAction::AgentStep));
        if let Some(multiplier) = self.speculation_multiplier {
            if !has_agent_step {
                self.run_speculation_monitor(
                    multiplier,
                    &tasks,
                    &slots,
                    &start_times,
                    &completed_durations,
                    num_partitions,
                );
            } else {
                log::debug!("speculation skipped for run_id={run_id}: pipeline contains AgentStep");
            }
        }

        self.collect_job_results(handles, slots, run_id).await
    }

    fn cleanup_job(&self, run_id: usize) {
        self.active_jobs.remove(&run_id);
        self.active_job_queue.lock().retain(|j| j.run_id != run_id);
        if let Some((_, task_keys)) = self.job_tasks.remove(&run_id) {
            for key in &task_keys {
                self.taskid_to_slaveid.remove(key);
                self.taskid_to_jobid.remove(key);
            }
        }
        self.job_cancel_tokens.remove(&run_id);
    }
}
