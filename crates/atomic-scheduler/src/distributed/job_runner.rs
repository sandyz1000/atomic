use std::{
    collections::HashSet,
    net::SocketAddrV4,
    sync::{
        Arc,
        atomic::{AtomicI16, Ordering},
    },
    time::{Duration, Instant},
};

use atomic_data::distributed::{PipelineOp, TaskEnvelope, TaskResultEnvelope};
use parking_lot::Mutex;

use crate::{
    base::NativeScheduler,
    error::{LibResult, SchedulerError},
    job::Job,
};

use super::{CacheDispatch, DistributedScheduler, InflightGuard};

impl DistributedScheduler {
    /// The registered worker at `endpoint` (exact host **and** port match for cache
    /// locality), or the next capacity-based worker if it's gone (e.g. the holder died).
    fn pick_preferred_worker(&self, endpoint: SocketAddrV4) -> LibResult<SocketAddrV4> {
        {
            let servers = self.server_uris.lock();
            if let Some(addr) = servers.iter().find(|a| **a == endpoint).copied() {
                return Ok(addr);
            }
        }
        self.next_executor_with_capacity()
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
            // First attempt honors the locality hint (the worker holding a cached
            // partition); retries fall back to capacity-based round-robin.
            let target = match (attempt, preferred) {
                (0, Some(endpoint)) => self.pick_preferred_worker(endpoint)?,
                _ => self.next_executor_with_capacity()?,
            };

            // Pre-flight capability validation — skip incompatible workers without using an inflight slot.
            for op in &task.ops {
                let cap = Self::required_capability(op);
                if !self.worker_has_capability(&target, &cap) {
                    log::warn!(
                        "worker {} does not support capability '{}' — skipping to next worker",
                        target,
                        cap,
                    );
                    last_err = Some(SchedulerError::NoCompatibleWorker(format!(
                        "worker {} does not support capability '{}'",
                        target, cap,
                    )));
                    continue 'retry;
                }
            }

            // Track inflight count; guard decrements on drop regardless of outcome.
            let counter = Arc::clone(
                &*self
                    .inflight
                    .entry(target)
                    .or_insert_with(|| Arc::new(AtomicI16::new(0))),
            );
            counter.fetch_add(1, Ordering::Relaxed);
            let _guard = InflightGuard(counter);

            let send_result = match self.task_timeout {
                Some(timeout) => {
                    tokio::time::timeout(timeout, self.submit_task_to_worker(task, target))
                        .await
                        .unwrap_or_else(|_| {
                            Err(SchedulerError::Transport(format!(
                                "task timed out after {:?}",
                                timeout
                            )))
                        })
                }
                None => self.submit_task_to_worker(task, target).await,
            };

            match send_result {
                Ok(result) => {
                    self.worker_failures.remove(&target);
                    return Ok((result, target));
                }
                Err(e) => {
                    log::warn!(
                        "task {}/{} attempt {}/{} failed on {}: {}",
                        task.run_id,
                        task.task_id,
                        attempt + 1,
                        self.max_failures + 1,
                        target,
                        e
                    );
                    let fails = {
                        let mut entry = self.worker_failures.entry(target).or_insert(0);
                        *entry += 1;
                        *entry
                    };
                    if fails >= super::MAX_WORKER_FAILURES {
                        log::warn!(
                            "removing dead worker {} from pool after {} consecutive failures",
                            target,
                            fails
                        );
                        self.worker_capabilities.remove(&target);
                        self.server_uris.lock().retain(|&ep| ep != target);
                        self.inflight.remove(&target);
                        self.worker_failures.remove(&target);
                    }
                    last_err = Some(e);
                }
            }

            // Exponential backoff before next attempt: 100ms, 200ms, 400ms … 3.2s
            if attempt < self.max_failures {
                let delay = Duration::from_millis(100 * (1u64 << attempt).min(32));
                tokio::time::sleep(delay).await;
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
        let speculation_multiplier = self.speculation_multiplier;

        let cancel_token = tokio_util::sync::CancellationToken::new();
        self.job_cancel_tokens.insert(run_id, cancel_token.clone());

        let plan = self.plan_cache_dispatch(&ops, num_partitions);
        let cache_holder_ips: Vec<Arc<Mutex<Option<SocketAddrV4>>>> = match &plan {
            CacheDispatch::Serve { locs, .. } => locs
                .iter()
                .map(|addr| Arc::new(Mutex::new(Some(*addr))))
                .collect(),
            CacheDispatch::Recompute => (0..num_partitions)
                .map(|_| Arc::new(Mutex::new(None)))
                .collect(),
        };

        struct PartitionTask {
            primary: TaskEnvelope,
            fallback: Option<TaskEnvelope>,
        }

        let tasks: Vec<PartitionTask> = partitions
            .into_iter()
            .enumerate()
            .map(|(partition_id, partition_data)| {
                let task_id = self.get_mutators().get_next_task_id();
                let attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
                let task_key = format!("{}:{}", run_id, task_id);
                self.taskid_to_jobid.insert(task_key.clone(), run_id);
                self.job_tasks.entry(run_id).or_default().insert(task_key);
                let trace = format!("native-pipeline-{}-{}", partition_id, pipeline_label);
                match &plan {
                    CacheDispatch::Serve {
                        rdd_id, post_ops, ..
                    } => {
                        let primary = TaskEnvelope::new(
                            run_id,
                            stage_id,
                            task_id,
                            attempt_id,
                            partition_id,
                            trace.clone(),
                            post_ops.clone(),
                            Vec::new(),
                        )
                        .with_cache_source(*rdd_id)
                        .with_broadcasts(broadcasts.clone());
                        let fb_task_id = self.get_mutators().get_next_task_id();
                        let fb_attempt = self.attempt_id.fetch_add(1, Ordering::SeqCst);
                        let fallback = TaskEnvelope::new(
                            run_id,
                            stage_id,
                            fb_task_id,
                            fb_attempt,
                            partition_id,
                            format!("{trace}-recompute"),
                            ops.clone(),
                            partition_data,
                        )
                        .with_broadcasts(broadcasts.clone());
                        PartitionTask {
                            primary,
                            fallback: Some(fallback),
                        }
                    }
                    CacheDispatch::Recompute => PartitionTask {
                        primary: TaskEnvelope::new(
                            run_id,
                            stage_id,
                            task_id,
                            attempt_id,
                            partition_id,
                            trace,
                            ops.clone(),
                            partition_data,
                        )
                        .with_broadcasts(broadcasts.clone()),
                        fallback: None,
                    },
                }
            })
            .collect();

        // First successful result (original or speculative) fills the slot.
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

        let handles: Vec<tokio::task::JoinHandle<LibResult<()>>> = tasks
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
                let holder_ip_cell = cache_holder_ips[partition_id].clone();

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
                                    let delay =
                                        Duration::from_millis(200 * (1u64 << retry_count).min(16));
                                    tokio::time::sleep(delay).await;
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
                                    sched.invalidate_cache_for_worker(worker_addr);
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
                                let mut guard = slot.lock();
                                if guard.is_none() {
                                    sched.register_cache_locs(
                                        &result.cached_partitions,
                                        worker_addr,
                                    );
                                    *guard = Some(result);
                                    sched
                                        .taskid_to_slaveid
                                        .insert(format!("{partition_id}"), worker_addr.to_string());
                                    completed_durations.lock().push(elapsed);
                                }
                                return Ok(());
                            }
                        }
                    }
                })
            })
            .collect();

        // Speculative execution monitor.
        if let Some(multiplier) = speculation_multiplier {
            let slots_ref = slots.clone();
            let start_times_ref = start_times.clone();
            let completed_durations_ref = completed_durations.clone();
            let spec_tasks: Vec<TaskEnvelope> = tasks.iter().map(|pt| pt.primary.clone()).collect();
            let cache_holder_ips_spec: Vec<Arc<Mutex<Option<SocketAddrV4>>>> =
                cache_holder_ips.iter().map(Arc::clone).collect();
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
                        if speculated.contains(&partition_id) {
                            continue;
                        }
                        if slots_ref[partition_id].lock().is_some() {
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
                            let holder_ip = *cache_holder_ips_spec[partition_id].lock();
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
                                    let mut guard = slot.lock();
                                    if guard.is_none() {
                                        sched.register_cache_locs(
                                            &result.cached_partitions,
                                            worker_addr,
                                        );
                                        *guard = Some(result);
                                        sched.taskid_to_slaveid.insert(
                                            format!("{partition_id}"),
                                            worker_addr.to_string(),
                                        );
                                        completed_durations.lock().push(start.elapsed());
                                    }
                                }
                            });
                        }
                    }
                }
            });
        }

        let join_results = futures::future::join_all(handles).await;
        for jr in join_results {
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
