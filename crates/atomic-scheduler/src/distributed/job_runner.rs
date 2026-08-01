use std::{
    net::SocketAddrV4,
    sync::{
        Arc,
        atomic::{AtomicI16, Ordering},
    },
    time::Duration,
};

use atomic_data::distributed::{EngineAction, StepKind, TaskEnvelope, TaskResultEnvelope};

use crate::error::{LibResult, SchedulerError};

use super::{
    DistributedScheduler, InflightGuard,
    locality::{CacheEndpointResolver, LocalityResolver, StateShardResolver},
};

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

    /// Check that `target` supports every capability required by `task`'s steps.
    /// Returns the first unsupported capability as an error, or `Ok(())` if all pass.
    fn worker_accepts_task(
        &self,
        target: &SocketAddrV4,
        task: &TaskEnvelope,
    ) -> Result<(), SchedulerError> {
        for op in &task.steps {
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
            .steps
            .iter()
            .any(|o| matches!(o.kind, StepKind::Engine(EngineAction::AgentStep)));
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
                            .steps
                            .iter()
                            .any(|o| matches!(o.kind, StepKind::Engine(EngineAction::AgentStep)))
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
}
