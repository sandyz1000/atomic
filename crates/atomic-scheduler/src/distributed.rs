use std::{
    collections::{BTreeSet, HashSet, VecDeque},
    fmt::Debug,
    net::{Ipv4Addr, SocketAddrV4},
    sync::{
        Arc,
        atomic::{AtomicI16, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use atomic_data::{
    data::Data,
    dependency::ShuffleDependencyBox,
    distributed::{
        PipelineOp, TRANSPORT_HEADER_LEN, TaskAction, TaskEnvelope, TaskResultEnvelope,
        TransportFrameKind, WireDecode, WireEncode, WorkerCapabilities, encode_transport_frame,
        parse_transport_header,
    },
    partial::{ApproximateEvaluator, result::PartialResult},
    rdd::Rdd,
    task::TaskOption,
    task_context::TaskContext,
};
use dashmap::DashMap;
use futures::future::try_join_all;
use parking_lot::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// RAII guard that decrements an inflight counter when dropped.
struct InflightGuard(Arc<AtomicI16>);
impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

use crate::{
    base::{Mutators, NativeScheduler},
    error::{LibResult, SchedulerError},
    job::Job,
    listener::LiveListenerBus,
    stage::Stage,
};

/// Outcome of [`DistributedScheduler::plan_cache_dispatch`]: either recompute the
/// staged pipeline, or serve a cached RDD and run the ops after the cache boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheDispatch {
    /// Run the pipeline as given (computes + caches when it ends in a `Cache` op).
    Recompute,
    /// Read partitions of `rdd_id` from cache (pinned to `locs[partition]`) and run
    /// `post_ops` on top.
    Serve {
        rdd_id: usize,
        post_ops: Vec<PipelineOp>,
        locs: Vec<SocketAddrV4>,
    },
}

#[derive(Clone, Default)]
pub struct DistributedScheduler {
    mutators: Mutators,
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,

    /// Per-worker capability declarations — keyed by endpoint.
    worker_capabilities: Arc<DashMap<SocketAddrV4, WorkerCapabilities>>,
    /// Number of tasks currently in-flight to each worker.
    inflight: Arc<DashMap<SocketAddrV4, Arc<AtomicI16>>>,
    /// Consecutive TCP-level failure count per worker — reset on success, triggers removal at MAX_WORKER_FAILURES.
    worker_failures: Arc<DashMap<SocketAddrV4, u32>>,
    /// Per-task timeout. `None` means no timeout (useful in tests / local mode).
    task_timeout: Option<Duration>,
    /// Speculative execution multiplier. When `Some(m)`, a straggler running longer
    /// than `m × median_task_duration` (once ≥50% of the stage has completed) gets a
    /// speculative re-run on a different worker; the first result wins.
    speculation_multiplier: Option<f64>,

    /// Scheduler role flag taken at construction; retained as config (driver vs. worker).
    #[allow(dead_code)]
    master: bool,
    active_jobs: Arc<DashMap<usize, Job>>,
    active_job_queue: Arc<Mutex<VecDeque<Job>>>,
    taskid_to_jobid: Arc<DashMap<String, usize>>,
    taskid_to_slaveid: Arc<DashMap<String, String>>,
    job_tasks: Arc<DashMap<usize, HashSet<String>>>,
    /// Per-job cancellation tokens — cancelled when `cancel_job()` is called.
    job_cancel_tokens: Arc<DashMap<usize, tokio_util::sync::CancellationToken>>,

    /// Fingerprint of the driver's compiled task registry — set by `with_driver_fingerprint`.
    /// Workers that advertise a different fingerprint are logged as mismatched at registration.
    driver_fingerprint: u64,

    /// Registered worker endpoints, round-robined for task dispatch.
    server_uris: Arc<Mutex<VecDeque<SocketAddrV4>>>,

    /// Per-RDD cached partition holders, keyed by full worker endpoint (host **and**
    /// port). Distinct from `Mutators.cache_locs` (host-only `Ipv4Addr`, consumed only
    /// by the legacy/unused `get_preferred_locs` DAG path) so that cache-locality
    /// dispatch is correct when multiple workers share a host IP (e.g. local
    /// multi-worker test clusters, or co-located pods in production).
    cache_endpoints: Arc<DashMap<usize, Vec<Vec<SocketAddrV4>>>>,

    scheduler_lock: Arc<Mutex<bool>>,
    live_listener_bus: LiveListenerBus,
}

/// Consecutive TCP-level failure count per worker before removal.
const MAX_WORKER_FAILURES: u32 = 3;

impl DistributedScheduler {
    pub fn new(max_failures: usize, master: bool) -> Self {
        let mut live_listener_bus = LiveListenerBus::new();
        live_listener_bus
            .start()
            .expect("LiveListenerBus failed to start its event-dispatch thread");
        Self {
            mutators: Mutators::new(),
            max_failures,
            attempt_id: Arc::new(AtomicUsize::new(0)),
            worker_capabilities: Arc::new(DashMap::new()),
            inflight: Arc::new(DashMap::new()),
            worker_failures: Arc::new(DashMap::new()),
            task_timeout: Some(Duration::from_secs(300)), // 5-minute default
            speculation_multiplier: None,
            master,
            active_jobs: Arc::new(DashMap::new()),
            active_job_queue: Arc::new(Mutex::new(VecDeque::new())),
            taskid_to_jobid: Arc::new(DashMap::new()),
            taskid_to_slaveid: Arc::new(DashMap::new()),
            job_tasks: Arc::new(DashMap::new()),
            server_uris: Arc::new(Mutex::new(VecDeque::new())),
            cache_endpoints: Arc::new(DashMap::new()),
            scheduler_lock: Arc::new(Mutex::new(false)),
            live_listener_bus,
            job_cancel_tokens: Arc::new(DashMap::new()),
            driver_fingerprint: 0,
        }
    }

    /// Cancel a running job by its `run_id`.
    ///
    /// Fires the cancellation token — all spawned task futures for that job will
    /// observe the cancellation on their next `tokio::select!` poll and return early.
    /// Returns `Err` when the job is not currently tracked (already done or never started).
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

    /// Total number of tasks currently dispatched to workers and not yet completed.
    pub fn total_inflight(&self) -> i64 {
        self.inflight
            .iter()
            .map(|e| e.value().load(Ordering::SeqCst) as i64)
            .sum()
    }

    /// Block (with a bounded timeout) until no tasks are in-flight, polling every
    /// `poll_interval`. Returns `true` if drained, `false` if the timeout elapsed
    /// with tasks still outstanding — callers proceed with shutdown regardless,
    /// since workers are long-running daemons and an abandoned task is simply
    /// recomputed on the next job.
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

    /// Start a proactive heartbeat loop that pings every registered worker every
    /// `interval_secs` seconds via `GET http://<shuffle_uri>/health`.
    ///
    /// Workers that fail `MAX_WORKER_FAILURES` consecutive heartbeat probes are
    /// removed from the active pool. Any stale shuffle-map outputs from a removed
    /// worker are cleared from `MapOutputTracker`.
    pub fn start_heartbeat(&self, interval_secs: u64, timeout_ms: u64) {
        if interval_secs == 0 {
            return;
        }
        let sched = self.clone();
        tokio::spawn(async move {
            let interval = Duration::from_secs(interval_secs);
            let timeout = Duration::from_millis(timeout_ms);
            loop {
                tokio::time::sleep(interval).await;
                let endpoints: Vec<SocketAddrV4> =
                    sched.worker_capabilities.iter().map(|e| *e.key()).collect();
                for endpoint in endpoints {
                    let shuffle_port = {
                        sched
                            .worker_capabilities
                            .get(&endpoint)
                            .and_then(|c| c.shuffle_server_port)
                    };
                    let healthy = if let Some(port) = shuffle_port {
                        let _url = format!("http://{}:{}/health", endpoint.ip(), port);
                        let probe = tokio::time::timeout(timeout, async {
                            // Lightweight HTTP GET via raw TCP (avoids pulling in full HTTP client)
                            let addr = format!("{}:{}", endpoint.ip(), port);
                            if let Ok(mut stream) = tokio::net::TcpStream::connect(&addr).await {
                                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                                let req = format!("GET /health HTTP/1.0\r\nHost: {addr}\r\n\r\n");
                                let _ = stream.write_all(req.as_bytes()).await;
                                let mut buf = [0u8; 16];
                                matches!(stream.read(&mut buf).await, Ok(n) if n > 0)
                            } else {
                                false
                            }
                        })
                        .await;
                        probe.unwrap_or(false)
                    } else {
                        // Fallback: try a plain TCP connect to the task port.
                        let probe =
                            tokio::time::timeout(timeout, tokio::net::TcpStream::connect(endpoint))
                                .await;
                        probe.is_ok_and(|r| r.is_ok())
                    };

                    if healthy {
                        sched.worker_failures.remove(&endpoint);
                    } else {
                        let mut failures = sched.worker_failures.entry(endpoint).or_insert(0);
                        *failures += 1;
                        let count = *failures;
                        drop(failures);
                        if count >= MAX_WORKER_FAILURES {
                            log::warn!(
                                "heartbeat: worker {endpoint} failed {count} consecutive probes; removing"
                            );
                            sched.remove_worker(endpoint);
                        }
                    }
                }
            }
        });
    }

    /// Whether `endpoint` is currently in the active worker pool.
    pub fn is_worker_registered(&self, endpoint: &SocketAddrV4) -> bool {
        self.worker_capabilities.contains_key(endpoint)
    }

    /// Decide whether a staged pipeline can be served from cache. If its terminal
    /// `Cache { rdd_id }` op's partitions are all present in `cache_locs`, return a
    /// `Serve` plan (read from cache + run the ops after the cache boundary, each
    /// pinned to a holding worker); otherwise `Recompute` the full pipeline.
    pub fn plan_cache_dispatch(&self, ops: &[PipelineOp], num_partitions: usize) -> CacheDispatch {
        let Some(idx) = ops
            .iter()
            .rposition(|o| matches!(o.action, TaskAction::Cache { .. }))
        else {
            return CacheDispatch::Recompute;
        };
        let TaskAction::Cache { rdd_id } = ops[idx].action else {
            return CacheDispatch::Recompute;
        };
        let Some(entry) = self.cache_endpoints.get(&rdd_id) else {
            return CacheDispatch::Recompute;
        };
        // Every partition must have at least one holding worker.
        let locs: Vec<SocketAddrV4> = (0..num_partitions)
            .map(|p| entry.get(p).and_then(|v| v.first().copied()))
            .collect::<Option<Vec<_>>>()
            .map_or(Vec::new(), |v| v);
        if locs.len() != num_partitions {
            return CacheDispatch::Recompute;
        }
        CacheDispatch::Serve {
            rdd_id,
            post_ops: ops[idx + 1..].to_vec(),
            locs,
        }
    }

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

    /// Record that `endpoint` now holds the given `(rdd_id, partition)` cached
    /// partitions, so `plan_cache_dispatch` can pin later reads to it.
    pub fn register_cache_locs(&self, cached: &[(usize, usize)], endpoint: SocketAddrV4) {
        for &(rdd_id, part) in cached {
            let mut entry = self.cache_endpoints.entry(rdd_id).or_default();
            if entry.len() <= part {
                entry.resize(part + 1, Vec::new());
            }
            if !entry[part].contains(&endpoint) {
                entry[part].push(endpoint);
            }
        }
    }

    /// Forget every cache location held by `endpoint` — the worker is gone, so its
    /// cached partitions no longer exist. Affected partitions lose their holder, so
    /// `plan_cache_dispatch` falls back to `Recompute` for them.
    fn invalidate_cache_for_worker(&self, endpoint: SocketAddrV4) {
        for mut entry in self.cache_endpoints.iter_mut() {
            for locs in entry.value_mut().iter_mut() {
                locs.retain(|a| *a != endpoint);
            }
        }
    }

    /// Forget all cache locations for `rdd_id` (used by `unpersist`); the next job
    /// over it recomputes.
    pub fn invalidate_rdd_cache(&self, rdd_id: usize) {
        self.cache_endpoints.remove(&rdd_id);
    }

    /// Remove a worker from the active pool and clean up its state.
    pub fn remove_worker(&self, endpoint: SocketAddrV4) {
        self.worker_capabilities.remove(&endpoint);
        self.server_uris.lock().retain(|e| *e != endpoint);
        self.inflight.remove(&endpoint);
        self.worker_failures.remove(&endpoint);
        // Cached partitions on the dead worker are gone — forget their locations so
        // the next job recomputes them instead of dispatching a doomed cache read.
        self.invalidate_cache_for_worker(endpoint);
        // Clear any stale shuffle-map outputs from this worker so failed
        // shuffle stages can be re-submitted on surviving workers.
        if let Some(tracker) = atomic_data::env::get_map_output_tracker() {
            let cleared = tracker.unregister_outputs_on_host(&endpoint.ip().to_string());
            if cleared > 0 {
                log::warn!(
                    "worker {endpoint} removed: invalidated {cleared} map output(s) for recompute"
                );
            }
        }
    }

    /// Dynamically add a new worker after the driver has started.
    ///
    /// Called by the HTTP `/register` route (or directly in tests) when a new
    /// worker announces itself. Safe to call concurrently with in-flight jobs.
    pub fn dynamically_add_worker(&self, endpoint: SocketAddrV4, capabilities: WorkerCapabilities) {
        log::info!(
            "dynamic worker registration: {endpoint} (max_tasks={})",
            capabilities.max_tasks
        );
        self.register_worker(endpoint, capabilities);
    }

    pub fn register_worker(&self, endpoint: SocketAddrV4, capabilities: WorkerCapabilities) {
        let driver_fp = self.driver_fingerprint;
        let worker_fp = capabilities.registry_fingerprint;
        if driver_fp != 0 {
            if worker_fp == 0 {
                log::warn!(
                    "worker {endpoint} (id={}) did not advertise a registry fingerprint — \
                     running an old binary? Proceeding, but task correctness is not guaranteed.",
                    capabilities.worker_id
                );
            } else if worker_fp != driver_fp {
                log::error!(
                    "worker {endpoint} (id={}) registry fingerprint mismatch: \
                     driver={driver_fp:#018x}, worker={worker_fp:#018x}. \
                     Task implementations diverged — redeploy workers with the same binary.",
                    capabilities.worker_id
                );
            }
        }
        self.worker_capabilities.insert(endpoint, capabilities);
        let mut servers = self.server_uris.lock();
        if !servers.contains(&endpoint) {
            servers.push_back(endpoint);
        }
    }

    /// Round-robin pick of the next available worker endpoint.
    pub fn next_executor(&self) -> LibResult<SocketAddrV4> {
        let mut servers = self.server_uris.lock();
        let endpoint = servers.pop_front().ok_or_else(|| {
            SchedulerError::NoCompatibleWorker("no registered workers".to_string())
        })?;
        servers.push_back(endpoint);
        Ok(endpoint)
    }

    /// Capacity-aware worker selection: pick a worker where in-flight count < max_tasks.
    pub fn next_executor_with_capacity(&self) -> LibResult<SocketAddrV4> {
        let mut servers = self.server_uris.lock();
        let len = servers.len();
        if len == 0 {
            return Err(SchedulerError::NoCompatibleWorker(
                "no registered workers".to_string(),
            ));
        }
        for _ in 0..len {
            let endpoint = servers.pop_front().ok_or_else(|| {
                SchedulerError::NoCompatibleWorker("no registered workers".to_string())
            })?;
            servers.push_back(endpoint);
            let max_tasks = self
                .worker_capabilities
                .get(&endpoint)
                .as_deref()
                .map(|c| c.max_tasks)
                .unwrap_or(1);
            let inflight = self
                .inflight
                .get(&endpoint)
                .map(|c| c.load(Ordering::Relaxed))
                .unwrap_or(0);
            if max_tasks > 0 && (inflight as u16) < max_tasks {
                return Ok(endpoint);
            }
        }
        Err(SchedulerError::NoCompatibleWorker(
            "all workers are at capacity".to_string(),
        ))
    }

    /// Unified capability check. For regular ops, `cap` is the `op_id`.
    /// For shuffle ops, `cap` is `"shuffle:<shuffle_key>"`.
    ///
    /// An empty `cap` means the op (e.g. `TaskAction::Cache`) is handled by the
    /// scheduler/worker runtime directly and never looked up in `TASK_REGISTRY`,
    /// so it requires no capability. An empty `registered_ops` list means "accept
    /// all" for backwards compatibility with workers that predate capability
    /// advertising.
    fn worker_has_capability(&self, endpoint: &SocketAddrV4, cap: &str) -> bool {
        if cap.is_empty() {
            return true;
        }
        self.worker_capabilities
            .get(endpoint)
            .map(|c| c.registered_ops.is_empty() || c.registered_ops.iter().any(|o| o == cap))
            .unwrap_or(false)
    }

    /// Resolve the required capability string for a pipeline op.
    ///
    /// Regular ops use their `op_id`. `ShuffleMap` ops use `"shuffle:<key>"` where
    /// `<key>` is the stringify-based type key — the first field of the
    /// bincode-encoded `ShuffleMapPayload` (`atomic-compute`'s `shuffle_map.rs`).
    /// `atomic-scheduler` has no dependency on that struct, so it decodes only the
    /// leading `String` field directly; bincode writes struct fields back-to-back
    /// with no struct-level framing, so this is byte-identical to decoding the
    /// full struct and reading `.type_id`.
    fn required_capability(op: &PipelineOp) -> String {
        use atomic_data::distributed::TaskAction;
        match &op.action {
            TaskAction::ShuffleMap { .. } => {
                let key: String =
                    bincode::decode_from_slice(&op.payload, bincode::config::standard())
                        .map(|(s, _)| s)
                        .unwrap_or_else(|_| "<invalid-payload>".to_string());
                format!("shuffle:{key}")
            }
            _ => op.op_id.clone(),
        }
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
            // Regular ops are checked by op_id; ShuffleMap ops are checked by "shuffle:<key>".
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
                    if fails >= MAX_WORKER_FAILURES {
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

    /// Run a native (non-artifact) job over a set of pre-encoded partitions.
    ///
    /// Sends one `TaskEnvelope` per partition, each carrying the full `ops` pipeline.
    /// Workers execute ops in order, threading data through each step.
    /// Returns raw result bytes per partition in submission order.
    /// Like `run_native_job` but attaches broadcast variable payloads to every `TaskEnvelope`.
    pub async fn run_native_job_with_broadcasts(
        &self,
        ops: Vec<PipelineOp>,
        partitions: Vec<Vec<u8>>,
        broadcasts: Vec<(usize, Vec<u8>)>,
    ) -> LibResult<Vec<Vec<u8>>> {
        if broadcasts.is_empty() {
            return self.run_native_job(ops, partitions).await;
        }
        // Attach broadcasts by marking each partition with them before dispatch.
        // Re-use run_native_job internals by building envelopes with broadcasts set.
        // For simplicity, run through the normal path and attach after construction.
        // The cleanest approach: pass broadcasts as context into the task-build loop.
        // We do this by intercepting at the TaskEnvelope level inside run_native_job_inner.
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

        // Create and register a cancellation token for this job.
        let cancel_token = tokio_util::sync::CancellationToken::new();
        self.job_cancel_tokens.insert(run_id, cancel_token.clone());

        // If this pipeline's cached RDD is already materialized on workers, serve it
        // from cache (pinned to holders) instead of recomputing.
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

        // For cache-serve plans, also build the full recompute envelope so that a
        // `CacheMiss` response (LRU eviction on the holder) can fall back to recompute
        // without failing the job. `None` for Recompute plans (no fallback needed).
        struct PartitionTask {
            primary: TaskEnvelope,
            fallback: Option<TaskEnvelope>,
        }

        // Build one TaskEnvelope per partition and register with job tracking.
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
                        // Fallback: full recompute pipeline with the original data.
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

        // First successful result (original or speculative) fills the slot; all
        // subsequent arrivals for the same partition are discarded.
        let slots: Vec<Arc<Mutex<Option<TaskResultEnvelope>>>> = (0..num_partitions)
            .map(|_| Arc::new(Mutex::new(None)))
            .collect();

        // Per-partition wall-clock start times (set when the task is first dispatched).
        let start_times: Arc<Vec<Mutex<Option<Instant>>>> =
            Arc::new((0..num_partitions).map(|_| Mutex::new(None)).collect());

        // Durations of completed tasks — used to compute the median for straggler detection.
        let completed_durations: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::new()));

        // Arc-wrap the fallback tasks so the dispatch futures can take ownership on CacheMiss.
        let fallback_tasks: Arc<Vec<Mutex<Option<TaskEnvelope>>>> = Arc::new(
            tasks
                .iter()
                .map(|pt| Mutex::new(pt.fallback.clone()))
                .collect(),
        );

        // `DistributedScheduler` is `Clone` and all fields are `Arc<...>`, so cloning
        // is cheap and gives a fully functional scheduler for `tokio::spawn` futures.
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
                        // Abort if a speculative copy already filled the slot.
                        if slot.lock().is_some() {
                            return Ok(());
                        }
                        // Abort if the job has been cancelled.
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
                                // The holder evicted this partition under LRU pressure. Invalidate
                                // the stale cache loc and re-dispatch the full recompute pipeline.
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

        // Polls every 500 ms. Once ≥50% of partitions complete, computes the median
        // task duration and speculatively re-runs partitions that exceed the threshold.
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
                        continue; // not enough data for a meaningful median
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

        // Wait for all primary handles.
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

    /// Run the shuffle-map phase of a shuffle stage in distributed mode.
    ///
    /// Dispatches one `TaskEnvelope` per input partition; each worker stores its output
    /// buckets in its local `ShuffleCache` and serves them via its `ShuffleManager` HTTP
    /// server. The worker returns its server URI in `TaskResultEnvelope::shuffle_server_uri`.
    /// This method registers all URIs with the driver's `MapOutputTracker` so the reduce
    /// phase can locate and fetch the right buckets from each worker.
    ///
    /// **Fault recovery**: on stage-level failure, stale URIs are cleared from
    /// `MapOutputTracker` and the entire map stage is re-submitted (up to `max_failures` times).
    pub async fn run_shuffle_map_stage(
        &self,
        shuffle_id: usize,
        ops: Vec<PipelineOp>,
        partitions: Vec<Vec<u8>>,
    ) -> LibResult<()> {
        let mut stage_attempt = 0usize;
        loop {
            match self
                .run_shuffle_map_stage_inner(shuffle_id, ops.clone(), partitions.clone())
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    stage_attempt += 1;
                    if stage_attempt > self.max_failures {
                        return Err(e);
                    }
                    // Clear stale map output URIs so the reduce phase doesn't try
                    // to fetch from the failed workers on the next attempt.
                    if let Some(tracker) = atomic_data::env::get_map_output_tracker() {
                        tracker.unregister_shuffle(shuffle_id);
                    }
                    log::warn!(
                        "shuffle-map stage for shuffle_id={} failed (attempt {}): {}; \
                         cleared MapOutputTracker, retrying",
                        shuffle_id,
                        stage_attempt,
                        e
                    );
                    let delay = Duration::from_millis(200 * (1u64 << stage_attempt).min(16));
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn run_shuffle_map_stage_inner(
        &self,
        shuffle_id: usize,
        ops: Vec<PipelineOp>,
        partitions: Vec<Vec<u8>>,
    ) -> LibResult<()> {
        let num_partitions = partitions.len();
        let m = self.get_mutators();
        let stage_id = {
            let _lock = self.scheduler_lock.lock();
            m.register_shuffle(shuffle_id, num_partitions);
            m.get_next_stage_id()
        };

        let submits = partitions.into_iter().enumerate().map(|(part_id, data)| {
            let task_id = m.get_next_task_id();
            let attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
            let trace_id = format!("shuffle-map-{shuffle_id}-{part_id}");
            let task = TaskEnvelope::new(
                0,
                stage_id,
                task_id,
                attempt_id,
                part_id,
                trace_id,
                ops.clone(),
                data,
            );
            async move {
                let mut retry_count = 0usize;
                loop {
                    let (result, _worker) = self.submit_native_task(&task, None).await?;
                    match result.status {
                        atomic_data::distributed::ResultStatus::FatalFailure
                        | atomic_data::distributed::ResultStatus::CacheMiss => {
                            return Err(SchedulerError::TaskFailed(
                                result
                                    .error
                                    .unwrap_or_else(|| "shuffle map fatal failure".to_string()),
                            ));
                        }
                        atomic_data::distributed::ResultStatus::RetryableFailure => {
                            if retry_count < self.max_failures {
                                retry_count += 1;
                                let delay =
                                    Duration::from_millis(200 * (1u64 << retry_count).min(16));
                                tokio::time::sleep(delay).await;
                                continue;
                            }
                            return Err(SchedulerError::TaskFailed(result.error.unwrap_or_else(
                                || "shuffle map retryable failure exhausted".to_string(),
                            )));
                        }
                        atomic_data::distributed::ResultStatus::Success => {
                            return Ok::<_, SchedulerError>((part_id, result.shuffle_server_uri));
                        }
                    }
                }
            }
        });

        let responses = try_join_all(submits).await?;

        let mut locs: Vec<Option<String>> = vec![None; num_partitions];
        for (part_id, uri_opt) in responses {
            if let Some(uri) = uri_opt {
                locs[part_id] = Some(uri);
            } else {
                log::warn!(
                    "shuffle-map stage {stage_id}: partition {part_id} returned no shuffle URI"
                );
            }
        }
        m.register_map_outputs(shuffle_id, locs);
        Ok(())
    }

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
        let _ = (self, func, final_rdd, evaluator, timeout);
        Err(SchedulerError::UnsupportedOperation(
            "distributed approximate jobs require the local scheduler",
        ))
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
        let _ = (self, func, final_rdd, partitions, allow_local);
        Err(SchedulerError::UnsupportedOperation(
            "use run_native_job for distributed execution",
        ))
    }

    pub async fn submit_task_to_worker(
        &self,
        task: &TaskEnvelope,
        target: SocketAddrV4,
    ) -> LibResult<TaskResultEnvelope> {
        let mut stream = TcpStream::connect(target).await?;
        let payload = task.encode_wire()?;
        Self::write_transport_frame(&mut stream, TransportFrameKind::TaskEnvelope, &payload)
            .await?;
        let (kind, payload) = Self::read_transport_frame(&mut stream).await?;
        if kind != TransportFrameKind::TaskResultEnvelope {
            return Err(SchedulerError::Transport(format!(
                "unexpected response frame kind: {:?}",
                kind
            )));
        }
        Ok(TaskResultEnvelope::decode_wire(&payload)?)
    }

    async fn write_transport_frame(
        stream: &mut TcpStream,
        frame_kind: TransportFrameKind,
        payload: &[u8],
    ) -> LibResult<()> {
        let frame = encode_transport_frame(frame_kind, payload);
        stream.write_all(&frame).await?;
        Ok(())
    }

    async fn read_transport_frame(
        stream: &mut TcpStream,
    ) -> LibResult<(TransportFrameKind, Vec<u8>)> {
        let mut header = [0_u8; TRANSPORT_HEADER_LEN];
        stream.read_exact(&mut header).await?;
        let (kind, payload_len) = parse_transport_header(&header)?;
        let mut payload = vec![0_u8; payload_len];
        stream.read_exact(&mut payload).await?;
        Ok((kind, payload))
    }
}

#[async_trait::async_trait]
impl NativeScheduler for DistributedScheduler {
    fn submit_task<T: Data, U: Data, F>(
        &self,
        task: TaskOption,
        _id_in_job: usize,
        _target_executor: SocketAddrV4,
    ) where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        let _ = (task, _id_in_job, _target_executor);
        log::debug!("legacy submit_task ignored; use run_native_job");
    }

    fn next_executor_server(&self, task: &TaskOption) -> SocketAddrV4 {
        if !task.is_pinned() {
            let socket_addr = self
                .server_uris
                .lock()
                .pop_back()
                .expect("next_executor_server called with an empty worker pool");
            self.server_uris.lock().push_front(socket_addr);
            socket_addr
        } else {
            let servers = &mut *self.server_uris.lock();
            let location: Ipv4Addr = task.preferred_locations()[0];
            if let Some((pos, _)) = servers
                .iter()
                .enumerate()
                .find(|(_, endpoint)| *endpoint.ip() == location)
            {
                let target_host = servers
                    .remove(pos)
                    .expect("invariant: pos was just produced by find() above");
                servers.push_front(target_host);
                target_host
            } else {
                unreachable!()
            }
        }
    }

    async fn update_cache_locs(&self) -> LibResult<()> {
        // Cache locations are populated incrementally as workers report cached
        // partitions (`register_cache_locs`); do not wipe them between jobs.
        Ok(())
    }

    async fn get_shuffle_map_stage(&self, shuf: Arc<ShuffleDependencyBox>) -> LibResult<Stage> {
        let stage = self
            .mutators
            .shuffle_to_map_stage
            .get(&shuf.get_shuffle_id());
        match stage {
            Some(stage) => Ok(stage.clone()),
            None => {
                let stage = self
                    .new_stage(shuf.get_rdd_base(), Some(shuf.clone()))
                    .await?;
                self.mutators
                    .shuffle_to_map_stage
                    .insert(shuf.get_shuffle_id(), stage.clone());
                Ok(stage)
            }
        }
    }

    async fn get_missing_parent_stages(&self, stage: Stage) -> LibResult<Vec<Stage>> {
        let mut missing: BTreeSet<Stage> = BTreeSet::new();
        let mut visited: HashSet<usize> = HashSet::new();
        self.visit_missing_parent(&mut missing, &mut visited, stage.get_rdd())
            .await?;
        Ok(missing.into_iter().collect())
    }

    fn get_mutators(&self) -> Mutators {
        self.mutators.clone()
    }
}

impl Drop for DistributedScheduler {
    fn drop(&mut self) {
        // Never panic in Drop — a panic here during unwinding would abort the process.
        if let Err(e) = self.live_listener_bus.stop() {
            log::warn!("failed to stop live listener bus during scheduler drop: {e}");
        }
    }
}

/// JSON body sent by a worker to `POST /register`.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RegisterRequest {
    /// The worker's own task-listener endpoint (IP:port) as a string, e.g. `"10.0.0.1:10001"`.
    pub endpoint: String,
    /// The worker's capability declaration.
    pub capabilities: atomic_data::distributed::WorkerCapabilities,
}

/// Start an HTTP listener that accepts worker self-registration on `POST /register`.
///
/// Workers POST a JSON [`RegisterRequest`] body; the driver calls
/// [`DistributedScheduler::dynamically_add_worker`] on receipt and returns HTTP 200.
/// Invalid bodies return HTTP 400. Any other path returns HTTP 404.
///
/// This mirrors the `start_metrics_server` pattern: a detached tokio task that loops
/// forever accepting connections; errors on individual connections are logged and skipped.
pub fn start_register_server(port: u16, scheduler: Arc<DistributedScheduler>) {
    tokio::spawn(async move {
        use http_body_util::{BodyExt, Full};
        use hyper::body::Bytes;
        use hyper::server::conn::http1;
        use hyper::service::service_fn;
        use hyper::{Method, Request, Response, StatusCode};

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                log::error!("register server: failed to bind on port {port}: {e}");
                return;
            }
        };

        log::info!("worker registration endpoint listening on http://0.0.0.0:{port}/register");

        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                continue;
            };
            let io = hyper_util::rt::TokioIo::new(stream);
            let sched = Arc::clone(&scheduler);

            tokio::spawn(async move {
                let _ = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(move |req: Request<hyper::body::Incoming>| {
                            let sched = Arc::clone(&sched);
                            async move {
                                let resp = if req.method() == Method::POST
                                    && req.uri().path() == "/register"
                                {
                                    let body_bytes = match req.collect().await {
                                        Ok(collected) => collected.to_bytes(),
                                        Err(e) => {
                                            log::warn!("register: failed to read body: {e}");
                                            return Ok::<_, std::convert::Infallible>(
                                                Response::builder()
                                                    .status(StatusCode::BAD_REQUEST)
                                                    .body(Full::new(Bytes::from("bad request")))
                                                    .unwrap(),
                                            );
                                        }
                                    };

                                    match serde_json::from_slice::<RegisterRequest>(&body_bytes) {
                                        Ok(reg) => match reg.endpoint.parse::<SocketAddrV4>() {
                                            Ok(endpoint) => {
                                                sched.dynamically_add_worker(
                                                    endpoint,
                                                    reg.capabilities,
                                                );
                                                Response::builder()
                                                    .status(StatusCode::OK)
                                                    .body(Full::new(Bytes::from("registered")))
                                                    .unwrap()
                                            }
                                            Err(e) => {
                                                log::warn!(
                                                    "register: invalid endpoint '{}': {e}",
                                                    reg.endpoint
                                                );
                                                Response::builder()
                                                    .status(StatusCode::BAD_REQUEST)
                                                    .body(Full::new(Bytes::from(
                                                        "invalid endpoint",
                                                    )))
                                                    .unwrap()
                                            }
                                        },
                                        Err(e) => {
                                            log::warn!("register: JSON parse error: {e}");
                                            Response::builder()
                                                .status(StatusCode::BAD_REQUEST)
                                                .body(Full::new(Bytes::from("invalid JSON")))
                                                .unwrap()
                                        }
                                    }
                                } else {
                                    Response::builder()
                                        .status(StatusCode::NOT_FOUND)
                                        .body(Full::new(Bytes::from("not found")))
                                        .unwrap()
                                };
                                Ok::<_, std::convert::Infallible>(resp)
                            }
                        }),
                    )
                    .await;
            });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomic_data::distributed::{
        ResultStatus, TRANSPORT_HEADER_LEN, TaskAction, TaskResultEnvelope, TaskRuntime,
        WireDecode, WireEncode, encode_transport_frame, parse_transport_header,
    };

    #[test]
    fn drain_returns_true_when_idle() {
        let sched = DistributedScheduler::new(4, true);
        assert_eq!(sched.total_inflight(), 0);
        assert!(sched.drain(Duration::from_millis(100), Duration::from_millis(5)));
    }

    #[test]
    fn drain_times_out_with_stuck_task() {
        let sched = DistributedScheduler::new(4, true);
        let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31099);
        sched.inflight.insert(addr, Arc::new(AtomicI16::new(1)));
        assert_eq!(sched.total_inflight(), 1);
        assert!(!sched.drain(Duration::from_millis(50), Duration::from_millis(5)));
    }

    #[test]
    fn register_worker_adds_to_server_list() {
        let scheduler = DistributedScheduler::new(4, true);
        let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31001);
        scheduler.register_worker(
            addr,
            WorkerCapabilities::new("native-1".to_string(), 2, vec![]),
        );
        let selected = scheduler.next_executor().expect("should select worker");
        assert_eq!(selected, addr);
    }

    #[test]
    fn plans_serve_when_cached() {
        use atomic_data::distributed::{PipelineOp, TaskAction, TaskRuntime};
        let sched = DistributedScheduler::new(4, true);
        let ip = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001);
        sched.register_cache_locs(&[(700, 0), (700, 1)], ip);

        let cache_op = PipelineOp {
            op_id: String::new(),
            action: TaskAction::Cache { rdd_id: 700 },
            runtime: TaskRuntime::Native,
            payload: vec![],
        };
        let map_op = PipelineOp {
            op_id: "m".into(),
            action: TaskAction::Map,
            runtime: TaskRuntime::Native,
            payload: vec![],
        };
        let ops = vec![map_op.clone(), cache_op];

        // All partitions cached → Serve, with the terminal Cache stripped.
        match sched.plan_cache_dispatch(&ops, 2) {
            CacheDispatch::Serve {
                rdd_id,
                post_ops,
                locs,
            } => {
                assert_eq!(rdd_id, 700);
                assert!(post_ops.is_empty());
                assert_eq!(locs, vec![ip, ip]);
            }
            other => panic!("expected Serve, got {other:?}"),
        }
        // Partition 2 not cached → Recompute.
        assert_eq!(sched.plan_cache_dispatch(&ops, 3), CacheDispatch::Recompute);
        // No Cache op → Recompute.
        assert_eq!(
            sched.plan_cache_dispatch(std::slice::from_ref(&map_op), 2),
            CacheDispatch::Recompute
        );
    }

    #[test]
    fn worker_death_invalidates_cache() {
        let sched = DistributedScheduler::new(4, true);
        let dead = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001);
        let live = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 2), 11002);
        sched.register_cache_locs(&[(800, 0), (800, 1)], dead);
        sched.register_cache_locs(&[(800, 1)], live); // partition 1 replicated

        sched.remove_worker(dead);

        let locs = sched.cache_endpoints.clone();
        let entry = locs.get(&800).unwrap();
        assert!(entry[0].is_empty(), "dead worker's sole copy removed");
        assert_eq!(entry[1], vec![live], "replica on live worker survives");
    }

    #[test]
    fn unpersist_clears_cache() {
        let sched = DistributedScheduler::new(4, true);
        sched.register_cache_locs(
            &[(801, 0)],
            SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001),
        );
        assert!(sched.cache_endpoints.contains_key(&801));
        sched.invalidate_rdd_cache(801);
        assert!(!sched.cache_endpoints.contains_key(&801));
    }

    #[test]
    fn cache_locs_register_grows() {
        let scheduler = DistributedScheduler::new(4, true);
        let ip = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 7), 11007);
        // Reporting partition 2 first must grow the per-rdd Vec to fit it.
        scheduler.register_cache_locs(&[(500, 2), (500, 0)], ip);
        let locs = scheduler.cache_endpoints.clone();
        let entry = locs.get(&500).expect("rdd 500 registered");
        assert_eq!(entry.len(), 3);
        assert_eq!(entry[2], vec![ip]);
        assert_eq!(entry[0], vec![ip]);
        assert!(entry[1].is_empty());
        // Idempotent: re-reporting the same (partition, ip) does not duplicate.
        drop(entry);
        scheduler.register_cache_locs(&[(500, 2)], ip);
        assert_eq!(locs.get(&500).unwrap()[2], vec![ip]);
    }

    #[test]
    fn next_executor_round_robins() {
        let scheduler = DistributedScheduler::new(4, true);
        let addr1 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31011);
        let addr2 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31012);
        scheduler.register_worker(addr1, WorkerCapabilities::new("w1".to_string(), 1, vec![]));
        scheduler.register_worker(addr2, WorkerCapabilities::new("w2".to_string(), 1, vec![]));
        let first = scheduler.next_executor().unwrap();
        let second = scheduler.next_executor().unwrap();
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn submits_native_task_envelope_and_reads_result() {
        let scheduler = DistributedScheduler::new(4, true);
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind");
        let endpoint = listener.local_addr().expect("local addr");
        let endpoint = SocketAddrV4::new(Ipv4Addr::LOCALHOST, endpoint.port());
        scheduler.register_worker(
            endpoint,
            WorkerCapabilities::new("w1".to_string(), 1, vec![]),
        );

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut header = [0_u8; TRANSPORT_HEADER_LEN];
            socket.read_exact(&mut header).await.expect("read header");
            let (kind, payload_len) = parse_transport_header(&header).expect("parse header");
            assert_eq!(kind, TransportFrameKind::TaskEnvelope);
            let mut payload = vec![0_u8; payload_len];
            socket.read_exact(&mut payload).await.expect("read payload");
            let task = TaskEnvelope::decode_wire(&payload).expect("decode task");
            assert_eq!(task.ops[0].op_id, "mycrate::double");
            let response = TaskResultEnvelope::ok(
                task.run_id,
                task.stage_id,
                task.task_id,
                task.attempt_id,
                task.partition_id,
                "worker-1".to_string(),
                vec![42],
                None,
            );
            let resp_bytes = response.encode_wire().expect("encode response");
            let frame = encode_transport_frame(TransportFrameKind::TaskResultEnvelope, &resp_bytes);
            socket.write_all(&frame).await.expect("write response");
        });

        let task = TaskEnvelope::new(
            1,
            2,
            3,
            0,
            0,
            "trace-1".to_string(),
            vec![PipelineOp {
                op_id: "mycrate::double".to_string(),
                action: TaskAction::Map,
                runtime: TaskRuntime::Native,
                payload: vec![],
            }],
            vec![1, 2, 3],
        );
        let result = scheduler
            .submit_task_to_worker(&task, endpoint)
            .await
            .expect("submit");
        assert_eq!(result.status, ResultStatus::Success);
        assert_eq!(result.data, vec![42]);
        server.await.expect("server join");
    }
}
