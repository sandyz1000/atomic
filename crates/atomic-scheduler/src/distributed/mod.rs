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
    dependency::ErasedShuffleDependency,
    distributed::WorkerCapabilities,
    partial::{ApproximateEvaluator, result::PartialResult},
    rdd::Rdd,
    task::TaskOption,
    task_context::TaskContext,
};
use dashmap::DashMap;
use parking_lot::Mutex;

use crate::{
    base::{NativeScheduler, SchedulerState},
    error::{LibResult, SchedulerError},
    job::Job,
    listener::LiveListenerBus,
    planner::StagePlanner,
    stage::Stage,
};

mod allocator;
mod cache_dispatch;
mod job_runner;
pub(crate) mod locality;
pub(crate) mod retry;
mod shuffle_stage;
mod transport;
mod worker_pool;

pub use allocator::{
    AllocatorError, AllocatorResult, ResourceProfile, StaticAllocator, WorkerAllocator,
};
pub use cache_dispatch::CacheDispatch;
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
}

#[async_trait::async_trait]
impl NativeScheduler for DistributedScheduler {
    fn submit_task<T: Data, U: Data, F>(&self, task: TaskOption, _target_executor: SocketAddrV4)
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        let _ = (task, _target_executor);
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
}

#[async_trait::async_trait]
impl StagePlanner for DistributedScheduler {
    async fn get_shuffle_map_stage(&self, shuf: Arc<ErasedShuffleDependency>) -> LibResult<Stage> {
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

/// JSON body sent by a worker to `POST /register`.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RegisterRequest {
    /// The worker's own task-listener endpoint (IP:port) as a string, e.g. `"10.0.0.1:10001"`.
    pub endpoint: String,
    /// The worker's capability declaration.
    pub capabilities: atomic_data::distributed::WorkerCapabilities,
}

/// Start an HTTP listener that accepts worker self-registration on `POST /register`.
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
                                let authorized = atomic_data::env::bearer_authorized(
                                    req.headers()
                                        .get(hyper::header::AUTHORIZATION)
                                        .and_then(|value| value.to_str().ok()),
                                );
                                let resp = if !authorized {
                                    log::warn!("register: rejected unauthenticated request");
                                    Response::builder()
                                        .status(StatusCode::UNAUTHORIZED)
                                        .body(Full::new(Bytes::from("unauthorized")))
                                        .unwrap()
                                } else if req.method() == Method::POST
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
        OpKind, PipelineOp, ResultStatus, StepKind, TRANSPORT_HEADER_LEN, TaskAction, TaskEnvelope,
        TaskResultEnvelope, TaskRuntime, TransportFrameKind, WireDecode, WireEncode,
        encode_transport_frame, parse_transport_header,
    };

    #[test]
    fn accumulator_sink_merges() {
        let sched = DistributedScheduler::new(4, true);
        // No sink installed: silently ignored.
        sched.merge_accumulator_deltas(&[(1, vec![1])]);

        let seen: Arc<Mutex<Vec<(usize, Vec<u8>)>>> = Arc::new(Mutex::new(Vec::new()));
        let sink_seen = Arc::clone(&seen);
        sched.set_accumulator_sink(Arc::new(move |deltas| {
            sink_seen.lock().extend_from_slice(deltas);
        }));
        sched.merge_accumulator_deltas(&[(7, vec![42])]);
        sched.merge_accumulator_deltas(&[]);
        assert_eq!(*seen.lock(), vec![(7, vec![42_u8])]);
    }

    #[test]
    fn drain_idle() {
        let sched = DistributedScheduler::new(4, true);
        assert_eq!(sched.total_inflight(), 0);
        assert!(sched.drain(Duration::from_millis(100), Duration::from_millis(5)));
    }

    #[test]
    fn drain_timeout() {
        let sched = DistributedScheduler::new(4, true);
        let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31099);
        sched.inflight.insert(addr, Arc::new(AtomicI16::new(1)));
        assert_eq!(sched.total_inflight(), 1);
        assert!(!sched.drain(Duration::from_millis(50), Duration::from_millis(5)));
    }

    #[test]
    fn register_worker_adds() {
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
    fn scoped_pins_subset() {
        let sched = DistributedScheduler::new(4, true);
        let all: Vec<_> = (0..3)
            .map(|i| SocketAddrV4::new(Ipv4Addr::LOCALHOST, 32000 + i))
            .collect();
        for (i, addr) in all.iter().enumerate() {
            sched.register_worker(*addr, WorkerCapabilities::new(format!("w{i}"), 2, vec![]));
        }

        let view = sched.scoped_to(vec![all[0], all[2]]);

        // The view places only on the two scoped endpoints...
        let mut picked = vec![view.next_executor().unwrap(), view.next_executor().unwrap()];
        picked.sort();
        assert_eq!(picked, vec![all[0], all[2]]);
        // ...while still sharing the full capability registry with the parent.
        assert_eq!(view.worker_capabilities.len(), 3);
        assert_eq!(sched.server_uris.lock().len(), 3);
    }

    #[test]
    fn plan_serve_cached() {
        use atomic_data::distributed::{OpKind, PipelineOp, StepKind, TaskAction, TaskRuntime};
        let sched = DistributedScheduler::new(4, true);
        let ip = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001);
        sched.register_cache_locs(&[(700, 0), (700, 1)], ip);

        let cache_op = PipelineOp {
            op_id: String::new(),
            kind: OpKind::Engine(StepKind::Cache { rdd_id: 700 }),
            runtime: TaskRuntime::Native,
            payload: vec![],
        };
        let map_op = PipelineOp {
            op_id: "m".into(),
            kind: OpKind::Task(TaskAction::Map),
            runtime: TaskRuntime::Native,
            payload: vec![],
        };
        let ops = vec![map_op.clone(), cache_op];

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
        assert_eq!(sched.plan_cache_dispatch(&ops, 3), CacheDispatch::Recompute);
        assert_eq!(
            sched.plan_cache_dispatch(std::slice::from_ref(&map_op), 2),
            CacheDispatch::Recompute
        );
    }

    #[test]
    fn worker_death_clears_cache() {
        let sched = DistributedScheduler::new(4, true);
        let dead = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001);
        let live = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 2), 11002);
        sched.register_cache_locs(&[(800, 0), (800, 1)], dead);
        sched.register_cache_locs(&[(800, 1)], live);

        sched.remove_worker(dead);

        let locs = sched.cache_endpoints.clone();
        let entry = locs.get(&800).unwrap();
        assert!(entry[0].is_empty(), "dead worker's sole copy removed");
        assert_eq!(entry[1], vec![live], "replica on live worker survives");
    }

    #[test]
    fn unpersist_clears_rdd() {
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
    fn cache_locs_grow_sparse() {
        let scheduler = DistributedScheduler::new(4, true);
        let ip = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 7), 11007);
        scheduler.register_cache_locs(&[(500, 2), (500, 0)], ip);
        let locs = scheduler.cache_endpoints.clone();
        let entry = locs.get(&500).expect("rdd 500 registered");
        assert_eq!(entry.len(), 3);
        assert_eq!(entry[2], vec![ip]);
        assert_eq!(entry[0], vec![ip]);
        assert!(entry[1].is_empty());
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
    async fn submit_task_roundtrip() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
                kind: OpKind::Task(TaskAction::Map),
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

    // G1: report-back state affinity tests.

    #[test]
    fn register_state_locs_and_pin_prefers_registered() {
        let sched = DistributedScheduler::new(4, true);
        let w1 = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001);
        let w2 = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 2), 11002);
        sched.register_worker(w1, WorkerCapabilities::new("w1".to_string(), 4, vec![]));
        sched.register_worker(w2, WorkerCapabilities::new("w2".to_string(), 4, vec![]));

        // Before any registration: falls back to modulo (shard 0 → index 0).
        let cold = sched.pin_state_shard(0, None);
        assert!(cold.is_some(), "modulo fallback should pick a worker");

        // Register shard 42 on w2.
        sched.register_state_locs(&[42u64], w2);

        // After registration: shard 42 pinned to w2 regardless of index.
        assert_eq!(sched.pin_state_shard(99, Some(42)), Some(w2));
        // Unregistered shard falls back to modulo.
        assert_ne!(sched.pin_state_shard(0, Some(999)), Some(w2));
    }

    #[test]
    fn invalidate_state_for_worker_clears_its_shards() {
        let sched = DistributedScheduler::new(4, true);
        let w1 = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001);
        let w2 = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 2), 11002);

        sched.register_state_locs(&[1u64, 2, 3], w1);
        sched.register_state_locs(&[4u64], w2);

        // Invalidate w1.
        sched.invalidate_worker_state(w1);

        // w1's shards are gone.
        assert!(!sched.state_locs.contains_key(&1));
        assert!(!sched.state_locs.contains_key(&2));
        assert!(!sched.state_locs.contains_key(&3));
        // w2's shard is untouched.
        assert_eq!(*sched.state_locs.get(&4).unwrap(), w2);
    }

    #[test]
    fn pin_state_shard_falls_back_when_worker_gone() {
        let sched = DistributedScheduler::new(4, true);
        let w1 = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 11001);
        let w2 = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 2), 11002);
        sched.register_worker(w2, WorkerCapabilities::new("w2".to_string(), 4, vec![]));

        // Register shard 7 on w1, but w1 is NOT in server_uris (it's "dead").
        sched.register_state_locs(&[7u64], w1);

        // pin_state_shard: w1 is not live → falls back to modulo (w2 at index 0).
        let picked = sched.pin_state_shard(0, Some(7));
        assert_eq!(
            picked,
            Some(w2),
            "should fall back to live worker via modulo"
        );
    }

    #[test]
    fn agent_step_timeout_unset_by_default() {
        let sched = DistributedScheduler::new(4, true);
        assert_eq!(sched.agent_step_timeout, None);
        // task_timeout (the cheap-CPU-task default) is unaffected.
        assert_eq!(sched.task_timeout, Some(Duration::from_secs(300)));
    }

    #[test]
    fn with_agent_step_timeout_overrides() {
        let sched =
            DistributedScheduler::new(4, true).with_agent_step_timeout(Duration::from_secs(900));
        assert_eq!(sched.agent_step_timeout, Some(Duration::from_secs(900)));
    }

    fn envelope_with_ops(ops: Vec<PipelineOp>) -> TaskEnvelope {
        TaskEnvelope::new(0, 0, 0, 0, 0, "test".to_string(), ops, Vec::new())
    }

    #[test]
    fn effective_timeout_uses_agent_step_default_when_unset() {
        let sched = DistributedScheduler::new(4, true);
        let task = envelope_with_ops(vec![PipelineOp {
            op_id: String::new(),
            kind: OpKind::Engine(StepKind::AgentStep),
            runtime: TaskRuntime::Native,
            payload: vec![],
        }]);
        assert_eq!(
            sched.effective_timeout(&task),
            Some(super::AGENT_STEP_DEFAULT_TIMEOUT)
        );
    }

    #[test]
    fn effective_timeout_uses_configured_agent_step_timeout() {
        let sched =
            DistributedScheduler::new(4, true).with_agent_step_timeout(Duration::from_secs(60));
        let task = envelope_with_ops(vec![PipelineOp {
            op_id: String::new(),
            kind: OpKind::Engine(StepKind::AgentStep),
            runtime: TaskRuntime::Native,
            payload: vec![],
        }]);
        assert_eq!(
            sched.effective_timeout(&task),
            Some(Duration::from_secs(60))
        );
    }

    #[test]
    fn effective_timeout_falls_back_to_task_timeout_for_non_agent_ops() {
        let sched = DistributedScheduler::new(4, true);
        let task = envelope_with_ops(vec![PipelineOp {
            op_id: String::new(),
            kind: OpKind::Task(TaskAction::Map),
            runtime: TaskRuntime::Native,
            payload: vec![],
        }]);
        assert_eq!(sched.effective_timeout(&task), sched.task_timeout);
    }
}
