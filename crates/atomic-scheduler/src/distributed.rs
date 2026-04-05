use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    fmt::Debug,
    net::{Ipv4Addr, SocketAddrV4},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use atomic_data::{
    data::Data,
    dependency::ShuffleDependencyBox,
    distributed::{
        ArtifactDescriptor, ArtifactManifest, TRANSPORT_HEADER_LEN, TaskEnvelope,
        TaskResultEnvelope, TransportFrameKind, WasmTaskPayload, WasmValueEncoding, WireDecode,
        WireEncode, WorkerCapabilities, encode_transport_frame, parse_transport_header,
    },
    partial::{ApproximateEvaluator, result::PartialResult},
    rdd::{Rdd, RddBase},
    task::TaskOption,
    task_context::TaskContext,
    task_registry::TaskRegistry,
};
use dashmap::DashMap;
use futures::future::try_join_all;
use parking_lot::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    base::{Mutators, NativeScheduler},
    dag::{CompletionEvent, TastEndReason},
    error::{LibResult, SchedulerError},
    job::{Job, JobTracker},
    listener::{JobListener, LiveListenerBus},
    stage::Stage,
};

#[derive(Clone, Default)]
// #[allow(dead_code)]
pub struct DistributedScheduler {
    /// Shared mutable state: stage cache, event queues, map-output tracker, and ID counters.
    /// Cloned cheaply because every inner value is `Arc`-wrapped.
    mutators: Mutators,

    /// Maximum number of times a single task partition is retried before the job fails.
    /// Consulted inside `submit_registered_task` to bound the retry loop per task.
    max_failures: usize,

    /// Monotonically increasing attempt counter across all tasks in this scheduler instance.
    /// Each call to `submit_registered_task` increments this and embeds the value in `TaskEnvelope`.
    attempt_id: Arc<AtomicUsize>,

    /// Per-worker capability declarations used to match an artifact's backend/kind to a worker.
    /// Consulted by `select_executor_for_artifact` before dispatching a `TaskEnvelope`.
    worker_capabilities: Arc<DashMap<SocketAddrV4, WorkerCapabilities>>,

    /// Registry of `ArtifactDescriptor` entries keyed by `operation_id`.
    /// Loaded from a TOML manifest or registered individually before jobs are submitted.
    task_registry: Arc<Mutex<TaskRegistry>>,

    /// Whether this process is the job driver (`true`) or a passthrough coordinator (`false`).
    /// Only the driver creates jobs, allocates IDs, and routes results back to the caller.
    master: bool,

    /// Tracks every in-flight job by `run_id → Job`.
    /// Populated at the start of `run_registered_wasm_job_with_payload`; removed on completion.
    active_jobs: Arc<DashMap<usize, Job>>,

    /// FIFO queue of `Job` records ordered by submission time.
    /// Used for ordered inspection and for pruning completed jobs by `run_id`.
    active_job_queue: Arc<Mutex<VecDeque<Job>>>,

    /// Routes a task's composite key (`"run_id:task_id"`) to its parent job's `run_id`.
    /// Populated before dispatch so completion-event handlers can locate the owning job.
    taskid_to_jobid: Arc<DashMap<String, usize>>,

    /// Records which worker endpoint handled each task (`"run_id:task_id"` → worker address).
    /// Populated after `submit_task_envelope_to_worker` succeeds; used for failure attribution.
    taskid_to_slaveid: Arc<DashMap<String, String>>,

    /// Full set of task keys (`"run_id:task_id"`) belonging to each job (`run_id → keys`).
    /// Enables bulk cleanup and job-level cancellation without scanning all task maps.
    job_tasks: Arc<DashMap<usize, HashSet<String>>>,

    /// Registered worker endpoints, round-robined for task dispatch.
    /// Populated via `register_worker`; rotated by `next_executor_server` and
    /// `select_executor_for_artifact`.
    server_uris: Arc<Mutex<VecDeque<SocketAddrV4>>>,

    /// Mutex used to serialise concurrent job creation in `run_registered_wasm_job_with_payload`.
    /// Prevents two callers from racing on the same `run_id` / `stage_id` allocation window.
    scheduler_lock: Arc<Mutex<bool>>,

    /// Event listener bus for broadcasting job-lifecycle events to registered listeners.
    /// Started in `new()` and stopped in `Drop`.
    live_listener_bus: LiveListenerBus,
}

impl DistributedScheduler {
    pub fn new(max_failures: usize, master: bool) -> Self {
        let mut live_listener_bus = LiveListenerBus::new();
        live_listener_bus.start().unwrap();
        Self {
            mutators: Mutators::new(),
            max_failures,
            attempt_id: Arc::new(AtomicUsize::new(0)),
            worker_capabilities: Arc::new(DashMap::new()),
            task_registry: Arc::new(Mutex::new(TaskRegistry::default())),
            master,
            active_jobs: Arc::new(DashMap::new()),
            active_job_queue: Arc::new(Mutex::new(VecDeque::new())),
            taskid_to_jobid: Arc::new(DashMap::new()),
            taskid_to_slaveid: Arc::new(DashMap::new()),
            job_tasks: Arc::new(DashMap::new()),
            server_uris: Arc::new(Mutex::new(VecDeque::new())),
            scheduler_lock: Arc::new(Mutex::new(false)),
            live_listener_bus,
        }
    }

    pub fn register_worker(&self, endpoint: SocketAddrV4, capabilities: WorkerCapabilities) {
        self.worker_capabilities.insert(endpoint, capabilities);
        let mut servers = self.server_uris.lock();
        if !servers.contains(&endpoint) {
            servers.push_back(endpoint);
        }
    }

    pub fn register_artifact_descriptor(
        &self,
        descriptor: ArtifactDescriptor,
    ) -> Option<ArtifactDescriptor> {
        self.task_registry.lock().register(descriptor)
    }

    pub fn register_artifact_manifest(&self, manifest: ArtifactManifest) {
        self.task_registry.lock().register_manifest(manifest);
    }

    pub fn load_artifact_manifest_toml(&self, path: impl AsRef<Path>) -> LibResult<()> {
        let registry = TaskRegistry::load_toml_file(path)
            .map_err(|err| SchedulerError::ArtifactResolution(err.to_string()))?;
        *self.task_registry.lock() = registry;
        Ok(())
    }

    pub fn resolve_artifact(&self, operation_id: &str) -> LibResult<ArtifactDescriptor> {
        self.task_registry
            .lock()
            .resolve(operation_id)
            .ok_or_else(|| SchedulerError::ArtifactResolution(operation_id.to_string()))
    }

    pub fn build_registered_task_envelope(
        &self,
        run_id: usize,
        stage_id: usize,
        task_id: usize,
        attempt_id: usize,
        partition_id: usize,
        trace_id: impl Into<String>,
        operation_id: &str,
        payload: Vec<u8>,
        partition_data: Vec<u8>,
    ) -> LibResult<TaskEnvelope> {
        let artifact = self.resolve_artifact(operation_id)?;
        Ok(TaskEnvelope::new(
            run_id,
            stage_id,
            task_id,
            attempt_id,
            partition_id,
            trace_id.into(),
            artifact,
            payload,
            partition_data,
        ))
    }

    /// Submit a single registered task to a compatible worker, retrying up to `max_failures`
    /// times on transport errors before propagating the failure.
    ///
    /// Returns the result envelope together with the address of the worker that succeeded,
    /// so callers can record the assignment in `taskid_to_slaveid`.
    pub async fn submit_registered_task(
        &self,
        run_id: usize,
        stage_id: usize,
        task_id: usize,
        attempt_id: usize,
        partition_id: usize,
        trace_id: impl Into<String>,
        operation_id: &str,
        payload: Vec<u8>,
        partition_data: Vec<u8>,
    ) -> LibResult<(TaskResultEnvelope, SocketAddrV4)> {
        let task = self.build_registered_task_envelope(
            run_id,
            stage_id,
            task_id,
            attempt_id,
            partition_id,
            trace_id,
            operation_id,
            payload,
            partition_data,
        )?;
        let mut last_err = None;
        for attempt in 0..=self.max_failures {
            // Re-select the executor on each attempt so a different worker is tried after failure.
            let target = self.select_executor_for_artifact(&task.artifact)?;
            match self.submit_task_envelope_to_worker(&task, target).await {
                Ok(result) => return Ok((result, target)),
                Err(e) => {
                    log::warn!(
                        "task {}/{} attempt {}/{} failed: {}",
                        run_id,
                        task_id,
                        attempt + 1,
                        self.max_failures + 1,
                        e
                    );
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap())
    }

    pub async fn run_registered_wasm_job(
        &self,
        operation_id: &str,
        partitions: Vec<Vec<u8>>,
    ) -> LibResult<Vec<Vec<u8>>> {
        self.run_registered_wasm_job_with_payload(
            operation_id,
            WasmTaskPayload::raw_bytes(),
            partitions,
        )
        .await
    }

    pub async fn run_registered_wasm_job_with_payload(
        &self,
        operation_id: &str,
        payload: WasmTaskPayload,
        partitions: Vec<Vec<u8>>,
    ) -> LibResult<Vec<Vec<u8>>> {
        // Serialise job creation to prevent concurrent callers from racing on the same
        // run_id / stage_id allocation window.
        let (run_id, stage_id) = {
            let _lock = self.scheduler_lock.lock();
            let run_id = self.get_mutators().get_next_job_id();
            let stage_id = self.get_mutators().get_next_stage_id();
            let job = Job::new(run_id, run_id);
            self.active_jobs.insert(run_id, job.clone());
            self.active_job_queue.lock().push_back(job);
            (run_id, stage_id)
        };

        let payload_bytes = payload
            .encode_wire()
            .map_err(|err| SchedulerError::Transport(err.to_string()))?;

        let submits = partitions
            .into_iter()
            .enumerate()
            .map(|(partition_id, partition_data)| {
                let task_id = self.get_mutators().get_next_task_id();
                let attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
                let task_key = format!("{}:{}", run_id, task_id);
                let payload = payload_bytes.clone();

                // Register the task → job mapping before dispatch so completion handlers can
                // route events and diagnostics back to the correct job.
                self.taskid_to_jobid.insert(task_key.clone(), run_id);
                self.job_tasks
                    .entry(run_id)
                    .or_default()
                    .insert(task_key.clone());

                async move {
                    let trace_id = format!("wasm-{}-{}", operation_id, partition_id);
                    let (result, worker_addr) = self
                        .submit_registered_task(
                            run_id,
                            stage_id,
                            task_id,
                            attempt_id,
                            partition_id,
                            trace_id,
                            operation_id,
                            payload,
                            partition_data,
                        )
                        .await?;

                    // Record which worker handled this task for failure attribution.
                    self.taskid_to_slaveid
                        .insert(task_key, worker_addr.to_string());

                    Ok::<_, SchedulerError>(result)
                }
            });

        let result = try_join_all(submits).await;

        // Clean up all tracking state for this job regardless of success or failure.
        self.active_jobs.remove(&run_id);
        self.active_job_queue
            .lock()
            .retain(|j| j.run_id() != run_id);
        if let Some((_, task_keys)) = self.job_tasks.remove(&run_id) {
            for key in &task_keys {
                self.taskid_to_slaveid.remove(key);
                self.taskid_to_jobid.remove(key);
            }
        }

        result.map(|responses| responses.into_iter().map(|r| r.data).collect())
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
            "legacy distributed TaskOption execution has been removed; use registered artifact jobs",
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
            "legacy distributed TaskOption execution has been removed; use registered artifact jobs",
        ))
    }

    async fn event_process_loop<T: Data, U: Data + Clone, F, L>(
        self: Arc<Self>,
        allow_local: bool,
        jt: Arc<JobTracker<F, U, T, L>>,
    ) -> LibResult<Vec<U>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
        L: JobListener,
    {
        if allow_local {
            if let Some(result) = DistributedScheduler::local_execution(jt.clone())? {
                return Ok(result);
            }
        }

        self.mutators
            .event_queues
            .insert(jt.run_id, VecDeque::new());

        let mut results: Vec<Option<U>> = (0..jt.num_output_parts).map(|_| None).collect();
        let mut fetch_failure_duration = Duration::new(0, 0);

        self.submit_stage(jt.final_stage.clone(), jt.clone())
            .await?;

        let mut num_finished = 0;
        while num_finished != jt.num_output_parts {
            let event_option = self.wait_for_event(jt.run_id, 100);
            let start = Instant::now();

            if let Some(evt) = event_option {
                let stage = self
                    .mutators
                    .stage_cache
                    .get(&evt.task.get_stage_id())
                    .unwrap()
                    .clone();
                jt.pending_tasks
                    .lock()
                    .await
                    .get_mut(&stage)
                    .unwrap()
                    .remove(&evt.task);
                use super::dag::TastEndReason::*;
                match evt.reason {
                    Success => {
                        self.on_event_success(evt, &mut results, &mut num_finished, jt.clone())
                            .await?;
                    }
                    FetchFailed(failed_vals) => {
                        self.on_event_failure(jt.clone(), failed_vals, evt.task.get_stage_id())
                            .await;
                        fetch_failure_duration = start.elapsed();
                    }
                    Error(error) => panic!("{}", error),
                    OtherFailure(msg) => panic!("{}", msg),
                }
            }
        }

        if !jt.failed.lock().await.is_empty() && fetch_failure_duration.as_millis() > 3000 {
            self.update_cache_locs().await?;
            for stage in jt.failed.lock().await.iter() {
                self.submit_stage(stage.clone(), jt.clone()).await?;
            }
            jt.failed.lock().await.clear();
        }

        self.mutators.event_queues.remove(&jt.run_id);
        Ok(results
            .into_iter()
            .map(|value| value.expect("some results still missing"))
            .collect())
    }

    pub fn select_executor_for_artifact(
        &self,
        artifact: &ArtifactDescriptor,
    ) -> LibResult<SocketAddrV4> {
        let mut servers = self.server_uris.lock();
        if servers.is_empty() {
            return Err(SchedulerError::NoCompatibleWorker(
                "no registered worker endpoints".to_string(),
            ));
        }

        let attempts = servers.len();
        for _ in 0..attempts {
            let endpoint = servers.pop_front().ok_or_else(|| {
                SchedulerError::NoCompatibleWorker("no worker endpoints".to_string())
            })?;
            servers.push_back(endpoint);

            if self
                .worker_capabilities
                .get(&endpoint)
                .map(|caps| caps.supports(artifact))
                .unwrap_or(false)
            {
                return Ok(endpoint);
            }
        }

        Err(SchedulerError::NoCompatibleWorker(format!(
            "backend {:?}, artifact {:?}, runtime {:?}",
            artifact.backend, artifact.kind, artifact.runtime
        )))
    }

    pub async fn submit_task_envelope_to_worker(
        &self,
        task: &TaskEnvelope,
        target_executor: SocketAddrV4,
    ) -> LibResult<TaskResultEnvelope> {
        let mut stream = TcpStream::connect(target_executor)
            .await
            .map_err(|err| SchedulerError::Transport(err.to_string()))?;
        let payload = task
            .encode_wire()
            .map_err(|err| SchedulerError::Transport(err.to_string()))?;
        Self::write_transport_frame(&mut stream, TransportFrameKind::TaskEnvelope, &payload)
            .await?;
        let (kind, payload) = Self::read_transport_frame(&mut stream).await?;
        if kind != TransportFrameKind::TaskResultEnvelope {
            return Err(SchedulerError::Transport(format!(
                "unexpected response frame kind: {:?}",
                kind
            )));
        }
        TaskResultEnvelope::decode_wire(&payload)
            .map_err(|err| SchedulerError::Transport(err.to_string()))
    }

    async fn write_transport_frame(
        stream: &mut TcpStream,
        frame_kind: TransportFrameKind,
        payload: &[u8],
    ) -> LibResult<()> {
        let frame = encode_transport_frame(frame_kind, payload);
        stream
            .write_all(&frame)
            .await
            .map_err(|err| SchedulerError::Transport(err.to_string()))
    }

    async fn read_transport_frame(
        stream: &mut TcpStream,
    ) -> LibResult<(TransportFrameKind, Vec<u8>)> {
        let mut header = [0_u8; TRANSPORT_HEADER_LEN];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|err| SchedulerError::Transport(err.to_string()))?;
        let (kind, payload_len) = parse_transport_header(&header)
            .map_err(|err| SchedulerError::Transport(err.to_string()))?;
        let mut payload = vec![0_u8; payload_len];
        stream
            .read_exact(&mut payload)
            .await
            .map_err(|err| SchedulerError::Transport(err.to_string()))?;
        Ok((kind, payload))
    }

    fn handle_completion_event(
        event_queues: Arc<DashMap<usize, VecDeque<CompletionEvent>>>,
        task: TaskOption,
        reason: TastEndReason,
        result: Box<dyn Data>,
    ) {
        let run_id = task.get_run_id();
        if let Some(mut queue) = event_queues.get_mut(&run_id) {
            queue.push_back(CompletionEvent {
                task,
                reason,
                result: Some(result),
                accum_updates: HashMap::new(),
            });
        } else {
            log::debug!("ignoring completion event for distributed job");
        }
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
        log::debug!(
            "ignoring legacy distributed submit_task call; registered artifact jobs are required"
        );
    }

    fn next_executor_server(&self, task: &TaskOption) -> SocketAddrV4 {
        if !task.is_pinned() {
            let socket_addr = self.server_uris.lock().pop_back().unwrap();
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
                let target_host = servers.remove(pos).unwrap();
                servers.push_front(target_host);
                target_host
            } else {
                unreachable!()
            }
        }
    }

    async fn update_cache_locs(&self) -> LibResult<()> {
        // Cache tracker integration is not yet wired — clear local locs only.
        self.mutators.cache_locs.clear();
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
        self.visit_for_missing_parent_stages(&mut missing, &mut visited, stage.get_rdd())
            .await?;
        Ok(missing.into_iter().collect())
    }

    fn get_mutators(&self) -> Mutators {
        self.mutators.clone()
    }
}

impl Drop for DistributedScheduler {
    fn drop(&mut self) {
        self.live_listener_bus.stop().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomic_data::distributed::{
        ArtifactKind, ArtifactManifest, ExecutionBackend, ResourceProfile, RuntimeKind,
        TaskResultEnvelope, WasmManifestEntry, WireDecode, WireEncode,
        encode_transport_frame,
    };

    fn artifact_descriptor(backend: ExecutionBackend, kind: ArtifactKind) -> ArtifactDescriptor {
        ArtifactDescriptor {
            op_id: "op.test".to_string(),
            backend,
            kind,
            uri: "registry/test@sha256:abc".to_string(),
            entrypoint: "run".to_string(),
            runtime: RuntimeKind::Rust,
            digest: Some("sha256:abc".to_string()),
            build_target: Some("wasm32-wasip2".to_string()),
            profile: ResourceProfile {
                cpu_millis: 250,
                memory_mb: 128,
                timeout_ms: 1_000,
            },
        }
    }

    #[test]
    fn selects_worker_with_matching_capabilities() {
        let scheduler = DistributedScheduler::new(4, true);
        let docker_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31001);
        let wasm_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31002);

        scheduler.register_worker(
            docker_addr,
            WorkerCapabilities {
                version: 1,
                worker_id: "docker-1".to_string(),
                backend: ExecutionBackend::Docker,
                artifacts: vec![ArtifactKind::Docker],
                runtimes: vec![RuntimeKind::Rust],
                max_tasks: 2,
            },
        );
        scheduler.register_worker(
            wasm_addr,
            WorkerCapabilities {
                version: 1,
                worker_id: "wasm-1".to_string(),
                backend: ExecutionBackend::Wasm,
                artifacts: vec![ArtifactKind::Wasm],
                runtimes: vec![RuntimeKind::Rust],
                max_tasks: 2,
            },
        );

        let selected = scheduler
            .select_executor_for_artifact(&artifact_descriptor(
                ExecutionBackend::Wasm,
                ArtifactKind::Wasm,
            ))
            .expect("should select wasm worker");

        assert_eq!(selected, wasm_addr);
    }

    #[test]
    fn registered_task_envelope_uses_manifest_module_path() {
        let scheduler = DistributedScheduler::new(4, true);
        scheduler.register_artifact_manifest(ArtifactManifest::new(vec![
            WasmManifestEntry::new(
                artifact_descriptor(ExecutionBackend::Wasm, ArtifactKind::Wasm),
                Some("target/wasm/map_task.wasm".to_string()),
            ),
        ]));

        let task = scheduler
            .build_registered_task_envelope(1, 2, 3, 0, 4, "trace-1", "op.test", vec![9], vec![8])
            .expect("task envelope should resolve from registry");

        assert_eq!(task.artifact.backend, ExecutionBackend::Wasm);
        assert_eq!(task.artifact.uri, "target/wasm/map_task.wasm");
    }

    #[tokio::test]
    async fn submits_task_envelope_and_reads_result_frame() {
        let scheduler = DistributedScheduler::new(4, true);
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind listener");
        let endpoint = listener.local_addr().expect("local addr");
        let endpoint = SocketAddrV4::new(Ipv4Addr::LOCALHOST, endpoint.port());

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut header = [0_u8; TRANSPORT_HEADER_LEN];
            socket.read_exact(&mut header).await.expect("read header");
            let (kind, payload_len) = parse_transport_header(&header).expect("parse header");
            assert_eq!(kind, TransportFrameKind::TaskEnvelope);

            let mut payload = vec![0_u8; payload_len];
            socket.read_exact(&mut payload).await.expect("read payload");
            let task = TaskEnvelope::decode_wire(&payload).expect("deserialize task");
            assert_eq!(task.artifact.backend, ExecutionBackend::Docker);

            let response = TaskResultEnvelope::ok(
                task.run_id,
                task.stage_id,
                task.task_id,
                task.attempt_id,
                "worker-1".to_string(),
                vec![1, 2, 3],
            );
            let payload = response.encode_wire().expect("serialize response");
            let frame = encode_transport_frame(TransportFrameKind::TaskResultEnvelope, &payload);
            socket.write_all(&frame).await.expect("write response");
        });

        let task = TaskEnvelope::new(
            1,
            2,
            3,
            0,
            4,
            "trace-1".to_string(),
            artifact_descriptor(ExecutionBackend::Docker, ArtifactKind::Docker),
            vec![9, 9],
            vec![8, 8],
        );
        let result = scheduler
            .submit_task_envelope_to_worker(&task, endpoint)
            .await
            .expect("submit task envelope");

        assert_eq!(result.worker_id, "worker-1");
        assert_eq!(result.data, vec![1, 2, 3]);
        server.await.expect("server join");
    }

    #[tokio::test]
    async fn registered_wasm_job_returns_partition_results() {
        let scheduler = DistributedScheduler::new(4, true);
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind listener");
        let endpoint = listener.local_addr().expect("local addr");
        let endpoint = SocketAddrV4::new(Ipv4Addr::LOCALHOST, endpoint.port());

        scheduler.register_worker(
            endpoint,
            WorkerCapabilities {
                version: 1,
                worker_id: "wasm-1".to_string(),
                backend: ExecutionBackend::Wasm,
                artifacts: vec![ArtifactKind::Wasm],
                runtimes: vec![RuntimeKind::Rust],
                max_tasks: 2,
            },
        );
        scheduler.register_artifact_manifest(ArtifactManifest::new(vec![
            WasmManifestEntry::new(
                artifact_descriptor(ExecutionBackend::Wasm, ArtifactKind::Wasm),
                Some("target/wasm/map_task.wasm".to_string()),
            ),
        ]));

        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut header = [0_u8; TRANSPORT_HEADER_LEN];
                socket.read_exact(&mut header).await.expect("read header");
                let (kind, payload_len) = parse_transport_header(&header).expect("parse header");
                assert_eq!(kind, TransportFrameKind::TaskEnvelope);
                let mut payload = vec![0_u8; payload_len];
                socket.read_exact(&mut payload).await.expect("read payload");
                let task = TaskEnvelope::decode_wire(&payload).expect("deserialize task");

                let response = TaskResultEnvelope::ok(
                    task.run_id,
                    task.stage_id,
                    task.task_id,
                    task.attempt_id,
                    "worker-1".to_string(),
                    task.data,
                );
                let payload = response.encode_wire().expect("serialize response");
                let frame =
                    encode_transport_frame(TransportFrameKind::TaskResultEnvelope, &payload);
                socket.write_all(&frame).await.expect("write response");
            }
        });

        let result = scheduler
            .run_registered_wasm_job("op.test", vec![vec![1, 2], vec![3, 4]])
            .await
            .expect("registered wasm job should succeed");

        assert_eq!(result, vec![vec![1, 2], vec![3, 4]]);
        server.await.expect("server join");
    }
}
