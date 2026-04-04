use std::{
    collections::{BTreeSet, HashSet, VecDeque},
    fmt::Debug,
    net::{Ipv4Addr, SocketAddrV4},
    path::Path,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    time::Duration,
};

use atomic_data::{
    data::Data,
    dependency::ShuffleDependencyBox,
    distributed::{
        ArtifactDescriptor, ArtifactManifest, TaskEnvelope, TaskResultEnvelope,
        TransportFrameKind, TRANSPORT_HEADER_LEN, WasmTaskPayload, WasmValueEncoding,
        WorkerCapabilities, WireDecode, WireEncode, encode_transport_frame,
        parse_transport_header,
    },
    env,
    partial::{ApproximateEvaluator, result::PartialResult},
    rdd::{Rdd, RddBase},
    task::TaskOption,
    task_registry::TaskRegistry,
    task_context::TaskContext,
};
use dashmap::DashMap;
use futures::future::try_join_all;
use parking_lot::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    base::{MutatorsAndGetter, NativeScheduler},
    error::{LibResult, SchedulerError},
    job::Job,
    listener::LiveListenerBus,
    stage::Stage,
};

#[derive(Clone, Default)]
pub struct DistributedScheduler {
    mutators: MutatorsAndGetter,
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,
    next_job_id: Arc<AtomicUsize>,
    next_run_id: Arc<AtomicUsize>,
    next_task_id: Arc<AtomicUsize>,
    next_stage_id: Arc<AtomicUsize>,
    shuffle_to_map_stage: Arc<DashMap<usize, Stage>>,
    cache_locs: Arc<DashMap<usize, Vec<Vec<Ipv4Addr>>>>,
    worker_capabilities: Arc<DashMap<SocketAddrV4, WorkerCapabilities>>,
    task_registry: Arc<Mutex<TaskRegistry>>,
    master: bool,
    framework_name: String,
    is_registered: bool,
    active_jobs: HashMap<usize, Job>,
    active_job_queue: Vec<Job>,
    taskid_to_jobid: HashMap<String, usize>,
    taskid_to_slaveid: HashMap<String, String>,
    job_tasks: HashMap<usize, HashSet<String>>,
    slaves_with_executors: HashSet<String>,
    server_uris: Arc<Mutex<VecDeque<SocketAddrV4>>>,
    port: u16,
    scheduler_lock: Arc<Mutex<bool>>,
    live_listener_bus: LiveListenerBus,
}

impl DistributedScheduler {
    pub fn new(max_failures: usize, master: bool) -> Self {
        let mut live_listener_bus = LiveListenerBus::new();
        live_listener_bus.start().unwrap();
        let mutators = MutatorsAndGetter::new();
        Self {
            mutators,
            max_failures,
            attempt_id: Arc::new(AtomicUsize::new(0)),
            next_job_id: Arc::new(AtomicUsize::new(0)),
            next_run_id: Arc::new(AtomicUsize::new(0)),
            next_task_id: Arc::new(AtomicUsize::new(0)),
            next_stage_id: Arc::new(AtomicUsize::new(0)),
            shuffle_to_map_stage: Arc::new(DashMap::new()),
            cache_locs: Arc::new(DashMap::new()),
            worker_capabilities: Arc::new(DashMap::new()),
            task_registry: Arc::new(Mutex::new(TaskRegistry::default())),
            master,
            framework_name: "atomic".to_string(),
            is_registered: true,
            active_jobs: HashMap::new(),
            active_job_queue: Vec::new(),
            taskid_to_jobid: HashMap::new(),
            taskid_to_slaveid: HashMap::new(),
            job_tasks: HashMap::new(),
            slaves_with_executors: HashSet::new(),
            server_uris: Arc::new(Mutex::new(VecDeque::new())),
            port: 0,
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
    ) -> LibResult<TaskResultEnvelope> {
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
        let target_executor = self.select_executor_for_artifact(&task.artifact)?;
        self.submit_task_envelope_to_worker(&task, target_executor).await
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
        let run_id = self.get_mutators().get_next_job_id();
        let stage_id = self.get_mutators().get_next_stage_id();
        let payload = payload
            .encode_wire()
            .map_err(|err| SchedulerError::Transport(err.to_string()))?;

        let submits = partitions.into_iter().enumerate().map(|(partition_id, partition_data)| {
            let task_id = self.get_mutators().get_next_task_id();
            let attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
            self.submit_registered_task(
                run_id,
                stage_id,
                task_id,
                attempt_id,
                partition_id,
                format!("wasm-{}-{}", operation_id, partition_id),
                operation_id,
                payload.clone(),
                partition_data,
            )
        });

        let responses = try_join_all(submits).await?;
        Ok(responses.into_iter().map(|response| response.result_data).collect())
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

        self.event_queues.insert(jt.run_id, VecDeque::new());

        let mut results: Vec<Option<U>> = (0..jt.num_output_parts).map(|_| None).collect();
        let mut fetch_failure_duration = Duration::new(0, 0);

        self.submit_stage(jt.final_stage.clone(), jt.clone()).await?;

        let mut num_finished = 0;
        while num_finished != jt.num_output_parts {
            let event_option = self.wait_for_event(jt.run_id, self.poll_timeout);
            let start = Instant::now();

            if let Some(evt) = event_option {
                let stage = self
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

        if !jt.failed.lock().await.is_empty()
            && fetch_failure_duration.as_millis() > self.resubmit_timeout
        {
            self.update_cache_locs().await?;
            for stage in jt.failed.lock().await.iter() {
                self.submit_stage(stage.clone(), jt.clone()).await?;
            }
            jt.failed.lock().await.clear();
        }

        self.event_queues.remove(&jt.run_id);
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
            artifact.execution_backend, artifact.artifact_kind, artifact.runtime
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

    async fn read_transport_frame(stream: &mut TcpStream) -> LibResult<(TransportFrameKind, Vec<u8>)> {
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
        self.cache_locs.clear();
        env::Env::get()
            .cache_tracker
            .get_location_snapshot()
            .await
            .map_err(|_| SchedulerError::Other)?
            .into_iter()
            .for_each(|(key, value)| {
                self.cache_locs.insert(key, value);
            });
        Ok(())
    }

    async fn get_shuffle_map_stage(&self, shuf: Arc<ShuffleDependencyBox>) -> LibResult<Stage> {
        let stage = self.shuffle_to_map_stage.get(&shuf.get_shuffle_id());
        match stage {
            Some(stage) => Ok(stage.clone()),
            None => {
                let stage = self
                    .new_stage(shuf.get_rdd_base(), Some(shuf.clone()))
                    .await?;
                self.shuffle_to_map_stage
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

    fn get_mutators(&self) -> MutatorsAndGetter {
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
        TaskResultEnvelope, WasmArtifactManifestEntry, WireDecode, WireEncode,
        encode_transport_frame,
    };

    fn artifact_descriptor(
        backend: ExecutionBackend,
        kind: ArtifactKind,
    ) -> ArtifactDescriptor {
        ArtifactDescriptor {
            operation_id: "op.test".to_string(),
            execution_backend: backend,
            artifact_kind: kind,
            artifact_ref: "registry/test@sha256:abc".to_string(),
            entrypoint: "run".to_string(),
            runtime: RuntimeKind::Rust,
            artifact_digest: Some("sha256:abc".to_string()),
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
                schema_version: 1,
                worker_id: "docker-1".to_string(),
                execution_backend: ExecutionBackend::Docker,
                supported_artifacts: vec![ArtifactKind::Docker],
                supported_runtimes: vec![RuntimeKind::Rust],
                max_concurrent_tasks: 2,
            },
        );
        scheduler.register_worker(
            wasm_addr,
            WorkerCapabilities {
                schema_version: 1,
                worker_id: "wasm-1".to_string(),
                execution_backend: ExecutionBackend::Wasm,
                supported_artifacts: vec![ArtifactKind::Wasm],
                supported_runtimes: vec![RuntimeKind::Rust],
                max_concurrent_tasks: 2,
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
            WasmArtifactManifestEntry::new(
                artifact_descriptor(ExecutionBackend::Wasm, ArtifactKind::Wasm),
                Some("target/wasm/map_task.wasm".to_string()),
            ),
        ]));

        let task = scheduler
            .build_registered_task_envelope(
                1,
                2,
                3,
                0,
                4,
                "trace-1",
                "op.test",
                vec![9],
                vec![8],
            )
            .expect("task envelope should resolve from registry");

        assert_eq!(task.artifact.execution_backend, ExecutionBackend::Wasm);
        assert_eq!(task.artifact.artifact_ref, "target/wasm/map_task.wasm");
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
            assert_eq!(task.artifact.execution_backend, ExecutionBackend::Docker);

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
        assert_eq!(result.result_data, vec![1, 2, 3]);
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
                schema_version: 1,
                worker_id: "wasm-1".to_string(),
                execution_backend: ExecutionBackend::Wasm,
                supported_artifacts: vec![ArtifactKind::Wasm],
                supported_runtimes: vec![RuntimeKind::Rust],
                max_concurrent_tasks: 2,
            },
        );
        scheduler.register_artifact_manifest(ArtifactManifest::new(vec![
            WasmArtifactManifestEntry::new(
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
                    task.partition_data,
                );
                let payload = response.encode_wire().expect("serialize response");
                let frame = encode_transport_frame(TransportFrameKind::TaskResultEnvelope, &payload);
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
