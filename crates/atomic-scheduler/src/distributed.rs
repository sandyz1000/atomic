use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    fmt::Debug,
    net::{Ipv4Addr, SocketAddrV4},
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
        PipelineOp, TRANSPORT_HEADER_LEN, TaskEnvelope, TaskResultEnvelope, TransportFrameKind,
        WireDecode, WireEncode, WorkerCapabilities, encode_transport_frame, parse_transport_header,
    },
    partial::{ApproximateEvaluator, result::PartialResult},
    rdd::{Rdd, RddBase},
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

use crate::{
    base::{Mutators, NativeScheduler},
    dag::{CompletionEvent, TastEndReason},
    error::{LibResult, SchedulerError},
    job::{Job, JobTracker},
    listener::{JobListener, LiveListenerBus},
    stage::Stage,
};

#[derive(Clone, Default)]
pub struct DistributedScheduler {
    mutators: Mutators,
    max_failures: usize,
    attempt_id: Arc<AtomicUsize>,

    /// Per-worker capability declarations — keyed by endpoint.
    worker_capabilities: Arc<DashMap<SocketAddrV4, WorkerCapabilities>>,

    master: bool,
    active_jobs: Arc<DashMap<usize, Job>>,
    active_job_queue: Arc<Mutex<VecDeque<Job>>>,
    taskid_to_jobid: Arc<DashMap<String, usize>>,
    taskid_to_slaveid: Arc<DashMap<String, String>>,
    job_tasks: Arc<DashMap<usize, HashSet<String>>>,

    /// Registered worker endpoints, round-robined for task dispatch.
    server_uris: Arc<Mutex<VecDeque<SocketAddrV4>>>,

    scheduler_lock: Arc<Mutex<bool>>,
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

    /// Round-robin pick of the next available worker endpoint.
    pub fn next_executor(&self) -> LibResult<SocketAddrV4> {
        let mut servers = self.server_uris.lock();
        let endpoint = servers.pop_front().ok_or_else(|| {
            SchedulerError::NoCompatibleWorker("no registered workers".to_string())
        })?;
        servers.push_back(endpoint);
        Ok(endpoint)
    }

    /// Capacity-aware worker selection: skip workers with `max_tasks == 0`,
    /// prefer those with higher declared capacity.
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
            if max_tasks > 0 {
                return Ok(endpoint);
            }
        }
        Err(SchedulerError::NoCompatibleWorker(
            "all workers are at capacity (max_tasks == 0)".to_string(),
        ))
    }

    /// Submit a single `TaskEnvelope` to a worker, retrying up to `max_failures` times.
    pub async fn submit_native_task(
        &self,
        task: &TaskEnvelope,
    ) -> LibResult<(TaskResultEnvelope, SocketAddrV4)> {
        let mut last_err = None;
        for attempt in 0..=self.max_failures {
            let target = self.next_executor_with_capacity()?;
            match self.submit_task_to_worker(task, target).await {
                Ok(result) => return Ok((result, target)),
                Err(e) => {
                    log::warn!(
                        "task {}/{} attempt {}/{} failed: {}",
                        task.run_id,
                        task.task_id,
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

    /// Run a native (non-artifact) job over a set of pre-encoded partitions.
    ///
    /// Sends one `TaskEnvelope` per partition, each carrying the full `ops` pipeline.
    /// Workers execute ops in order, threading data through each step.
    /// Returns raw result bytes per partition in submission order.
    pub async fn run_native_job(
        &self,
        ops: Vec<PipelineOp>,
        partitions: Vec<Vec<u8>>,
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

        let submits = partitions
            .into_iter()
            .enumerate()
            .map(|(partition_id, partition_data)| {
                let task_id = self.get_mutators().get_next_task_id();
                let attempt_id = self.attempt_id.fetch_add(1, Ordering::SeqCst);
                let task_key = format!("{}:{}", run_id, task_id);
                let task = TaskEnvelope::new(
                    run_id,
                    stage_id,
                    task_id,
                    attempt_id,
                    partition_id,
                    format!("native-pipeline-{}-{}", partition_id, pipeline_label),
                    ops.clone(),
                    partition_data,
                );

                self.taskid_to_jobid.insert(task_key.clone(), run_id);
                self.job_tasks
                    .entry(run_id)
                    .or_default()
                    .insert(task_key.clone());

                async move {
                    let (result, worker_addr) = self.submit_native_task(&task).await?;
                    self.taskid_to_slaveid
                        .insert(task_key, worker_addr.to_string());
                    // Surface application-level failures rather than silently dropping them.
                    match result.status {
                        atomic_data::distributed::ResultStatus::FatalFailure => {
                            return Err(SchedulerError::TaskFailed(
                                result.error.unwrap_or_else(|| "fatal failure".to_string()),
                            ));
                        }
                        atomic_data::distributed::ResultStatus::RetryableFailure => {
                            // submit_native_task already retried at the TCP layer;
                            // promote to a job-level failure.
                            return Err(SchedulerError::TaskFailed(
                                result.error.unwrap_or_else(|| "retryable failure exhausted".to_string()),
                            ));
                        }
                        atomic_data::distributed::ResultStatus::Success => {}
                    }
                    Ok::<_, SchedulerError>(result)
                }
            });

        let result = try_join_all(submits).await;

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

        result.map(|mut responses| {
            // Sort by partition_id so result order matches partition submission order
            // even when tasks are retried and land on different workers.
            responses.sort_by_key(|r| r.partition_id);
            responses.into_iter().map(|r| r.data).collect()
        })
    }

    /// Run the shuffle-map phase of a shuffle stage in distributed mode.
    ///
    /// Dispatches one `TaskEnvelope` per input partition; each worker stores its output
    /// buckets in its local `ShuffleCache` and serves them via its `ShuffleManager` HTTP
    /// server. The worker returns its server URI in `TaskResultEnvelope::shuffle_server_uri`.
    /// This method registers all URIs with the driver's `MapOutputTracker` so the reduce
    /// phase can locate and fetch the right buckets from each worker.
    pub async fn run_shuffle_map_stage(
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
            let task = TaskEnvelope::new(
                0,
                stage_id,
                task_id,
                attempt_id,
                part_id,
                format!("shuffle-map-{}-{}", shuffle_id, part_id),
                ops.clone(),
                data,
            );
            async move {
                let (result, _worker) = self.submit_native_task(&task).await?;
                match result.status {
                    atomic_data::distributed::ResultStatus::FatalFailure => {
                        return Err(SchedulerError::TaskFailed(
                            result.error.unwrap_or_else(|| "shuffle map fatal failure".to_string()),
                        ));
                    }
                    atomic_data::distributed::ResultStatus::RetryableFailure => {
                        return Err(SchedulerError::TaskFailed(
                            result.error.unwrap_or_else(|| "shuffle map retryable failure exhausted".to_string()),
                        ));
                    }
                    atomic_data::distributed::ResultStatus::Success => {}
                }
                Ok::<_, SchedulerError>((part_id, result.shuffle_server_uri))
            }
        });

        let responses = try_join_all(submits).await?;

        let mut locs: Vec<Option<String>> = vec![None; num_partitions];
        for (part_id, uri_opt) in responses {
            if let Some(uri) = uri_opt {
                m.add_output_loc_to_stage(stage_id, part_id, uri.clone());
                locs[part_id] = Some(uri);
            } else {
                log::warn!(
                    "shuffle-map stage {}: partition {} worker returned no shuffle URI",
                    stage_id,
                    part_id
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
        log::debug!("legacy submit_task ignored; use run_native_job");
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
        self.live_listener_bus.stop().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomic_data::distributed::{
        ResultStatus, TRANSPORT_HEADER_LEN, TaskAction, TaskResultEnvelope, WireDecode, WireEncode,
        encode_transport_frame, parse_transport_header,
    };

    #[test]
    fn register_worker_adds_to_server_list() {
        let scheduler = DistributedScheduler::new(4, true);
        let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31001);
        scheduler.register_worker(
            addr,
            WorkerCapabilities {
                version: 1,
                worker_id: "native-1".to_string(),
                max_tasks: 2,
            },
        );
        let selected = scheduler.next_executor().expect("should select worker");
        assert_eq!(selected, addr);
    }

    #[test]
    fn next_executor_round_robins() {
        let scheduler = DistributedScheduler::new(4, true);
        let addr1 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31011);
        let addr2 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 31012);
        scheduler.register_worker(
            addr1,
            WorkerCapabilities {
                version: 1,
                worker_id: "w1".to_string(),
                max_tasks: 1,
            },
        );
        scheduler.register_worker(
            addr2,
            WorkerCapabilities {
                version: 1,
                worker_id: "w2".to_string(),
                max_tasks: 1,
            },
        );
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
            WorkerCapabilities {
                version: 1,
                worker_id: "w1".to_string(),
                max_tasks: 1,
            },
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
