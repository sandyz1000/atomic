use crate::env::{Config, DeploymentMode};
use crate::error::{ComputeError, ComputeResult};
use crate::executor::{Executor, Signal};
use crate::runtimes::{Backend, ComputeEngine};
use crate::io::ReaderConfiguration;
use crate::rdd::typed::TypedRdd;
use crate::rdd::{ParallelCollection, UnionRdd};
use crate::{env, hosts};
use atomic_data::accumulator::{Accumulator, MergeFn, make_merge_fn, next_accumulator_id};
use atomic_data::broadcast::{BroadcastVar, next_broadcast_id};
use atomic_data::data::Data;
use atomic_data::distributed::{
    PipelineOp, ResultStatus, TRANSPORT_HEADER_LEN, TaskAction, TaskEnvelope, TaskRuntime,
    TransportFrameKind, WireDecode, WireEncode, WorkerCapabilities, encode_transport_frame,
    parse_transport_header,
};
use atomic_data::partial::ApproximateEvaluator;
use atomic_data::partial::result::PartialResult;
use atomic_data::rdd::{Rdd, RddBase};
use atomic_data::task_context::TaskContext;
use atomic_scheduler::{DistributedScheduler, LocalScheduler, Schedulers};
use atomic_utils::clean_up_work_dir;
use log::error;
use std::fmt::Debug;
use std::fs;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use uuid::Uuid;

pub struct Context {
    /// Runtime configuration — built at the entry point and passed in.
    config: Arc<Config>,
    /// Routes native `#[task]` jobs: local `NativeBackend` or distributed TCP dispatch.
    scheduler: Schedulers,
    /// Always-local scheduler used by `run_job` for closure-based driver operations.
    /// Closures cannot be sent to remote workers, so all `collect()`, `count()`,
    /// `fold()`, `take()` etc. execute here regardless of deployment mode.
    driver_scheduler: Arc<LocalScheduler>,
    pub(crate) next_rdd_id: Arc<AtomicUsize>,
    pub(crate) next_shuffle_id: Arc<AtomicUsize>,
    pub(crate) address_map: Vec<SocketAddrV4>,
    pub(crate) distributed_driver: bool,
    pub(crate) work_dir: PathBuf,
    /// Driver-side broadcast variable store: `broadcast_id → rkyv-encoded bytes`.
    /// Attached to every TaskEnvelope dispatched to workers.
    pub(crate) broadcast_store: Arc<dashmap::DashMap<usize, Vec<u8>>>,
    /// Driver-side accumulator store: `accumulator_id → (current_bytes, merge_fn)`.
    /// Updated by `merge_accumulator_deltas` after each task completes.
    pub(crate) accumulator_store: Arc<dashmap::DashMap<usize, (Vec<u8>, Arc<MergeFn>)>>,
}

impl Drop for Context {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        {
            if self.distributed_driver {
                log::info!("inside context drop in master");
            }
        }
        Context::driver_clean_up_directives(&self.work_dir, &self.address_map);
        atomic_data::env::clear_shuffle_infrastructure();
    }
}

pub fn save<R: Data>(
    ctx: TaskContext,
    iter: Box<dyn Iterator<Item = R>>,
    path: String,
) -> crate::error::ComputeResult<()> {
    std::fs::create_dir_all(&path).map_err(crate::error::ComputeError::OutputWrite)?;
    let id = ctx.split_id;
    let file_path = std::path::Path::new(&path).join(format!("part-{}", id));
    let f = std::fs::File::create(file_path).map_err(crate::error::ComputeError::OutputWrite)?;
    let mut f = std::io::BufWriter::new(f);
    for item in iter {
        let line = format!("{:?}", item);
        f.write_all(line.as_bytes())
            .map_err(crate::error::ComputeError::OutputWrite)?;
    }
    Ok(())
}

impl Context {
    /// Create a context using an explicit [`Config`] built at the program entry point.
    ///
    /// This is the preferred constructor for new Rust programs. Build the config
    /// at `main()` using [`Config::local`], [`Config::distributed_driver`], or
    /// [`Config::worker`], then pass it here.
    ///
    /// For a worker process, use [`start_worker`] instead — it takes a `Config`
    /// and never returns.
    pub fn new_with_config(config: Config) -> ComputeResult<Arc<Self>> {
        match config.mode {
            DeploymentMode::Distributed => {
                let ctx = Context::init_distributed_driver(config)?;
                ctx.set_cleanup_process();
                Ok(ctx)
            }
            DeploymentMode::Local => Context::init_local_scheduler(config),
        }
    }

    /// Create a context from environment variables.
    ///
    /// Reads `ATOMIC_DEPLOYMENT_MODE`, `ATOMIC_LOCAL_IP`, `ATOMIC_SLAVE_PORT`, etc.
    /// Prefer [`Context::new_with_config`] for new Rust programs; this exists for
    /// Python/JS bindings and legacy code where explicit config is not practical.
    pub fn new() -> ComputeResult<Arc<Self>> {
        let mut config = Config::from_env();
        // In env-var mode, load workers from hosts.conf for distributed drivers.
        if config.mode == DeploymentMode::Distributed
            && config.workers.is_empty()
            && let Ok(hosts) = hosts::Hosts::get()
        {
            config.workers = hosts
                .slaves
                .iter()
                .filter_map(|s| {
                    let hp = s.split('@').nth(1)?;
                    hp.parse().ok()
                })
                .collect();
        }
        Context::new_with_config(config)
    }

    /// Create a local-only context.
    pub fn local() -> ComputeResult<Arc<Self>> {
        Context::new_with_config(Config::local())
    }

    pub fn is_distributed(&self) -> bool {
        self.distributed_driver
    }

    fn set_cleanup_process(&self) {
        let address_map = self.address_map.clone();
        let work_dir = self.work_dir.clone();
        env::Env::run_in_async_rt(|| {
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    log::info!("received termination signal, cleaning up");
                    Context::driver_clean_up_directives(&work_dir, &address_map);
                    std::process::exit(0);
                }
            });
        })
    }

    fn init_local_scheduler(config: Config) -> ComputeResult<Arc<Self>> {
        let job_id = Uuid::new_v4().to_string();
        let job_work_dir = config.work_dir.join(format!("ns-session-{}", job_id));
        fs::create_dir_all(&job_work_dir).map_err(ComputeError::OutputWrite)?;

        let _ = env_logger::try_init();
        atomic_data::cache::init_partition_cache();
        // Set the RDD cache spill directory for MemoryAndDisk / DiskOnly partitions.
        let spill_dir = job_work_dir.join("rdd-cache");
        fs::create_dir_all(&spill_dir).ok();
        atomic_data::env::set_rdd_cache_spill_dir(spill_dir);
        // Start Prometheus metrics server if a port is configured.
        if let Some(port) = config.metrics_port {
            atomic_scheduler::metrics::init_metrics();
            env::Env::run_in_async_rt(|| {
                atomic_scheduler::metrics::start_metrics_server(port);
            });
        }
        let config = Arc::new(config);
        if let Err(e) = env::Env::run_in_async_rt(|| env::init_shuffle(&config)) {
            log::warn!("shuffle service could not start (wide transforms will be local-only): {e}");
        }
        let local = Arc::new(LocalScheduler::new_with_coalesce(
            20,
            true,
            config.coalesce_shuffle_threshold_bytes,
        ));
        let scheduler = Schedulers::Local(local.clone());

        Ok(Arc::new(Context {
            config,
            scheduler,
            driver_scheduler: local,
            next_rdd_id: Arc::new(AtomicUsize::new(0)),
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            address_map: vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)],
            distributed_driver: false,
            work_dir: job_work_dir,
            broadcast_store: Arc::new(dashmap::DashMap::new()),
            accumulator_store: Arc::new(dashmap::DashMap::new()),
        }))
    }

    /// Connect to all registered workers and return a distributed driver context.
    fn init_distributed_driver(config: Config) -> ComputeResult<Arc<Self>> {
        let job_id = Uuid::new_v4().to_string();
        let job_work_dir = config.work_dir.join(format!("ns-session-{}", job_id));

        fs::create_dir_all(&job_work_dir).unwrap();
        let _ = env_logger::try_init();
        atomic_data::cache::init_partition_cache();

        let config = Arc::new(config);

        // init_shuffle starts an async HTTP server — wrap in the tokio runtime.
        if let Err(e) = env::Env::run_in_async_rt(|| env::init_shuffle(&config)) {
            log::warn!("shuffle service could not start: {e}");
        }

        let mut dist_sched = DistributedScheduler::new(20, true)
            .with_driver_fingerprint(*crate::task_registry::REGISTRY_FINGERPRINT);
        if let Some(m) = config.speculation_multiplier {
            dist_sched = dist_sched.with_speculation(m);
        }
        let scheduler = Arc::new(dist_sched);
        let mut address_map = Vec::new();

        for &endpoint in &config.workers {
            log::info!("connecting to worker at {}", endpoint);
            match Context::request_worker_capabilities(endpoint) {
                Ok(capabilities) => {
                    log::info!(
                        "worker {} ready (max_tasks={})",
                        capabilities.worker_id,
                        capabilities.max_tasks,
                    );
                    scheduler.register_worker(endpoint, capabilities);
                    address_map.push(endpoint);
                }
                Err(e) => {
                    log::warn!(
                        "worker {} unreachable during handshake, skipping: {}",
                        endpoint,
                        e
                    );
                }
            }
        }

        if address_map.is_empty() {
            return Err(ComputeError::WorkerHandshake(
                "no reachable workers found".to_string(),
            ));
        }

        // Start proactive heartbeat loop if configured.
        env::Env::run_in_async_rt(|| {
            scheduler.start_heartbeat(config.heartbeat_interval_secs, config.heartbeat_timeout_ms);
        });

        // Start self-registration endpoint so workers can join dynamically.
        if let Some(port) = config.register_port {
            env::Env::run_in_async_rt(|| {
                atomic_scheduler::start_register_server(port, Arc::clone(&scheduler));
            });
            log::info!("worker registration endpoint started on port {port}");
        }

        let scheduler = Schedulers::Distributed(scheduler);
        let driver_scheduler = Arc::new(LocalScheduler::new_with_coalesce(
            20,
            false,
            config.coalesce_shuffle_threshold_bytes,
        ));

        Ok(Arc::new(Context {
            config,
            scheduler,
            driver_scheduler,
            next_rdd_id: Arc::new(AtomicUsize::new(0)),
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            address_map,
            distributed_driver: true,
            work_dir: job_work_dir,
            broadcast_store: Arc::new(dashmap::DashMap::new()),
            accumulator_store: Arc::new(dashmap::DashMap::new()),
        }))
    }

    fn worker_clean_up_directives(run_result: ComputeResult<Signal>, work_dir: PathBuf) -> ! {
        clean_up_work_dir(&work_dir, true);
        match run_result {
            Err(err) => {
                log::error!("executor failed with error: {}", err);
                std::process::exit(1);
            }
            Ok(value) => {
                log::info!("executor closed gracefully with signal: {:?}", value);
                std::process::exit(0);
            }
        }
    }

    /// Shut down the compute context, releasing shuffle infrastructure.
    ///
    /// Called by `StreamingContext::stop(stop_sc=true)` to also tear down the
    /// underlying compute layer.
    pub fn shutdown(&self) {
        atomic_data::env::clear_shuffle_infrastructure();
    }

    fn driver_clean_up_directives(work_dir: &std::path::Path, _executors: &[SocketAddrV4]) {
        // Workers are long-running daemons — the driver does NOT send shutdown
        // signals on completion. Workers stay alive for subsequent driver runs
        // and must be stopped explicitly (Ctrl-C or a dedicated stop command).
        clean_up_work_dir(work_dir, true);
    }

    fn request_worker_capabilities(endpoint: SocketAddrV4) -> ComputeResult<WorkerCapabilities> {
        let frame = encode_transport_frame(TransportFrameKind::WorkerCapabilities, &[]);
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut last_err = None;

        while Instant::now() < deadline {
            match TcpStream::connect(endpoint) {
                Ok(mut stream) => {
                    stream
                        .write_all(&frame)
                        .map_err(ComputeError::OutputWrite)?;

                    let mut header = [0_u8; TRANSPORT_HEADER_LEN];
                    stream
                        .read_exact(&mut header)
                        .map_err(ComputeError::InputRead)?;
                    let (kind, payload_len) = parse_transport_header(&header)
                        .map_err(|err| ComputeError::InvalidTransportFrame(err.to_string()))?;
                    if kind != TransportFrameKind::WorkerCapabilities {
                        return Err(ComputeError::WorkerHandshake(format!(
                            "unexpected worker response frame: {:?}",
                            kind
                        )));
                    }

                    let mut payload = vec![0_u8; payload_len];
                    stream
                        .read_exact(&mut payload)
                        .map_err(ComputeError::InputRead)?;
                    return WorkerCapabilities::decode_wire(&payload)
                        .map_err(|err| ComputeError::WorkerHandshake(err.to_string()));
                }
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::ConnectionRefused {
                        // Worker is actively refusing — it is not starting up; it is dead.
                        return Err(ComputeError::WorkerHandshake(format!(
                            "worker {} refused connection ({})",
                            endpoint, err
                        )));
                    }
                    last_err = Some(err.to_string());
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }

        Err(ComputeError::WorkerHandshake(format!(
            "timed out waiting for worker {} ({})",
            endpoint,
            last_err.unwrap_or_else(|| "no response".to_string())
        )))
    }

    /// Send a graceful-shutdown signal to every registered worker.
    ///
    /// Not called automatically — reserved for an explicit `atomic stop` command.
    #[allow(dead_code)]
    pub(crate) fn drop_executors(address_map: &[SocketAddrV4]) {
        for socket_addr in address_map {
            log::debug!(
                "dropping executor {}:{}",
                socket_addr.ip(),
                socket_addr.port()
            );
            match serde_json::to_vec(&Signal::ShutDownGracefully) {
                Err(e) => error!("Failed to serialise shutdown signal: {}", e),
                Ok(json) => {
                    let addr = format!("{}:{}", socket_addr.ip(), socket_addr.port() + 10);
                    match TcpStream::connect(&addr) {
                        Err(_) => error!("Failed to connect to {} to stop its executor", addr),
                        Ok(mut stream) => {
                            let mut signal = (json.len() as u32).to_le_bytes().to_vec();
                            signal.extend_from_slice(&json);
                            if let Err(e) = stream.write_all(&signal) {
                                error!("Failed to send shutdown signal to {}: {}", addr, e);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Cancel a running distributed job by its `run_id`.
    ///
    /// The cancellation is best-effort: tasks that have already received their results
    /// from a worker are not rolled back.  In local mode this is a no-op.
    pub fn cancel_job(&self, run_id: usize) -> ComputeResult<()> {
        match &self.scheduler {
            atomic_scheduler::Schedulers::Distributed(sched) => Ok(sched.cancel_job(run_id)?),
            atomic_scheduler::Schedulers::Local(_) => Ok(()),
        }
    }

    /// Gracefully stop this context.
    ///
    /// In distributed mode, sends a `ShutDownGracefully` signal to every registered worker.
    /// Clears the global shuffle infrastructure so a new context can be started in the same
    /// process.  In local mode this is a no-op (local threads finish naturally on drop).
    pub fn stop(&self) {
        if !self.config.workers.is_empty() {
            Context::drop_executors(&self.config.workers);
        }
        atomic_data::env::clear_shuffle_infrastructure();
    }

    /// Collect all elements of an RDD into a `Vec`, distribution-aware.
    ///
    /// In **local mode** runs via the driver's thread-pool scheduler.
    /// In **distributed mode** delegates to `TypedRdd::collect()` which routes
    /// staged pipelines through `dispatch_pipeline()` and shuffle deps through
    /// `run_pending_shuffle_stages()`.
    ///
    /// This is the preferred method for streaming output operations that receive
    /// an `Arc<dyn Rdd>` from the DStream graph and want to materialise it.
    pub fn collect_rdd<T>(self: &Arc<Self>, rdd: Arc<dyn Rdd<Item = T>>) -> ComputeResult<Vec<T>>
    where
        T: Data + Clone + WireDecode,
        Vec<T>: WireDecode,
    {
        use crate::rdd::TypedRdd;
        Ok(TypedRdd::new(rdd, self.clone()).collect()?)
    }

    pub fn new_rdd_id(&self) -> usize {
        self.next_rdd_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn new_shuffle_id(&self) -> usize {
        self.next_shuffle_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Create a broadcast variable.
    ///
    /// The value is rkyv-encoded on the driver and embedded in every `TaskEnvelope`
    /// dispatched to workers. Workers call `broadcast_var.value()` inside `#[task]`
    /// functions to read the data without re-serializing per element.
    ///
    /// In local mode the broadcast value is still loaded into the thread-local registry
    /// before each task, so the same `#[task]` code works in both modes.
    pub fn broadcast<T>(self: &Arc<Self>, value: T) -> BroadcastVar<T>
    where
        T: atomic_data::distributed::WireEncode + atomic_data::distributed::WireDecode,
    {
        let id = next_broadcast_id();
        let bytes = value.encode_wire().expect("broadcast: encode failed");
        self.broadcast_store.insert(id, bytes);
        BroadcastVar::new(id)
    }

    /// Return a snapshot of all broadcast values as `(id, bytes)` pairs.
    /// Used by `dispatch_pipeline` and `run_shuffle_map_stage` to attach broadcasts
    /// to outgoing `TaskEnvelope`s.
    pub fn broadcast_snapshot(&self) -> Vec<(usize, Vec<u8>)> {
        self.broadcast_store
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }

    /// Create an accumulator with an initial value and an associative merge function.
    ///
    /// Workers call `acc.add(delta)` inside `#[task]` functions. The driver merges
    /// per-task deltas by calling `merge(current, delta)` after every task result.
    /// Read the current driver-side value with `Context::accumulator_value(&acc)`.
    pub fn accumulator<T, F>(self: &Arc<Self>, init: T, merge: F) -> Accumulator<T>
    where
        T: atomic_data::distributed::WireEncode + atomic_data::distributed::WireDecode + 'static,
        F: Fn(T, T) -> T + Send + Sync + 'static,
    {
        let id = next_accumulator_id();
        let bytes = init.encode_wire().expect("accumulator: encode init failed");
        let merge_fn = Arc::new(make_merge_fn::<T, F>(merge));
        self.accumulator_store.insert(id, (bytes, merge_fn));
        Accumulator::new(id)
    }

    /// Read the current driver-side value of an accumulator.
    pub fn accumulator_value<T>(&self, acc: &Accumulator<T>) -> T
    where
        T: atomic_data::distributed::WireDecode,
    {
        let entry = self.accumulator_store.get(&acc.id).unwrap_or_else(|| {
            panic!(
                "accumulator id={} is not registered on this context; \
                 ensure the accumulator was created from the same Context",
                acc.id
            )
        });
        T::decode_wire(&entry.value().0).unwrap_or_else(|e| {
            panic!(
                "accumulator id={}: failed to decode current value: {}",
                acc.id, e
            )
        })
    }

    /// Merge incoming accumulator deltas from a completed task result into driver-side values.
    /// Called by the scheduler after each successful task.
    pub fn merge_accumulator_deltas(&self, deltas: &[(usize, Vec<u8>)]) {
        for (id, delta_bytes) in deltas {
            if let Some(mut entry) = self.accumulator_store.get_mut(id) {
                let merge_fn = entry.value().1.clone();
                let new_bytes = merge_fn(entry.value().0.clone(), delta_bytes.clone());
                entry.value_mut().0 = new_bytes;
            }
        }
    }

    /// Default number of output partitions for wide transformations (reduce_by_key, group_by_key).
    /// Uses the number of CPUs, clamped to a sensible range.
    pub fn default_parallelism(&self) -> usize {
        num_cpus::get().clamp(1, 64)
    }

    pub fn make_rdd<T: Data + Clone, I>(
        self: &Arc<Self>,
        seq: I,
        num_slices: usize,
    ) -> Arc<dyn Rdd<Item = T>>
    where
        I: IntoIterator<Item = T>,
    {
        let rdd = self.parallelize(seq, num_slices);
        rdd.register_op_name("make_rdd");
        rdd
    }

    pub fn range(
        self: &Arc<Self>,
        start: u64,
        end: u64,
        step: usize,
        num_slices: usize,
    ) -> Arc<dyn Rdd<Item = u64>> {
        let seq = (start..=end).step_by(step);
        let rdd = self.parallelize(seq, num_slices);
        rdd.register_op_name("range");
        rdd
    }

    pub fn parallelize<T: Data + Clone, I>(
        self: &Arc<Self>,
        seq: I,
        num_slices: usize,
    ) -> Arc<dyn Rdd<Item = T>>
    where
        I: IntoIterator<Item = T>,
    {
        let id = self.new_rdd_id();
        Arc::new(ParallelCollection::new(id, seq, num_slices))
    }

    /// Create a TypedRdd from a collection with explicit typing.
    pub fn parallelize_typed<T: Data + Clone, I>(
        self: &Arc<Self>,
        seq: I,
        num_slices: usize,
    ) -> TypedRdd<T>
    where
        I: IntoIterator<Item = T>,
    {
        let id = self.new_rdd_id();
        let rdd = Arc::new(ParallelCollection::new(id, seq, num_slices));
        TypedRdd::new(rdd, self.clone())
    }

    pub fn read_source<F, C, I: Data, O: Data>(
        self: &Arc<Self>,
        config: C,
        func: F,
    ) -> Arc<dyn Rdd<Item = O>>
    where
        F: Fn(I) -> O + Send + Sync + 'static,
        C: ReaderConfiguration<I>,
    {
        config.make_reader(self.clone(), func)
    }

    /// Read a text file (or directory of files, or S3 prefix) as a `TypedRdd<String>`.
    ///
    /// URI schemes:
    /// - `s3://bucket/prefix` — lists all objects under the prefix; each key is one partition.
    ///   Requires the `s3` feature flag.
    /// - `file:///absolute/path` or `/absolute/path` or `relative/path` — reads a local file
    ///   (single partition) or, if the path is a directory, all files in the directory (one
    ///   partition per file).
    ///
    /// Each partition yields one line per element.
    pub fn text_file(self: &Arc<Self>, uri: &str) -> TypedRdd<String> {
        use crate::io::{TextFileRdd, TextFileSource};

        let sources: Vec<TextFileSource> = if uri.starts_with("s3://") {
            #[cfg(feature = "s3")]
            {
                use crate::io::s3::s3_impl::S3Uri;
                if let Some(s3uri) = S3Uri::parse(uri) {
                    let keys = crate::io::s3::s3_impl::list_keys(&s3uri.bucket, &s3uri.key);
                    if keys.is_empty() {
                        // Treat the URI itself as a single key (file, not a prefix directory).
                        vec![TextFileSource::S3 {
                            bucket: s3uri.bucket,
                            key: s3uri.key,
                        }]
                    } else {
                        keys.into_iter()
                            .map(|k| TextFileSource::S3 {
                                bucket: s3uri.bucket.clone(),
                                key: k,
                            })
                            .collect()
                    }
                } else {
                    vec![]
                }
            }
            #[cfg(not(feature = "s3"))]
            {
                log::warn!("text_file: s3:// URI requested but 's3' feature is disabled");
                vec![]
            }
        } else {
            // Local filesystem — strip file:// if present
            let path = std::path::Path::new(uri.strip_prefix("file://").unwrap_or(uri));
            if path.is_dir() {
                std::fs::read_dir(path)
                    .into_iter()
                    .flatten()
                    .filter_map(|entry| entry.ok())
                    .map(|entry| TextFileSource::Local(entry.path()))
                    .collect()
            } else {
                vec![TextFileSource::Local(path.to_path_buf())]
            }
        };

        let id = self.new_rdd_id();
        let rdd = Arc::new(TextFileRdd::new(id, sources));
        TypedRdd::new(rdd, self.clone())
    }

    pub fn run_job<T: Data, U: Data + Clone, F>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
    ) -> ComputeResult<Vec<U>>
    where
        F: Fn(Box<dyn Iterator<Item = T>>) -> U + Send + Sync + 'static,
    {
        // Closures cannot be sent to remote workers; always execute on the driver
        // using the dedicated local scheduler, regardless of deployment mode.
        // `run_in_async_rt` ensures spawn_blocking (used by LocalScheduler) has a
        // Tokio handle, and that shuffle fetches (hyper HTTP) work correctly.
        let cl = move |(_task_context, iter)| (func)(iter);
        let sched = self.driver_scheduler.clone();
        let partitions = (0..rdd.number_of_splits()).collect();
        Ok(env::Env::run_in_async_rt(|| {
            sched.run_job(Arc::new(cl), rdd, partitions, false)
        })?)
    }

    pub fn run_job_with_partitions<T: Data, U: Data + Clone, F, P>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
        partitions: P,
    ) -> ComputeResult<Vec<U>>
    where
        F: Fn(Box<dyn Iterator<Item = T>>) -> U + Send + Sync + 'static,
        P: IntoIterator<Item = usize>,
    {
        let cl = move |(_task_context, iter)| (func)(iter);
        let sched = self.driver_scheduler.clone();
        let partitions: Vec<usize> = partitions.into_iter().collect();
        Ok(env::Env::run_in_async_rt(|| {
            sched.run_job(Arc::new(cl), rdd, partitions, false)
        })?)
    }

    pub fn run_job_with_context<T: Data, U: Data + Clone, F>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
    ) -> ComputeResult<Vec<U>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
    {
        log::debug!("inside run job in context");
        let func = Arc::new(func);
        let sched = self.driver_scheduler.clone();
        let partitions = (0..rdd.number_of_splits()).collect();
        Ok(env::Env::run_in_async_rt(|| {
            sched.run_job(func, rdd, partitions, false)
        })?)
    }

    pub fn run_approximate_job<T: Data, U: Data + Clone, R, F, E>(
        self: &Arc<Self>,
        func: F,
        rdd: Arc<dyn Rdd<Item = T>>,
        evaluator: E,
        timeout: Duration,
    ) -> ComputeResult<PartialResult<R>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
        E: ApproximateEvaluator<U, R> + Send + Sync + 'static,
        R: Clone + Debug + Send + Sync + 'static,
    {
        Ok(self
            .scheduler
            .run_approximate_job(Arc::new(func), rdd, evaluator, timeout)?)
    }

    pub fn union<T: Data + Clone>(
        self: &Arc<Self>,
        rdds: &[Arc<dyn Rdd<Item = T>>],
    ) -> ComputeResult<UnionRdd<T>> {
        UnionRdd::new(self.new_rdd_id(), rdds)
    }

    /// Dispatch a `#[task]`-registered Map/Filter/FlatMap over every partition,
    /// returning decoded `Vec<U>` per partition.
    ///
    /// Thin wrapper over [`dispatch_pipeline`] for single-op map jobs.
    pub fn run_native_job_map<T, U>(
        self: &Arc<Self>,
        op_id: &str,
        action: TaskAction,
        payload: Vec<u8>,
        rdd: Arc<dyn Rdd<Item = T>>,
    ) -> ComputeResult<Vec<Vec<U>>>
    where
        T: Data + Clone + WireEncode,
        Vec<T>: WireEncode,
        U: Data + Clone + WireDecode,
        Vec<U>: WireDecode,
    {
        let ops = vec![PipelineOp {
            op_id: op_id.to_string(),
            action,
            runtime: TaskRuntime::Native,
            payload,
        }];
        let encoded = Self::encode_rdd_partitions(rdd)?;
        let result_bytes = self.dispatch_pipeline(encoded, ops)?;
        result_bytes
            .into_iter()
            .map(|bytes| Vec::<U>::decode_wire(&bytes).map_err(ComputeError::from))
            .collect()
    }

    /// Dispatch a `#[task]`-registered binary Fold over every partition,
    /// then combine the per-partition results into a single value on the driver.
    ///
    /// Thin wrapper over [`dispatch_pipeline`] for single-op fold jobs.
    pub fn run_native_job_fold<T>(
        self: &Arc<Self>,
        op_id: &str,
        zero: T,
        rdd: Arc<dyn Rdd<Item = T>>,
    ) -> ComputeResult<T>
    where
        T: Data + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        let payload = zero.encode_wire()?;
        let ops = vec![PipelineOp {
            op_id: op_id.to_string(),
            action: TaskAction::Fold,
            runtime: TaskRuntime::Native,
            payload,
        }];
        let encoded = Self::encode_rdd_partitions(rdd)?;
        let partition_results_raw = self.dispatch_pipeline(encoded, ops.clone())?;

        let mut partition_values: Vec<T> = partition_results_raw
            .into_iter()
            .map(|bytes| T::decode_wire(&bytes).map_err(ComputeError::from))
            .collect::<ComputeResult<_, _>>()?;

        if partition_values.is_empty() {
            return Ok(zero);
        }
        if partition_values.len() == 1 {
            return Ok(partition_values.remove(0));
        }

        // Combine partition results via Reduce on the driver using NativeBackend.
        let combined_data = partition_values.encode_wire()?;
        let reduce_ops = vec![PipelineOp {
            op_id: op_id.to_string(),
            action: TaskAction::Reduce,
            runtime: TaskRuntime::Native,
            payload: vec![],
        }];
        let task = TaskEnvelope::new(
            0,
            0,
            0,
            0,
            0,
            format!("driver-reduce-{}", op_id),
            reduce_ops,
            combined_data,
        );
        let result = ComputeEngine::default().execute("local-driver", &task)?;
        match result.status {
            ResultStatus::Success => Ok(T::decode_wire(&result.data)?),
            _ => Err(ComputeError::InvalidPayload(
                result.error.unwrap_or_else(|| "reduce failed".to_string()),
            )),
        }
    }

    /// Dispatch a full pipeline of ops over pre-encoded partition bytes.
    ///
    /// - **Local mode**: runs all ops via [`NativeBackend`] in-process.
    /// - **Distributed mode**: sends one `TaskEnvelope` per partition to a worker via TCP.
    ///
    /// Returns raw result bytes per partition; callers decode into the concrete type.
    pub fn dispatch_pipeline(
        &self,
        source_partitions: Vec<Vec<u8>>,
        ops: Vec<PipelineOp>,
    ) -> ComputeResult<Vec<Vec<u8>>> {
        let broadcasts = self.broadcast_snapshot();
        match &self.scheduler {
            Schedulers::Local(_) => {
                let backend = ComputeEngine::default();
                source_partitions
                    .into_iter()
                    .enumerate()
                    .map(|(part_id, data)| {
                        let task = TaskEnvelope::new(
                            0,
                            0,
                            part_id,
                            0,
                            part_id,
                            format!("local-pipeline-{}", part_id),
                            ops.clone(),
                            data,
                        )
                        .with_broadcasts(broadcasts.clone());
                        let result = backend.execute("local-driver", &task)?;
                        match result.status {
                            ResultStatus::Success => {
                                if !result.accumulator_deltas.is_empty() {
                                    self.merge_accumulator_deltas(&result.accumulator_deltas);
                                }
                                Ok(result.data)
                            }
                            _ => Err(ComputeError::InvalidPayload(
                                result.error.unwrap_or_else(|| "task failed".to_string()),
                            )),
                        }
                    })
                    .collect()
            }
            Schedulers::Distributed(sched) => Ok(env::Env::run_in_async_rt(|| {
                futures::executor::block_on(sched.run_native_job_with_broadcasts(
                    ops,
                    source_partitions,
                    broadcasts,
                ))
            })?),
        }
    }

    /// Encode every partition of an RDD into rkyv bytes.
    pub(crate) fn encode_rdd_partitions<T>(
        rdd: Arc<dyn Rdd<Item = T>>,
    ) -> ComputeResult<Vec<Vec<u8>>>
    where
        T: Data + Clone + WireEncode,
        Vec<T>: WireEncode,
    {
        rdd.splits()
            .iter()
            .map(|split| {
                let items: Vec<T> = rdd.compute(split.clone())?.collect();
                Ok(items.encode_wire()?)
            })
            .collect()
    }

    /// In distributed mode, find all pending `ShuffleDependency` nodes in the DAG,
    /// run their map stages on remote workers, and register all shuffle server URIs
    /// with `MapOutputTracker`.
    ///
    /// After this returns, `ShuffleFetcher::fetch()` can read the shuffle data from
    /// workers via HTTP — enabling the reduce phase to run on the driver.
    ///
    /// # Arguments
    /// - `rdd`: the RDD whose shuffle dependencies should be computed on workers
    ///          (e.g. the parent of a `ShuffledRdd`).
    /// - `preceding_ops`: staged `PipelineOp`s from prior `_task` transforms that
    ///                     should run on workers *before* the shuffle-write op.
    pub fn run_pending_shuffle_stages(
        self: &Arc<Self>,
        rdd: &Arc<dyn RddBase>,
        preceding_ops: Vec<PipelineOp>,
    ) -> ComputeResult<()> {
        use atomic_data::dependency::Dependency;

        let sched = match &self.scheduler {
            Schedulers::Distributed(s) => s.clone(),
            Schedulers::Local(_) => return Ok(()), // local mode: no-op, DAG handles it
        };

        for dep in rdd.get_dependencies() {
            if let Dependency::Shuffle(shuffle_dep) = dep {
                // Encode the parent RDD partitions (the shuffle map input).
                // The typed closure on ShuffleDependencyBox rkyv-encodes Vec<(K,V)> per partition.
                let parent_partitions =
                    (shuffle_dep.encode_partitions)().map_err(ComputeError::InvalidPayload)?;

                // Build the shuffle-map op. The payload carries the dispatch key (for the
                // SHUFFLE_MAP_REGISTRY lookup) plus the serializable partitioner spec, so the
                // worker partitions output with the RDD's real partitioner (range for sort_by_key).
                let shuffle_op = PipelineOp {
                    op_id: format!("shuffle-map-{}", shuffle_dep.shuffle_id),
                    action: TaskAction::ShuffleMap {
                        shuffle_id: shuffle_dep.shuffle_id,
                        num_output_partitions: shuffle_dep.num_output_partitions,
                    },
                    runtime: TaskRuntime::Native,
                    payload: bincode::encode_to_vec(
                        crate::shuffle_map::ShuffleMapPayload {
                            type_id: shuffle_dep.type_id.to_string(),
                            partitioner_spec: shuffle_dep.partitioner_spec.clone(),
                        },
                        bincode::config::standard(),
                    )
                    .map_err(|e| {
                        ComputeError::InvalidPayload(format!("shuffle-map payload encode: {e}"))
                    })?,
                };

                // Prefer ops stored on the dep (set when a staged pipeline precedes the shuffle).
                // Fall back to the caller-supplied preceding_ops.
                let dep_preceding = shuffle_dep.preceding_ops.clone();
                let base_ops = if !dep_preceding.is_empty() {
                    dep_preceding
                } else {
                    preceding_ops.clone()
                };
                let mut ops = base_ops;
                ops.push(shuffle_op);

                env::Env::run_in_async_rt(|| {
                    futures::executor::block_on(sched.run_shuffle_map_stage(
                        shuffle_dep.shuffle_id,
                        ops,
                        parent_partitions,
                    ))
                })?;

                log::info!(
                    "shuffle map stage complete: shuffle_id={} num_reduce_partitions={}",
                    shuffle_dep.shuffle_id,
                    shuffle_dep.num_output_partitions,
                );
            }
        }

        Ok(())
    }
}

/// Start the worker process.
///
/// Initialises the shuffle service from `config`, then enters the TCP
/// task-executor loop. This function **never returns** — it terminates the
/// process when the executor shuts down.
///
/// # Example
/// ```no_run
/// use atomic_compute::env::Config;
/// use atomic_compute::context::start_worker;
/// use std::net::Ipv4Addr;
///
/// let config = Config::worker(Ipv4Addr::LOCALHOST, 10001);
/// start_worker(config);
/// ```
pub fn start_worker(config: Config) -> ! {
    let work_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_default();

    let _ = env_logger::try_init();

    // init_shuffle starts an async HTTP server — run it inside the tokio runtime.
    if let Err(e) = env::Env::run_in_async_rt(|| env::init_shuffle(&config)) {
        log::warn!("shuffle service could not start on worker: {e}");
    }

    // Eagerly warm up the JS V8 runtime on N blocking threads so the first UDF task
    // does not pay the cold-start cost.  spawn_blocking is used because JsRuntime is
    // !Send and must be initialized on the thread that will use it.
    #[cfg(feature = "js")]
    {
        let n = num_cpus::get().max(1);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let handles: Vec<_> = (0..n)
                .map(|_| handle.spawn_blocking(|| crate::runtimes::js::JsDispatcher::with_runtime(|_| {})))
                .collect();
            for h in handles {
                let _ = tokio::task::block_in_place(|| handle.block_on(h));
            }
            log::info!("JS V8 runtime warmed up on {n} blocking threads");
        } else {
            // Not inside a tokio runtime (e.g. integration tests) — skip pre-warming.
            log::debug!("JS V8 warmup skipped: no tokio runtime available");
        }
    }

    let result = config
        .worker
        .as_ref()
        .map(|w| (w.port, w.max_concurrent_tasks))
        .ok_or(ComputeError::GetOrCreateConfig(
            "start_worker called without a WorkerConfig — use Config::worker(ip, port)",
        ))
        .and_then(|(port, max_tasks)| {
            let mut executor = Executor::new(port, max_tasks);
            // If TLS cert/key/CA are configured, upgrade to mutual TLS.
            #[cfg(feature = "tls")]
            if crate::tls::tls_is_configured(
                config.tls_ca_cert.as_deref(),
                config.tls_cert.as_deref(),
                config.tls_key.as_deref(),
            ) {
                executor = executor
                    .with_tls(
                        config.tls_cert.as_ref().unwrap(),
                        config.tls_key.as_ref().unwrap(),
                        config.tls_ca_cert.as_ref().unwrap(),
                    )
                    .map_err(|e| {
                        ComputeError::GetOrCreateConfig(Box::leak(
                            format!("TLS init: {e}").into_boxed_str(),
                        ))
                    })?;
            }
            let executor = Arc::new(executor);
            executor.worker()
        });

    Context::worker_clean_up_directives(result, work_dir)
}

#[cfg(test)]
mod tests {
    use super::Context;

    #[test]
    fn local_context_creates_successfully() {
        let ctx = Context::local();
        assert!(ctx.is_ok());
    }

    #[test]
    fn local_context_is_not_distributed() {
        let ctx = Context::local().unwrap();
        assert!(!ctx.is_distributed());
    }
}
