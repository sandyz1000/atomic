use crate::env::{Config, DeploymentMode};
use crate::error::Error;
use crate::executor::{Executor, Signal};
use crate::io::ReaderConfiguration;
use crate::backend::NativeBackend;
use crate::rdd::typed::TypedRdd;
use crate::rdd::{ParallelCollection, UnionRdd};
use crate::{env, hosts};
use atomic_data::data::Data;
use atomic_data::distributed::{
    TRANSPORT_HEADER_LEN, PipelineOp, ResultStatus, TaskAction, TaskEnvelope, TransportFrameKind,
    WireDecode, WireEncode, WorkerCapabilities, encode_transport_frame, parse_transport_header,
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
    pub next_rdd_id: Arc<AtomicUsize>,
    pub next_shuffle_id: Arc<AtomicUsize>,
    pub address_map: Vec<SocketAddrV4>,
    pub distributed_driver: bool,
    pub work_dir: PathBuf,
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
    }
}

pub fn save<R: Data>(ctx: TaskContext, iter: Box<dyn Iterator<Item = R>>, path: String) {
    std::fs::create_dir_all(&path).unwrap();
    let id = ctx.split_id;
    let file_path = std::path::Path::new(&path).join(format!("part-{}", id));
    let f = std::fs::File::create(file_path).expect("unable to create file");
    let mut f = std::io::BufWriter::new(f);
    for item in iter {
        let line = format!("{:?}", item);
        f.write_all(line.as_bytes())
            .expect("error while writing to file");
    }
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
    pub fn new_with_config(config: Config) -> Result<Arc<Self>, Error> {
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
    /// Reads `VEGA_DEPLOYMENT_MODE`, `VEGA_LOCAL_IP`, `VEGA_SLAVE_PORT`, etc.
    /// Prefer [`Context::new_with_config`] for new Rust programs; this exists for
    /// Python/JS bindings and legacy code where explicit config is not practical.
    pub fn new() -> Result<Arc<Self>, Error> {
        let mut config = Config::from_env();
        // In env-var mode, load workers from hosts.conf for distributed drivers.
        if config.mode == DeploymentMode::Distributed && config.workers.is_empty()
            && let Ok(hosts) = hosts::Hosts::get() {
                config.workers = hosts.slaves
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
    pub fn local() -> Result<Arc<Self>, Error> {
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

    fn init_local_scheduler(config: Config) -> Result<Arc<Self>, Error> {
        let job_id = Uuid::new_v4().to_string();
        let job_work_dir = config.work_dir.join(format!("ns-session-{}", job_id));
        fs::create_dir_all(&job_work_dir).unwrap();

        let _ = env_logger::try_init();
        atomic_data::cache::init_partition_cache();
        let config = Arc::new(config);
        if let Err(e) = env::Env::run_in_async_rt(|| env::init_shuffle(&config)) {
            log::warn!("shuffle service could not start (wide transforms will be local-only): {e}");
        }
        let local = Arc::new(LocalScheduler::new(20, true));
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
        }))
    }

    /// Connect to all registered workers and return a distributed driver context.
    fn init_distributed_driver(config: Config) -> Result<Arc<Self>, Error> {
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

        let scheduler = Arc::new(DistributedScheduler::new(20, true));
        let mut address_map = Vec::new();

        for &endpoint in &config.workers {
            log::info!("connecting to worker at {}", endpoint);
            let capabilities = Context::request_worker_capabilities(endpoint)?;
            log::info!(
                "worker {} ready (max_tasks={})",
                capabilities.worker_id,
                capabilities.max_tasks,
            );
            scheduler.register_worker(endpoint, capabilities);
            address_map.push(endpoint);
        }

        let scheduler = Schedulers::Distributed(scheduler);
        let driver_scheduler = Arc::new(LocalScheduler::new(20, false));

        Ok(Arc::new(Context {
            config,
            scheduler,
            driver_scheduler,
            next_rdd_id: Arc::new(AtomicUsize::new(0)),
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            address_map,
            distributed_driver: true,
            work_dir: job_work_dir,
        }))
    }

    fn worker_clean_up_directives(run_result: Result<Signal, Error>, work_dir: PathBuf) -> ! {
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

    fn driver_clean_up_directives(work_dir: &std::path::Path, _executors: &[SocketAddrV4]) {
        // Workers are long-running daemons — the driver does NOT send shutdown
        // signals on completion. Workers stay alive for subsequent driver runs
        // and must be stopped explicitly (Ctrl-C or a dedicated stop command).
        clean_up_work_dir(work_dir, true);
    }

    fn request_worker_capabilities(endpoint: SocketAddrV4) -> Result<WorkerCapabilities, Error> {
        let frame = encode_transport_frame(TransportFrameKind::WorkerCapabilities, &[]);
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut last_err = None;

        while Instant::now() < deadline {
            match TcpStream::connect(endpoint) {
                Ok(mut stream) => {
                    stream.write_all(&frame).map_err(Error::OutputWrite)?;

                    let mut header = [0_u8; TRANSPORT_HEADER_LEN];
                    stream.read_exact(&mut header).map_err(Error::InputRead)?;
                    let (kind, payload_len) = parse_transport_header(&header)
                        .map_err(|err| Error::InvalidTransportFrame(err.to_string()))?;
                    if kind != TransportFrameKind::WorkerCapabilities {
                        return Err(Error::WorkerHandshake(format!(
                            "unexpected worker response frame: {:?}",
                            kind
                        )));
                    }

                    let mut payload = vec![0_u8; payload_len];
                    stream.read_exact(&mut payload).map_err(Error::InputRead)?;
                    return WorkerCapabilities::decode_wire(&payload)
                        .map_err(|err| Error::WorkerHandshake(err.to_string()));
                }
                Err(err) => {
                    last_err = Some(err.to_string());
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }

        Err(Error::WorkerHandshake(format!(
            "timed out waiting for worker {} ({})",
            endpoint,
            last_err.unwrap_or_else(|| "no response".to_string())
        )))
    }

    /// Send a graceful-shutdown signal to every registered worker.
    ///
    /// Not called automatically — reserved for an explicit `atomic stop` command.
    /// Send a graceful-shutdown signal to every registered worker.
    ///
    /// Not called automatically — reserved for an explicit `atomic stop` command.
    #[allow(dead_code)]
    pub fn drop_executors(address_map: &[SocketAddrV4]) {
        if address_map.is_empty() {
            return;
        }

        for socket_addr in address_map {
            log::debug!(
                "dropping executor in {:?}:{:?}",
                socket_addr.ip(),
                socket_addr.port()
            );
            if let Ok(mut stream) =
                TcpStream::connect(format!("{}:{}", socket_addr.ip(), socket_addr.port() + 10))
            {
                let json = serde_json::to_vec(&Signal::ShutDownGracefully).unwrap();
                let mut signal = (json.len() as u32).to_le_bytes().to_vec();
                signal.extend_from_slice(&json);
                if let Err(e) = stream.write_all(&signal) {
                    error!("Failed to send shutdown signal: {}", e);
                }
            } else {
                error!(
                    "Failed to connect to {}:{} in order to stop its executor",
                    socket_addr.ip(),
                    socket_addr.port()
                );
            }
        }
    }

    pub fn new_rdd_id(self: &Arc<Self>) -> usize {
        self.next_rdd_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn new_shuffle_id(self: &Arc<Self>) -> usize {
        self.next_shuffle_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Default number of output partitions for wide transformations (reduce_by_key, group_by_key).
    /// Uses the number of CPUs, clamped to a sensible range.
    pub fn default_parallelism(self: &Arc<Self>) -> usize {
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

    pub fn run_job<T: Data, U: Data + Clone, F>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
    ) -> Result<Vec<U>, Error>
    where
        F: Fn(Box<dyn Iterator<Item = T>>) -> U + Send + Sync + 'static,
    {
        // Closures cannot be sent to remote workers; always execute on the driver
        // using the dedicated local scheduler, regardless of deployment mode.
        let cl = move |(_task_context, iter)| (func)(iter);
        self.driver_scheduler
            .clone()
            .run_job(
                Arc::new(cl),
                rdd.clone(),
                (0..rdd.number_of_splits()).collect(),
                false,
            )
            .map_err(Error::from)
    }

    pub fn run_job_with_partitions<T: Data, U: Data + Clone, F, P>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
        partitions: P,
    ) -> Result<Vec<U>, Error>
    where
        F: Fn(Box<dyn Iterator<Item = T>>) -> U + Send + Sync + 'static,
        P: IntoIterator<Item = usize>,
    {
        let cl = move |(_task_context, iter)| (func)(iter);
        self.driver_scheduler
            .clone()
            .run_job(Arc::new(cl), rdd, partitions.into_iter().collect(), false)
            .map_err(Error::from)
    }

    pub fn run_job_with_context<T: Data, U: Data + Clone, F>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
    ) -> Result<Vec<U>, Error>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
    {
        log::debug!("inside run job in context");
        let func = Arc::new(func);
        self.driver_scheduler
            .clone()
            .run_job(
                func,
                rdd.clone(),
                (0..rdd.number_of_splits()).collect(),
                false,
            )
            .map_err(Error::from)
    }

    pub fn run_approximate_job<T: Data, U: Data + Clone, R, F, E>(
        self: &Arc<Self>,
        func: F,
        rdd: Arc<dyn Rdd<Item = T>>,
        evaluator: E,
        timeout: Duration,
    ) -> Result<PartialResult<R>, Error>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
        E: ApproximateEvaluator<U, R> + Send + Sync + 'static,
        R: Clone + Debug + Send + Sync + 'static,
    {
        self.scheduler
            .run_approximate_job(Arc::new(func), rdd, evaluator, timeout)
            .map_err(Error::from)
    }

    pub fn union<T: Data + Clone>(
        self: &Arc<Self>,
        rdds: &[Arc<dyn Rdd<Item = T>>],
    ) -> std::result::Result<UnionRdd<T>, Error> {
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
    ) -> Result<Vec<Vec<U>>, Error>
    where
        T: Data + Clone + WireEncode,
        Vec<T>: WireEncode,
        U: Data + Clone + WireDecode,
        Vec<U>: WireDecode,
    {
        let ops = vec![PipelineOp { op_id: op_id.to_string(), action, payload }];
        let encoded = Self::encode_rdd_partitions(rdd)?;
        let result_bytes = self.dispatch_pipeline(encoded, ops)?;
        result_bytes
            .into_iter()
            .map(|bytes| Vec::<U>::decode_wire(&bytes).map_err(Error::from))
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
    ) -> Result<T, Error>
    where
        T: Data + Clone + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        let payload = zero.encode_wire()?;
        let ops = vec![PipelineOp { op_id: op_id.to_string(), action: TaskAction::Fold, payload }];
        let encoded = Self::encode_rdd_partitions(rdd)?;
        let partition_results_raw = self.dispatch_pipeline(encoded, ops.clone())?;

        let mut partition_values: Vec<T> = partition_results_raw
            .into_iter()
            .map(|bytes| T::decode_wire(&bytes).map_err(Error::from))
            .collect::<Result<_, _>>()?;

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
            payload: vec![],
        }];
        let task = TaskEnvelope::new(
            0, 0, 0, 0, 0,
            format!("driver-reduce-{}", op_id),
            reduce_ops,
            combined_data,
        );
        let result = NativeBackend.execute("local-driver", &task)?;
        match result.status {
            ResultStatus::Success => T::decode_wire(&result.data).map_err(Error::from),
            _ => Err(Error::InvalidPayload(
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
    ) -> Result<Vec<Vec<u8>>, Error> {
        match &self.scheduler {
            Schedulers::Local(_) => {
                let backend = NativeBackend;
                source_partitions
                    .into_iter()
                    .enumerate()
                    .map(|(part_id, data)| {
                        let task = TaskEnvelope::new(
                            0, 0, part_id, 0, part_id,
                            format!("local-pipeline-{}", part_id),
                            ops.clone(),
                            data,
                        );
                        let result = backend.execute("local-driver", &task)?;
                        match result.status {
                            ResultStatus::Success => Ok(result.data),
                            _ => Err(Error::InvalidPayload(
                                result.error.unwrap_or_else(|| "task failed".to_string()),
                            )),
                        }
                    })
                    .collect()
            }
            Schedulers::Distributed(sched) => {
                env::Env::run_in_async_rt(|| {
                    futures::executor::block_on(sched.run_native_job(ops, source_partitions))
                })
                .map_err(Error::from)
            }
        }
    }

    /// Encode every partition of an RDD into rkyv bytes.
    pub(crate) fn encode_rdd_partitions<T>(rdd: Arc<dyn Rdd<Item = T>>) -> Result<Vec<Vec<u8>>, Error>
    where
        T: Data + Clone + WireEncode,
        Vec<T>: WireEncode,
    {
        rdd.splits()
            .iter()
            .map(|split| {
                let items: Vec<T> = rdd.compute(split.clone())?.collect();
                items.encode_wire().map_err(Error::from)
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
    ) -> Result<(), Error> {
        use atomic_data::dependency::Dependency;

        let sched = match &self.scheduler {
            Schedulers::Distributed(s) => s.clone(),
            Schedulers::Local(_) => return Ok(()), // local mode: no-op, DAG handles it
        };

        for dep in rdd.get_dependencies() {
            if let Dependency::Shuffle(shuffle_dep) = dep {
                // Encode the parent RDD partitions (the shuffle map input).
                // The typed closure on ShuffleDependencyBox rkyv-encodes Vec<(K,V)> per partition.
                let parent_partitions = (shuffle_dep.encode_partitions)()
                    .map_err(Error::InvalidPayload)?;

                // Build the shuffle-map op. The payload carries the type_id string so
                // the worker can look up the correct SHUFFLE_MAP_REGISTRY handler.
                let shuffle_op = PipelineOp {
                    op_id: format!("shuffle-map-{}", shuffle_dep.shuffle_id),
                    action: TaskAction::ShuffleMap {
                        shuffle_id: shuffle_dep.shuffle_id,
                        num_output_partitions: shuffle_dep.num_output_partitions,
                    },
                    payload: shuffle_dep.type_id.as_bytes().to_vec(),
                };

                let mut ops = preceding_ops.clone();
                ops.push(shuffle_op);

                env::Env::run_in_async_rt(|| {
                    futures::executor::block_on(sched.run_shuffle_map_stage(
                        shuffle_dep.shuffle_id,
                        ops,
                        parent_partitions,
                    ))
                })
                .map_err(Error::from)?;

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

    let result = config
        .worker
        .as_ref()
        .map(|w| (w.port, w.max_concurrent_tasks))
        .ok_or(Error::GetOrCreateConfig(
            "start_worker called without a WorkerConfig — use Config::worker(ip, port)",
        ))
        .and_then(|(port, max_tasks)| {
            let executor = Arc::new(Executor::new(port, max_tasks));
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
