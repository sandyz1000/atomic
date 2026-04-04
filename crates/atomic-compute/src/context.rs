use crate::error::Error;
use crate::executor::{Executor, Signal};
use crate::io::ReaderConfiguration;
use crate::rdd::typed::TypedRdd;
use crate::{env, hosts};
use atomic_data::distributed::{
    ArtifactDescriptor, ArtifactManifest, ExecutionBackend, RkyvWireSerializer,
    RkyvWireStrategy, RkyvWireValidator, TRANSPORT_HEADER_LEN, TransportFrameKind,
    WasmTaskPayload, WasmValueEncoding, WorkerCapabilities, WireDecode, WireEncode,
    encode_transport_frame, parse_transport_header,
};
use atomic_data::data::Data;
use atomic_data::partial::ApproximateEvaluator;
use atomic_data::partial::result::PartialResult;
use atomic_data::rdd::{Rdd, RddBase};
use atomic_data::task_context::TaskContext;
use atomic_scheduler::{DistributedScheduler, LocalScheduler, Schedulers};
use crate::rdd::{ParallelCollection, UnionRdd};
use atomic_utils::clean_up_work_dir;
use log::error;
use once_cell::sync::OnceCell;
use std::fmt::Debug;
use std::fs;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionRuntime {
    Local,
    Docker,
    Wasm,
}

impl Default for ExecutionRuntime {
    fn default() -> Self {
        Self::Local
    }
}

impl ExecutionRuntime {
    pub fn default_for(mode: env::DeploymentMode) -> Self {
        match mode {
            env::DeploymentMode::Local => Self::Local,
            env::DeploymentMode::Distributed => Self::Wasm,
        }
    }
}

impl From<ExecutionBackend> for ExecutionRuntime {
    fn from(value: ExecutionBackend) -> Self {
        match value {
            ExecutionBackend::LocalThread => Self::Local,
            ExecutionBackend::Docker => Self::Docker,
            ExecutionBackend::Wasm => Self::Wasm,
        }
    }
}

impl From<ExecutionRuntime> for ExecutionBackend {
    fn from(value: ExecutionRuntime) -> Self {
        match value {
            ExecutionRuntime::Local => ExecutionBackend::LocalThread,
            ExecutionRuntime::Docker => ExecutionBackend::Docker,
            ExecutionRuntime::Wasm => ExecutionBackend::Wasm,
        }
    }
}

pub struct Context {
    scheduler: Schedulers,
    execution_runtime: ExecutionRuntime,
    pub next_rdd_id: Arc<AtomicUsize>,
    pub next_shuffle_id: Arc<AtomicUsize>,
    pub address_map: Vec<SocketAddrV4>,
    pub distributed_driver: bool,
    /// this context/session temp work dir  
    pub work_dir: PathBuf,
}

// There is a problem with this approach since T needs to satisfy PartialEq, Eq for Range
// No such restrictions are needed for Vec

impl Drop for Context {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        {
            let deployment_mode = env::Configuration::get().deployment_mode;
            if self.distributed_driver && deployment_mode == env::DeploymentMode::Distributed {
                log::info!("inside context drop in master");
            } else if deployment_mode == env::DeploymentMode::Distributed {
                log::info!("inside context drop in executor");
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
    pub fn new() -> Result<Arc<Self>, Error> {
        let mode = env::Configuration::get().deployment_mode;
        Context::with_mode_and_runtime(mode, ExecutionRuntime::default_for(mode))
    }

    pub fn local() -> Result<Arc<Self>, Error> {
        Context::with_mode_and_runtime(env::DeploymentMode::Local, ExecutionRuntime::Local)
    }

    pub fn docker() -> Result<Arc<Self>, Error> {
        Context::with_mode_and_runtime(
            env::DeploymentMode::Distributed,
            ExecutionRuntime::Docker,
        )
    }

    pub fn wasm() -> Result<Arc<Self>, Error> {
        Context::with_mode_and_runtime(env::DeploymentMode::Distributed, ExecutionRuntime::Wasm)
    }

    /// Create a WASM context and load an artifact manifest in one step.
    pub fn wasm_from_manifest(path: impl AsRef<Path>) -> Result<Arc<Self>, Error> {
        let ctx = Self::wasm()?;
        ctx.load_artifact_manifest_toml(path)?;
        Ok(ctx)
    }

    /// Create a Docker context and load an artifact manifest in one step.
    pub fn docker_from_manifest(path: impl AsRef<Path>) -> Result<Arc<Self>, Error> {
        let ctx = Self::docker()?;
        ctx.load_artifact_manifest_toml(path)?;
        Ok(ctx)
    }

    pub fn with_runtime(runtime: ExecutionRuntime) -> Result<Arc<Self>, Error> {
        let mode = env::Configuration::get().deployment_mode;
        Context::with_mode_and_runtime(mode, runtime)
    }

    // TODO: This method should be moved to TypedRdd or removed
    // fn save_as_text_file(&self, path: String) -> Result<Vec<()>, Error> {
    //     let cl = move |(ctx, iter)| save::<Self::Item>(ctx, iter, path.to_string());
    //     self.run_job_with_context(self.get_rdd(), cl)
    // }

    pub fn with_mode(mode: env::DeploymentMode) -> Result<Arc<Self>, Error> {
        Context::with_mode_and_runtime(mode, ExecutionRuntime::default_for(mode))
    }

    pub fn with_mode_and_runtime(
        mode: env::DeploymentMode,
        runtime: ExecutionRuntime,
    ) -> Result<Arc<Self>, Error> {
        Context::validate_runtime(mode, runtime)?;
        match mode {
            env::DeploymentMode::Distributed => {
                if env::Configuration::get().is_driver {
                    let ctx = Context::init_distributed_driver(runtime)?;
                    ctx.set_cleanup_process();
                    Ok(ctx)
                } else {
                    Context::init_distributed_worker()?;
                    unreachable!("worker process always terminates via std::process::exit")
                }
            }
            env::DeploymentMode::Local => Context::init_local_scheduler(runtime),
        }
    }

    pub fn runtime(&self) -> ExecutionRuntime {
        self.execution_runtime
    }

    fn validate_runtime(
        mode: env::DeploymentMode,
        runtime: ExecutionRuntime,
    ) -> Result<(), Error> {
        match (mode, runtime) {
            (env::DeploymentMode::Local, ExecutionRuntime::Local)
            | (env::DeploymentMode::Distributed, ExecutionRuntime::Docker)
            | (env::DeploymentMode::Distributed, ExecutionRuntime::Wasm) => Ok(()),
            (env::DeploymentMode::Local, _) => Err(Error::UnsupportedOperation(
                "local deployment mode only supports the local runtime",
            )),
            (env::DeploymentMode::Distributed, ExecutionRuntime::Local) => {
                Err(Error::UnsupportedOperation(
                    "distributed deployment mode requires the docker or wasm runtime",
                ))
            }
        }
    }

    /// Sets a handler to receives any external signal to stop the process
    /// and shuts down gracefully any ongoing op
    fn set_cleanup_process(&self) {
        let address_map = self.address_map.clone();
        let work_dir = self.work_dir.clone();
        env::Env::run_in_async_rt(|| {
            tokio::spawn(async move {
                // avoid moving a self clone here or drop won't be potentially called
                // before termination and never clean up
                if tokio::signal::ctrl_c().await.is_ok() {
                    log::info!("received termination signal, cleaning up");
                    Context::driver_clean_up_directives(&work_dir, &address_map);
                    std::process::exit(0);
                }
            });
        })
    }

    fn init_local_scheduler(runtime: ExecutionRuntime) -> Result<Arc<Self>, Error> {
        let job_id = Uuid::new_v4().to_string();
        let job_work_dir = env::Configuration::get()
            .local_dir
            .join(format!("ns-session-{}", job_id));
        fs::create_dir_all(&job_work_dir).unwrap();

        let _ = env_logger::try_init();
        let scheduler = Schedulers::Local(Arc::new(LocalScheduler::new(20, true)));

        Ok(Arc::new(Context {
            scheduler,
            execution_runtime: runtime,
            next_rdd_id: Arc::new(AtomicUsize::new(0)),
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            address_map: vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)],
            distributed_driver: false,
            work_dir: job_work_dir,
        }))
    }

    /// Initialization function for the application driver.
    /// * Distributes the configuration setup to the workers.
    /// * Distributes a copy of the application binary to all the active worker host nodes.
    /// * Launches the workers in the remote machine using the same binary (required).
    /// * Creates and returns a working Context.
    fn init_distributed_driver(runtime: ExecutionRuntime) -> Result<Arc<Self>, Error> {
        let mut port: u16 = 10000;
        let mut address_map = Vec::new();
        let job_id = Uuid::new_v4().to_string();
        let job_work_dir = env::Configuration::get()
            .local_dir
            .join(format!("ns-session-{}", job_id));
        let job_work_dir_str = job_work_dir
            .to_str()
            .ok_or_else(|| Error::PathToString(job_work_dir.clone()))?;

        let binary_path = std::env::current_exe().map_err(|_| Error::CurrentBinaryPath)?;
        let binary_path_str = binary_path
            .to_str()
            .ok_or_else(|| Error::PathToString(binary_path.clone()))?
            .into();
        let binary_name = binary_path
            .file_name()
            .ok_or(Error::CurrentBinaryName)?
            .to_os_string()
            .into_string()
            .map_err(Error::OsStringToString)?;

        fs::create_dir_all(&job_work_dir).unwrap();
        let conf_path = job_work_dir.join("config.toml");
        let conf_path = conf_path.to_str().unwrap();
        let _ = env_logger::try_init();
        let scheduler = Arc::new(DistributedScheduler::new(20, true));

        for address in &hosts::Hosts::get()?.slaves {
            log::debug!("deploying executor at address {:?}", address);
            let address_ip: Ipv4Addr = address
                .split('@')
                .nth(1)
                .ok_or_else(|| Error::ParseHostAddress(address.into()))?
                .parse()
                .map_err(|x| Error::ParseHostAddress(format!("{}", x)))?;
            let endpoint = SocketAddrV4::new(address_ip, port);
            address_map.push(endpoint);

            // Create work dir:
            Command::new("ssh")
                .args(&[address, "mkdir", &job_work_dir_str])
                .output()
                .map_err(|e| Error::CommandOutput {
                    source: e,
                    command: "ssh mkdir".into(),
                })?;

            // Copy conf file to remote:
            Context::create_workers_config_file(address_ip, port, conf_path)?;
            let remote_path = format!("{}:{}/config.toml", address, job_work_dir_str);
            Command::new("scp")
                .args(&[conf_path, &remote_path])
                .output()
                .map_err(|e| Error::CommandOutput {
                    source: e,
                    command: "scp config".into(),
                })?;

            // Copy binary:
            let remote_path = format!("{}:{}/{}", address, job_work_dir_str, binary_name);
            Command::new("scp")
                .args(&[&binary_path_str, &remote_path])
                .output()
                .map_err(|e| Error::CommandOutput {
                    source: e,
                    command: "scp executor".into(),
                })?;

            // Deploy a remote slave:
            let path = format!("{}/{}", job_work_dir_str, binary_name);
            log::debug!("remote path {}", path);
            Command::new("ssh")
                .args(&[address, &path])
                .spawn()
                .map_err(|e| Error::CommandOutput {
                    source: e,
                    command: "ssh run".into(),
                })?;

            let capabilities = Context::request_worker_capabilities(endpoint)?;
            scheduler.register_worker(endpoint, capabilities);
            port += 5000;
        }

        let scheduler = Schedulers::Distributed(scheduler);

        Ok(Arc::new(Context {
            scheduler,
            execution_runtime: runtime,
            next_rdd_id: Arc::new(AtomicUsize::new(0)),
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            address_map,
            distributed_driver: true,
            work_dir: job_work_dir,
        }))
    }

    fn init_distributed_worker() -> Result<(), Error> {
        let mut work_dir = PathBuf::from("");
        match std::env::current_exe().map_err(|_| Error::CurrentBinaryPath) {
            Ok(binary_path) => {
                match binary_path.parent().ok_or_else(|| Error::CurrentBinaryPath) {
                    Ok(dir) => work_dir = dir.into(),
                    Err(err) => Context::worker_clean_up_directives(Err(err), work_dir),
                };
                let _ = env_logger::try_init();
            }
            Err(err) => Context::worker_clean_up_directives(Err(err), work_dir),
        }

        log::debug!("starting worker");
        let port = match env::Configuration::get()
            .slave
            .as_ref()
            .map(|c| c.port)
            .ok_or(Error::GetOrCreateConfig("executor port not set"))
        {
            Ok(port) => port,
            Err(err) => Context::worker_clean_up_directives(Err(err), work_dir),
        };
        let executor = Arc::new(Executor::new(port));
        Context::worker_clean_up_directives(executor.worker(), work_dir)
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

    fn driver_clean_up_directives(work_dir: &Path, executors: &[SocketAddrV4]) {
        Context::drop_executors(executors);
        // Give some time for the executors to shut down and clean up
        std::thread::sleep(std::time::Duration::from_millis(1_500));
        clean_up_work_dir(work_dir, true);
    }

    fn create_workers_config_file(
        local_ip: Ipv4Addr,
        port: u16,
        config_path: &str,
    ) -> Result<(), Error> {
        let mut current_config = env::Configuration::get().clone();
        current_config.local_ip = local_ip;
        current_config.slave = Some(std::convert::From::<(bool, u16)>::from((true, port)));
        current_config.is_driver = false;

        let config_string = toml::to_string_pretty(&current_config).unwrap();
        let mut config_file = fs::File::create(config_path).unwrap();
        config_file.write_all(config_string.as_bytes()).unwrap();
        Ok(())
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

    fn drop_executors(address_map: &[SocketAddrV4]) {
        if env::Configuration::get().deployment_mode.is_local() {
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
                // Serialize signal as 4-byte LE length + serde_json bytes
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
        // TODO: input validity check
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
    ///
    /// This is a convenience method that returns TypedRdd instead of Arc<dyn Rdd>.
    ///
    /// # Example
    /// ```ignore
    /// let rdd = ctx.parallelize_typed(vec![1, 2, 3, 4, 5], 2);
    /// let sum = rdd.reduce(|a, b| a + b)?;
    /// ```
    pub fn parallelize_typed<T: Data + Clone, I>(
        self: &Arc<Self>,
        seq: I,
        num_slices: usize,
    ) -> crate::rdd::TypedRdd<T>
    where
        I: IntoIterator<Item = T>,
    {
        let id = self.new_rdd_id();
        let rdd = Arc::new(ParallelCollection::new(id, seq, num_slices));
        crate::rdd::TypedRdd::new(rdd, self.clone())
    }

    /// Load from a distributed source and turns it into a parallel collection.
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
        let cl = move |(_task_context, iter)| (func)(iter);
        let func = Arc::new(cl);
        self.scheduler.run_job(
            func,
            rdd.clone(),
            (0..rdd.number_of_splits()).collect(),
            false,
        ).map_err(Error::from)
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
        self.scheduler
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
        self.scheduler.run_job(
            func,
            rdd.clone(),
            (0..rdd.number_of_splits()).collect(),
            false,
        ).map_err(Error::from)
    }

    pub fn run_registered_wasm_job(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = u8>>,
        operation_id: &str,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let partitions = (0..rdd.number_of_splits())
            .map(|partition| {
                rdd.get_rdd_base()
                    .wasm_bytes(partition)
                    .ok_or_else(|| {
                        Error::UnsupportedOperation(
                            "rdd does not support wasm byte materialization for this partition",
                        )
                    })?
                    .map_err(|err| Error::InvalidPayload(err.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        match &self.scheduler {
            Schedulers::Distributed(scheduler) => futures::executor::block_on(
                scheduler.run_registered_wasm_job(operation_id, partitions),
            )
            .map_err(|err| Error::InvalidPayload(err.to_string())),
            Schedulers::Local(_) => Err(Error::UnsupportedOperation(
                "registered wasm jobs require the distributed scheduler",
            )),
        }
    }

    pub fn run_registered_wasm_job_rkyv<T, U, C>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        operation_id: &str,
        config: &C,
    ) -> Result<Vec<U>, Error>
    where
        T: Data + Clone,
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        U: rkyv::Archive,
        U::Archived: for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
            + rkyv::Deserialize<U, RkyvWireStrategy>,
        C: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        let payload = WasmTaskPayload::with_encodings(
            WasmValueEncoding::Rkyv,
            config.encode_wire().map_err(|err| Error::InvalidPayload(err.to_string()))?,
            WasmValueEncoding::Rkyv,
            WasmValueEncoding::Rkyv,
        );

        let partitions = rdd
            .splits()
            .into_iter()
            .map(|split| {
                let values = rdd
                    .iterator(split)
                    .map_err(|err| Error::InvalidPayload(err.to_string()))?
                    .collect::<Vec<T>>();
                values.encode_wire().map_err(|err| Error::InvalidPayload(err.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let outputs = match &self.scheduler {
            Schedulers::Distributed(scheduler) => futures::executor::block_on(
                scheduler.run_registered_wasm_job_with_payload(operation_id, payload, partitions),
            )
            .map_err(|err| Error::InvalidPayload(err.to_string()))?,
            Schedulers::Local(_) => {
                return Err(Error::UnsupportedOperation(
                    "registered wasm jobs require the distributed scheduler",
                ));
            }
        };

        outputs
            .into_iter()
            .map(|bytes| {
                U::decode_wire(&bytes)
                    .map_err(|err| Error::InvalidPayload(err.to_string()))
            })
            .collect()
    }

    /// Run a job that can return approximate results. Returns a partial result
    /// (how partial depends on whether the job was finished before or after timeout).
    pub(crate) fn run_approximate_job<T: Data, U: Data + Clone, R, F, E>(
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

    pub fn register_artifact(&self, descriptor: ArtifactDescriptor) -> Result<(), Error> {
        match &self.scheduler {
            Schedulers::Distributed(scheduler) => {
                scheduler.register_artifact_descriptor(descriptor);
                Ok(())
            }
            Schedulers::Local(_) => Err(Error::UnsupportedOperation(
                "artifact registration requires the distributed scheduler",
            )),
        }
    }

    /// Register a typed artifact stub's descriptor with the scheduler.
    ///
    /// This is an alternative to loading the entire manifest: register only the
    /// specific stubs your pipeline uses.
    pub fn register_stub<I, O>(
        &self,
        stub: &atomic_data::stub::ArtifactStub<I, O>,
    ) -> Result<(), Error> {
        self.register_artifact(stub.descriptor.clone())
    }

    pub fn register_artifact_manifest(&self, manifest: ArtifactManifest) -> Result<(), Error> {
        match &self.scheduler {
            Schedulers::Distributed(scheduler) => {
                scheduler.register_artifact_manifest(manifest);
                Ok(())
            }
            Schedulers::Local(_) => Err(Error::UnsupportedOperation(
                "artifact registration requires the distributed scheduler",
            )),
        }
    }

    pub fn load_artifact_manifest_toml(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<(), Error> {
        match &self.scheduler {
            Schedulers::Distributed(scheduler) => scheduler
                .load_artifact_manifest_toml(path)
                .map_err(|err| Error::ArtifactLoad(err.to_string())),
            Schedulers::Local(_) => Err(Error::UnsupportedOperation(
                "artifact registration requires the distributed scheduler",
            )),
        }
    }

    pub(crate) fn resolve_registered_wasm_operation<I>(&self, candidates: I) -> Option<String>
    where
        I: IntoIterator<Item = String>,
    {
        match &self.scheduler {
            Schedulers::Distributed(scheduler) => candidates
                .into_iter()
                .find(|candidate| scheduler.resolve_artifact(candidate).is_ok()),
            Schedulers::Local(_) => None,
        }
    }

    pub fn union<T: Data + Clone>(self: &Arc<Self>, rdds: &[Arc<dyn Rdd<Item = T>>]) -> std::result::Result<UnionRdd<T>, Error> {
        UnionRdd::new(self.new_rdd_id(), rdds)
    }
}

#[cfg(test)]
mod tests {
    use super::{Context, ExecutionRuntime};
    use crate::env::DeploymentMode;

    #[test]
    fn local_mode_rejects_non_local_runtime() {
        let result = Context::with_mode_and_runtime(DeploymentMode::Local, ExecutionRuntime::Wasm);
        assert!(result.is_err());
    }

    #[test]
    fn distributed_mode_rejects_local_runtime() {
        let result =
            Context::with_mode_and_runtime(DeploymentMode::Distributed, ExecutionRuntime::Local);
        assert!(result.is_err());
    }
}

