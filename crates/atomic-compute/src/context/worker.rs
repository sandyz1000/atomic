use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use atomic_data::distributed::{
    TRANSPORT_HEADER_LEN, TransportFrameKind, WireDecode, WorkerCapabilities,
    encode_transport_frame, parse_transport_header,
};
use atomic_scheduler::DistributedScheduler;
use uuid::Uuid;

use crate::env::Config;
use crate::error::{ComputeError, ComputeResult};
use crate::executor::{Executor, Signal};
use crate::{env, task_registry};
use atomic_scheduler::LocalScheduler;
use atomic_utils::clean_up_work_dir;

use super::Context;

impl Context {
    pub(super) fn set_cleanup_process(&self) {
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

    pub(super) fn init_local_scheduler(config: Config) -> ComputeResult<Arc<Self>> {
        let job_id = Uuid::new_v4().to_string();
        let job_work_dir = config.work_dir.join(format!("ns-session-{}", job_id));
        std::fs::create_dir_all(&job_work_dir).map_err(ComputeError::OutputWrite)?;

        let _ = env_logger::try_init();
        atomic_data::cache::init_partition_cache();
        let spill_dir = job_work_dir.join("rdd-cache");
        std::fs::create_dir_all(&spill_dir).ok();
        atomic_data::env::set_rdd_cache_spill_dir(spill_dir);
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
        let scheduler = atomic_scheduler::Schedulers::Local(local.clone());

        Ok(Arc::new(Context {
            config,
            scheduler,
            driver_scheduler: local,
            next_rdd_id: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            address_map: vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)],
            distributed_driver: false,
            work_dir: job_work_dir,
            broadcast_store: Arc::new(dashmap::DashMap::new()),
            accumulator_store: Arc::new(dashmap::DashMap::new()),
            allocator: None,
            scoped: false,
            active_shuffle_stages: Arc::new(dashmap::DashMap::new()),
        }))
    }

    /// Connect to all registered workers and return a distributed driver context.
    pub(super) fn init_distributed_driver(config: Config) -> ComputeResult<Arc<Self>> {
        let job_id = Uuid::new_v4().to_string();
        let job_work_dir = config.work_dir.join(format!("ns-session-{}", job_id));

        std::fs::create_dir_all(&job_work_dir).unwrap();
        let _ = env_logger::try_init();
        atomic_data::cache::init_partition_cache();

        let config = Arc::new(config);

        if let Err(e) = env::Env::run_in_async_rt(|| env::init_shuffle(&config)) {
            log::warn!("shuffle service could not start: {e}");
        }

        let mut dist_sched = DistributedScheduler::new(20, true)
            .with_driver_fingerprint(*task_registry::REGISTRY_FINGERPRINT);
        if let Some(m) = config.speculation_multiplier {
            dist_sched = dist_sched.with_speculation(m);
        }
        if let Some(secs) = config.agent_step_timeout_secs {
            dist_sched = dist_sched.with_agent_step_timeout(std::time::Duration::from_secs(secs));
        }
        let scheduler = Arc::new(dist_sched);
        let mut address_map = Vec::new();

        for &endpoint in &config.workers {
            log::info!("connecting to worker at {}", endpoint);
            match Context::probe_worker(endpoint) {
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

        // The Kubernetes allocator starts with no standing workers and provisions
        // dedicated pods per job, so an empty pool is valid in that mode only.
        if address_map.is_empty() && config.allocator != crate::env::AllocatorKind::Kube {
            return Err(ComputeError::WorkerHandshake(
                "no reachable workers found".to_string(),
            ));
        }

        env::Env::run_in_async_rt(|| {
            scheduler.start_heartbeat(config.heartbeat_interval_secs, config.heartbeat_timeout_ms);
        });

        if let Some((host, port)) = config.worker_dns.clone() {
            let interval = config.heartbeat_interval_secs.max(10);
            let disc_sched = Arc::clone(&scheduler);
            env::Env::run_in_async_rt(move || {
                Context::start_worker_discovery(disc_sched, host, port, interval);
            });
        }

        if let Some(port) = config.register_port {
            env::Env::run_in_async_rt(|| {
                atomic_scheduler::start_register_server(port, Arc::clone(&scheduler));
            });
            log::info!("worker registration endpoint started on port {port}");
        }

        let driver_scheduler = Arc::new(LocalScheduler::new_with_coalesce(
            20,
            false,
            config.coalesce_shuffle_threshold_bytes,
        ));
        let active_shuffle_stages: super::ActiveShuffleStages = Arc::new(dashmap::DashMap::new());
        Context::install_map_output_recovery(
            &driver_scheduler,
            Arc::clone(&scheduler),
            Arc::clone(&active_shuffle_stages),
        );

        let accumulator_store: super::AccumulatorStore = Arc::new(dashmap::DashMap::new());
        let sink_store = Arc::clone(&accumulator_store);
        scheduler.set_accumulator_sink(Arc::new(move |deltas| {
            super::broadcast::merge_deltas_into(&sink_store, deltas);
        }));
        let scheduler = atomic_scheduler::Schedulers::Distributed(scheduler);

        let allocator = Context::build_allocator(&config, &address_map);

        Ok(Arc::new(Context {
            config,
            scheduler,
            driver_scheduler,
            next_rdd_id: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            address_map,
            distributed_driver: true,
            work_dir: job_work_dir,
            broadcast_store: Arc::new(dashmap::DashMap::new()),
            accumulator_store,
            allocator: Some(allocator),
            scoped: false,
            active_shuffle_stages,
        }))
    }

    /// Select the worker allocator for `Context::with_workers` from `config`.
    /// `Static` scopes jobs over the existing pool; `Kube` provisions dedicated pods.
    fn build_allocator(
        config: &Config,
        address_map: &[SocketAddrV4],
    ) -> Arc<dyn atomic_scheduler::WorkerAllocator> {
        match config.allocator {
            crate::env::AllocatorKind::Kube => Context::build_kube_allocator(config, address_map),
            crate::env::AllocatorKind::Static => {
                Arc::new(atomic_scheduler::StaticAllocator::new(address_map.to_vec()))
            }
        }
    }

    crate::cfg_k8s! {
    fn build_kube_allocator(
        config: &Config,
        _address_map: &[SocketAddrV4],
    ) -> Arc<dyn atomic_scheduler::WorkerAllocator> {
        // Driver pod identity (downward API) becomes the OwnerReference on worker
        // pods so Kubernetes garbage-collects them if the driver dies.
        let owner = match (std::env::var("POD_NAME"), std::env::var("POD_UID")) {
            (Ok(name), Ok(uid)) => Some(atomic_k8s::DriverOwner { name, uid }),
            _ => None,
        };
        let kube_config = atomic_k8s::KubeConfig {
            namespace: config.kube.namespace.clone(),
            worker_image: config.kube.worker_image.clone(),
            service_account: config.kube.service_account.clone(),
            task_port: config.kube.task_port,
            ready_timeout: Duration::from_secs(config.kube.ready_timeout_secs),
            command: config.kube.command.clone(),
            owner,
        };
        Arc::new(atomic_k8s::KubeWorkerAllocator::new(kube_config))
    }
    } // cfg_k8s!

    crate::cfg_not_k8s! {
    fn build_kube_allocator(
        _config: &Config,
        address_map: &[SocketAddrV4],
    ) -> Arc<dyn atomic_scheduler::WorkerAllocator> {
        log::error!(
            "Config requested the Kubernetes allocator but this binary was built without the \
             `k8s` feature; falling back to the static worker pool"
        );
        Arc::new(atomic_scheduler::StaticAllocator::new(address_map.to_vec()))
    }
    } // cfg_not_k8s!

    pub(super) fn worker_clean_up_directives(
        run_result: ComputeResult<Signal>,
        work_dir: PathBuf,
    ) -> ! {
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

    pub(crate) fn driver_clean_up_directives(
        work_dir: &std::path::Path,
        _executors: &[SocketAddrV4],
    ) {
        clean_up_work_dir(work_dir, true);
    }

    /// Periodically re-resolve `host:port` and register any endpoints that have appeared.
    pub(super) fn start_worker_discovery(
        scheduler: Arc<DistributedScheduler>,
        host: String,
        port: u16,
        interval_secs: u64,
    ) {
        tokio::spawn(async move {
            let interval = Duration::from_secs(interval_secs);
            loop {
                tokio::time::sleep(interval).await;
                let resolved = crate::app::resolve_worker_dns(&host, port);
                for endpoint in resolved {
                    if scheduler.is_worker_registered(&endpoint) {
                        continue;
                    }
                    let caps =
                        tokio::task::spawn_blocking(move || Context::probe_worker(endpoint)).await;
                    match caps {
                        Ok(Ok(capabilities)) => {
                            log::info!("discovery: new worker {endpoint} via DNS {host}:{port}");
                            scheduler.dynamically_add_worker(endpoint, capabilities);
                        }
                        Ok(Err(e)) => {
                            log::debug!("discovery: {endpoint} not ready yet: {e}");
                        }
                        Err(e) => log::warn!("discovery: handshake task panicked: {e}"),
                    }
                }
            }
        });
    }

    /// TCP handshake with a worker: send a `WorkerCapabilities` request frame and
    /// decode the response. Retries for up to 10 seconds on transient failures.
    pub(crate) fn probe_worker(endpoint: SocketAddrV4) -> ComputeResult<WorkerCapabilities> {
        let mut frame = Vec::new();
        if let Some(token) = atomic_data::env::get_auth_token() {
            frame.extend_from_slice(&encode_transport_frame(
                TransportFrameKind::Auth,
                token.as_bytes(),
            ));
        }
        frame.extend_from_slice(&encode_transport_frame(
            TransportFrameKind::WorkerCapabilities,
            &[],
        ));
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
    #[allow(dead_code)]
    pub(crate) fn drop_executors(address_map: &[SocketAddrV4]) {
        use log::error;
        for socket_addr in address_map {
            log::debug!(
                "dropping executor {}:{}",
                socket_addr.ip(),
                socket_addr.port()
            );
            let envelope = crate::executor::SignalEnvelope {
                token: atomic_data::env::get_auth_token(),
                signal: Signal::ShutDownGracefully,
            };
            match serde_json::to_vec(&envelope) {
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
}

/// Start the worker process.
///
/// Initialises the shuffle service from `config`, then enters the TCP
/// task-executor loop. This function **never returns** — it terminates the
/// process when the executor shuts down.
pub fn start_worker(config: Config) -> ! {
    let work_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_default();

    let _ = env_logger::try_init();

    if let Err(e) = env::Env::run_in_async_rt(|| env::init_shuffle(&config)) {
        log::warn!("shuffle service could not start on worker: {e}");
    }

    warmup_js();

    let result = config
        .worker
        .as_ref()
        .map(|w| (w.port, w.max_concurrent_tasks))
        .ok_or(ComputeError::GetOrCreateConfig(
            "start_worker called without a WorkerConfig — use Config::worker(ip, port)",
        ))
        .and_then(|(port, max_tasks)| {
            let executor = configure_tls(Executor::new(port, max_tasks), &config)?;
            let executor = Arc::new(executor);
            executor.worker()
        });

    Context::worker_clean_up_directives(result, work_dir)
}

crate::cfg_js! {
    fn warmup_js() {
        let n = num_cpus::get().max(1);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let handles: Vec<_> = (0..n)
                .map(|_| {
                    handle
                        .spawn_blocking(|| crate::runtimes::js::JsDispatcher::with_runtime(|_| {}))
                })
                .collect();
            for h in handles {
                let _ = tokio::task::block_in_place(|| handle.block_on(h));
            }
            log::info!("JS V8 runtime warmed up on {n} blocking threads");
        } else {
            log::debug!("JS V8 warmup skipped: no tokio runtime available");
        }
    }
}
crate::cfg_not_js! {
    fn warmup_js() {}
}

crate::cfg_tls! {
    fn configure_tls(executor: Executor, config: &Config) -> ComputeResult<Executor> {
        let ca_cert = config.tls_ca_cert.as_deref();
        let cert = config.tls_cert.as_deref();
        let key = config.tls_key.as_deref();
        if crate::tls::tls_is_configured(ca_cert, cert, key) {
            executor
                .with_tls(ca_cert.unwrap(), key.unwrap(), ca_cert.unwrap())
                .map_err(|e| {
                    ComputeError::GetOrCreateConfig(Box::leak(
                        format!("TLS init: {e}").into_boxed_str(),
                    ))
                })
        } else {
            Ok(executor)
        }
    }
}
crate::cfg_not_tls! {
    fn configure_tls(executor: Executor, _config: &Config) -> ComputeResult<Executor> {
        Ok(executor)
    }
}
