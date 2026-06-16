use std::{
    net::SocketAddrV4,
    sync::{
        Arc,
        atomic::{AtomicI16, Ordering},
    },
    time::Duration,
};

use atomic_data::distributed::{PipelineOp, TaskAction, WorkerCapabilities};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::{LibResult, SchedulerError};

use super::DistributedScheduler;

/// RAII guard that decrements a worker's inflight counter when dropped.
pub struct InflightGuard(pub(crate) Arc<AtomicI16>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl DistributedScheduler {
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
                        let probe = tokio::time::timeout(timeout, async {
                            let addr = format!("{}:{}", endpoint.ip(), port);
                            if let Ok(mut stream) = tokio::net::TcpStream::connect(&addr).await {
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
                        if count >= super::MAX_WORKER_FAILURES {
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
    pub(crate) fn worker_has_capability(&self, endpoint: &SocketAddrV4, cap: &str) -> bool {
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
    /// bincode-encoded `ShuffleMapPayload`.
    pub(crate) fn required_capability(op: &PipelineOp) -> String {
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
}
