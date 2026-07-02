//! Kubernetes worker allocator for Atomic: provisions dedicated worker pods per job
//! and tears them down when the job finishes.
//!
//! Implements [`atomic_scheduler::WorkerAllocator`] against the Kubernetes API
//! (kube-rs). [`KubeWorkerAllocator::allocate`] creates one pod per requested worker
//! sized to the job's [`ResourceProfile`], waits until each is `Running`+`Ready`, and
//! returns their endpoints; [`KubeWorkerAllocator::release`] deletes them by label.
//!
//! Driver-crash cleanup is handled by Kubernetes garbage collection: each worker pod
//! carries an `OwnerReference` to the driver pod (see [`DriverOwner`]), so deleting
//! the driver cascades to its workers. Bare-process drivers (no owner) must rely on
//! `release` or manual cleanup of `atomic.dev/role=worker` pods.

mod driver_job;
mod pod_spec;

use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use atomic_scheduler::{AllocatorError, AllocatorResult, ResourceProfile, WorkerAllocator};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{DeleteParams, ListParams, PostParams};
use kube::{Api, Client};

pub use driver_job::{DriverJobSpec, InitFetch, build_driver_job};
pub use pod_spec::{ALLOC_ID_LABEL, DriverOwner, ROLE_LABEL};
use pod_spec::{PodTemplate, build_pod, ready_ip};

/// How often to poll a pod's status while waiting for it to become ready.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Static configuration for the allocator — everything except the per-job
/// [`ResourceProfile`].
#[derive(Debug, Clone)]
pub struct KubeConfig {
    pub namespace: String,
    pub worker_image: String,
    pub service_account: Option<String>,
    pub task_port: u16,
    pub ready_timeout: Duration,
    /// Container entrypoint. Worker flags (`--worker --port <task_port>`) are appended
    /// as args. Empty uses the image's own entrypoint.
    pub command: Vec<String>,
    /// Driver pod for `OwnerReference` GC. `None` for a bare-process driver.
    pub owner: Option<DriverOwner>,
}

pub struct KubeWorkerAllocator {
    /// Lazily initialized on first use so the constructor can stay synchronous and
    /// the driver only contacts the API server when a job actually allocates workers.
    client: tokio::sync::OnceCell<Client>,
    config: KubeConfig,
}

impl KubeWorkerAllocator {
    /// Build an allocator. The Kubernetes client (ambient in-cluster service account,
    /// or `~/.kube/config` locally) is created on first `allocate`/`release`.
    pub fn new(config: KubeConfig) -> Self {
        Self {
            client: tokio::sync::OnceCell::new(),
            config,
        }
    }

    async fn client(&self) -> AllocatorResult<&Client> {
        self.client
            .get_or_try_init(|| async {
                Client::try_default()
                    .await
                    .map_err(|e| AllocatorError::Provision(format!("kube client init: {e}")))
            })
            .await
    }

    async fn pods(&self) -> AllocatorResult<Api<Pod>> {
        Ok(Api::namespaced(
            self.client().await?.clone(),
            &self.config.namespace,
        ))
    }

    fn template(&self) -> PodTemplate<'_> {
        PodTemplate {
            namespace: &self.config.namespace,
            image: &self.config.worker_image,
            service_account: self.config.service_account.as_deref(),
            task_port: self.config.task_port,
            command: &self.config.command,
            owner: self.config.owner.as_ref(),
        }
    }

    /// Poll `pod_name` until it is ready and return its endpoint, or error on timeout.
    async fn await_ready(
        &self,
        pod_name: &str,
        deadline: Instant,
    ) -> AllocatorResult<SocketAddrV4> {
        let pods = self.pods().await?;
        loop {
            let pod = pods
                .get(pod_name)
                .await
                .map_err(|e| AllocatorError::Provision(format!("get pod {pod_name}: {e}")))?;
            if let Some(ip) = ready_ip(&pod) {
                let addr: Ipv4Addr = ip
                    .parse()
                    .map_err(|e| AllocatorError::Provision(format!("bad pod IP {ip}: {e}")))?;
                return Ok(SocketAddrV4::new(addr, self.config.task_port));
            }
            if Instant::now() >= deadline {
                return Err(AllocatorError::Timeout(self.config.ready_timeout));
            }
            tokio::time::sleep(READY_POLL_INTERVAL).await;
        }
    }
}

#[async_trait]
impl WorkerAllocator for KubeWorkerAllocator {
    async fn allocate(
        &self,
        alloc_id: &str,
        profile: &ResourceProfile,
    ) -> AllocatorResult<Vec<SocketAddrV4>> {
        let count = profile.count.max(1);
        let pods = self.pods().await?;
        let template = self.template();
        let post = PostParams::default();

        let mut names = Vec::with_capacity(count);
        for index in 0..count {
            let pod = build_pod(&template, alloc_id, index, profile)?;
            match pods.create(&post, &pod).await {
                Ok(created) => {
                    if let Some(name) = created.metadata.name {
                        names.push(name);
                    }
                }
                Err(e) => {
                    // Roll back partially-created pods so a failed allocation leaves nothing.
                    let _ = self.release(alloc_id).await;
                    return Err(AllocatorError::Provision(format!("create worker pod: {e}")));
                }
            }
        }

        let deadline = Instant::now() + self.config.ready_timeout;
        let mut endpoints = Vec::with_capacity(names.len());
        for name in &names {
            match self.await_ready(name, deadline).await {
                Ok(endpoint) => endpoints.push(endpoint),
                Err(e) => {
                    let _ = self.release(alloc_id).await;
                    return Err(e);
                }
            }
        }
        Ok(endpoints)
    }

    async fn release(&self, alloc_id: &str) -> AllocatorResult<()> {
        let selector = format!("{ALLOC_ID_LABEL}={alloc_id}");
        let list = ListParams::default().labels(&selector);
        self.pods()
            .await?
            .delete_collection(&DeleteParams::default(), &list)
            .await
            .map_err(|e| AllocatorError::Release(format!("delete worker pods: {e}")))?;
        Ok(())
    }
}
