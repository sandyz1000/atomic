//! Per-job worker resource profiles and the allocator that provisions workers to
//! match them.
//!
//! A [`WorkerAllocator`] turns a [`ResourceProfile`] into a set of ready worker
//! endpoints for one job and releases them when the job finishes. The default
//! [`StaticAllocator`] hands out endpoints from a fixed, already-running pool;
//! `atomic-k8s`'s `KubeWorkerAllocator` creates dedicated pods on demand.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddrV4;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum AllocatorError {
    #[error("worker provisioning failed: {0}")]
    Provision(String),
    #[error("workers not ready within {0:?}")]
    Timeout(Duration),
    #[error("worker release failed: {0}")]
    Release(String),
}

pub type AllocatorResult<T> = Result<T, AllocatorError>;

/// Resource shape for the dedicated workers a single job needs.
///
/// The Kubernetes-specific fields (`extended`, `node_selector`, `tolerations`,
/// `affinity`) are ignored by [`StaticAllocator`] and consulted only by the
/// Kubernetes allocator. `tolerations`/`affinity` are carried as raw JSON so this
/// crate stays free of `k8s-openapi`; the kube allocator deserializes them into the
/// corresponding API types.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceProfile {
    /// Number of workers to provision. `0` means "use the whole existing pool"
    /// (only meaningful for [`StaticAllocator`]).
    pub count: usize,
    pub cpu_request: Option<String>,
    pub cpu_limit: Option<String>,
    pub mem_request: Option<String>,
    pub mem_limit: Option<String>,
    /// Extended/scheduler resources, e.g. `{"nvidia.com/gpu": "1"}`.
    pub extended: BTreeMap<String, String>,
    pub node_selector: BTreeMap<String, String>,
    /// Raw JSON array of Kubernetes tolerations, passed through verbatim.
    pub tolerations: Option<serde_json::Value>,
    /// Raw JSON Kubernetes affinity object, passed through verbatim.
    pub affinity: Option<serde_json::Value>,
    /// Extra environment variables set on each worker.
    pub env: BTreeMap<String, String>,
    /// Extra labels applied to each worker pod.
    pub labels: BTreeMap<String, String>,
}

impl ResourceProfile {
    pub fn new(count: usize) -> Self {
        Self {
            count,
            ..Default::default()
        }
    }

    pub fn with_cpu(mut self, request: impl Into<String>, limit: impl Into<String>) -> Self {
        self.cpu_request = Some(request.into());
        self.cpu_limit = Some(limit.into());
        self
    }

    pub fn with_memory(mut self, request: impl Into<String>, limit: impl Into<String>) -> Self {
        self.mem_request = Some(request.into());
        self.mem_limit = Some(limit.into());
        self
    }

    pub fn with_gpu(mut self, count: u32) -> Self {
        self.extended
            .insert("nvidia.com/gpu".to_string(), count.to_string());
        self
    }

    pub fn with_node_selector(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.node_selector.insert(key.into(), value.into());
        self
    }
}

/// Provisions and releases the workers a job runs on.
///
/// Implementations must be `Send + Sync` so the driver can hold one behind an
/// `Arc<dyn WorkerAllocator>` across async tasks.
#[async_trait]
pub trait WorkerAllocator: Send + Sync {
    /// Provision workers for `profile` and return their ready endpoints. `alloc_id`
    /// uniquely identifies this allocation so [`release`](Self::release) can find it.
    async fn allocate(
        &self,
        alloc_id: &str,
        profile: &ResourceProfile,
    ) -> AllocatorResult<Vec<SocketAddrV4>>;

    /// Release every worker provisioned under `alloc_id`. Must be idempotent.
    async fn release(&self, alloc_id: &str) -> AllocatorResult<()>;
}

/// Default allocator over a fixed, already-running worker pool. `allocate` hands
/// back up to `profile.count` of the configured endpoints (all of them when
/// `count == 0`); `release` is a no-op because the pool outlives the job.
pub struct StaticAllocator {
    endpoints: Vec<SocketAddrV4>,
}

impl StaticAllocator {
    pub fn new(endpoints: Vec<SocketAddrV4>) -> Self {
        Self { endpoints }
    }
}

#[async_trait]
impl WorkerAllocator for StaticAllocator {
    async fn allocate(
        &self,
        _alloc_id: &str,
        profile: &ResourceProfile,
    ) -> AllocatorResult<Vec<SocketAddrV4>> {
        if self.endpoints.is_empty() {
            return Err(AllocatorError::Provision(
                "static allocator has no configured workers".to_string(),
            ));
        }
        let n = if profile.count == 0 {
            self.endpoints.len()
        } else {
            profile.count.min(self.endpoints.len())
        };
        Ok(self.endpoints[..n].to_vec())
    }

    async fn release(&self, _alloc_id: &str) -> AllocatorResult<()> {
        Ok(())
    }
}

/// Build a `VecDeque` placement queue from allocated endpoints (used by the
/// scheduler's scoped view). Kept here so the conversion lives next to the type.
pub(crate) fn placement_queue(endpoints: Vec<SocketAddrV4>) -> VecDeque<SocketAddrV4> {
    endpoints.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(n: u16) -> SocketAddrV4 {
        SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 10000 + n)
    }

    #[tokio::test]
    async fn static_returns_count() {
        let alloc = StaticAllocator::new(vec![ep(1), ep(2), ep(3)]);
        let got = alloc.allocate("a", &ResourceProfile::new(2)).await.unwrap();
        assert_eq!(got, vec![ep(1), ep(2)]);
    }

    #[tokio::test]
    async fn static_zero_means_all() {
        let alloc = StaticAllocator::new(vec![ep(1), ep(2)]);
        let got = alloc.allocate("a", &ResourceProfile::new(0)).await.unwrap();
        assert_eq!(got.len(), 2);
    }

    #[tokio::test]
    async fn static_clamps_overask() {
        let alloc = StaticAllocator::new(vec![ep(1)]);
        let got = alloc.allocate("a", &ResourceProfile::new(5)).await.unwrap();
        assert_eq!(got, vec![ep(1)]);
    }

    #[tokio::test]
    async fn static_empty_errors() {
        let alloc = StaticAllocator::new(vec![]);
        assert!(alloc.allocate("a", &ResourceProfile::new(1)).await.is_err());
    }

    #[test]
    fn profile_builders() {
        let p = ResourceProfile::new(3)
            .with_cpu("4", "8")
            .with_memory("8Gi", "16Gi")
            .with_gpu(2)
            .with_node_selector("pool", "gpu");
        assert_eq!(p.count, 3);
        assert_eq!(p.cpu_request.as_deref(), Some("4"));
        assert_eq!(
            p.extended.get("nvidia.com/gpu").map(String::as_str),
            Some("2")
        );
        assert_eq!(p.node_selector.get("pool").map(String::as_str), Some("gpu"));
    }
}
