//! Gated end-to-end test for per-job Kubernetes worker allocation
//! (`Context::with_workers` with `allocator: kube`).
//!
//! Compiles only with `--features k8s` and is `#[ignore]`d: it needs a reachable
//! cluster and a worker image containing this binary (the worker pods run that image
//! in `--worker` mode, and the registry fingerprint must match the driver).
//!
//! Manual run:
//! ```bash
//! kind create cluster
//! # build an image whose entrypoint runs this crate's worker mode, then:
//! kind load docker-image <img>
//! export ATOMIC_K8S_WORKER_IMAGE=<img>
//! export ATOMIC_LOCAL_IP=<ip the worker pods can reach the driver on>
//! cargo test -p atomic-compute --features k8s --test test_k8s_allocation \
//!     -- --ignored --nocapture
//! # cleanup check (should print nothing):
//! kubectl get pods -l atomic.dev/role=worker
//! ```
#![cfg(feature = "k8s")]

use std::net::Ipv4Addr;

use atomic_compute::context::Context;
use atomic_compute::env::{AllocatorKind, Config, DeploymentMode};
use atomic_compute::{ResourceProfile, task};

#[task]
fn times_ten(x: i32) -> i32 {
    x * 10
}

#[test]
#[ignore = "requires a Kubernetes cluster + a worker image (see module docs)"]
fn with_workers_kube_e2e() {
    let worker_image = std::env::var("ATOMIC_K8S_WORKER_IMAGE")
        .expect("set ATOMIC_K8S_WORKER_IMAGE to the worker pod image");
    let local_ip: Ipv4Addr = std::env::var("ATOMIC_LOCAL_IP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(Ipv4Addr::LOCALHOST);

    // A kube-allocator driver starts with no standing workers and provisions pods per job.
    let mut config = Config::distributed_driver(local_ip, vec![]);
    config.allocator = AllocatorKind::Kube;
    config.kube.worker_image = worker_image;
    config.kube.namespace =
        std::env::var("ATOMIC_K8S_NAMESPACE").unwrap_or_else(|_| "default".to_string());
    assert_eq!(config.mode, DeploymentMode::Distributed);

    let ctx = Context::new_with_config(config)
        .expect("kube driver should start with an empty worker pool");

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut result = runtime
        .block_on(
            ctx.with_workers(
                ResourceProfile::new(2)
                    .with_cpu("500m", "1")
                    .with_memory("256Mi", "512Mi"),
                |sc| {
                    let out = sc
                        .parallelize_typed(vec![1, 2, 3, 4], 2)
                        .map_task(TimesTen)
                        .collect()?;
                    Ok(out)
                },
            ),
        )
        .expect("with_workers job should succeed on dedicated pods");

    result.sort();
    assert_eq!(result, vec![10, 20, 30, 40]);
}
