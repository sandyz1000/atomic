//! Job scheduling for the Atomic engine.
//!
//! Two schedulers are available:
//!
//! - [`LocalScheduler`] — runs every partition on a thread pool in the current process.
//!   Used when `Config::local()` is set or no workers are registered.
//! - [`DistributedScheduler`] — dispatches [`TaskEnvelope`](atomic_data::distributed::TaskEnvelope)s
//!   to remote worker processes over TCP. Handles stage splitting at shuffle boundaries,
//!   speculative execution, adaptive shuffle coalescing, and heartbeat-based worker health.
//!
//! Select via [`Schedulers`], which wraps both behind a common `run_job` interface.
//!
//! # Kubernetes allocation
//!
//! [`WorkerAllocator`] is the trait for on-demand worker provisioning.
//! [`StaticAllocator`] is the default (uses a fixed worker list).
//! The `atomic-k8s` crate provides [`KubeWorkerAllocator`](atomic_k8s::KubeWorkerAllocator)
//! for per-job pod creation.

pub mod base;
pub mod dag;
pub mod distributed;
pub mod error;
pub mod job;
pub mod listener;
pub mod local;
pub mod metrics;
pub mod planner;
pub mod stage;

use atomic_data::partial::{ApproximateEvaluator, result::PartialResult};
use atomic_data::{data::Data, rdd::Rdd, task_context::TaskContext};
use std::sync::Arc;

pub use crate::distributed::{
    AllocatorError, AllocatorResult, RegisterRequest, ResourceProfile, StaticAllocator,
    WorkerAllocator, start_register_server,
};
pub use crate::{base::NativeScheduler, error::LibResult, planner::StagePlanner};
pub use crate::{
    distributed::DistributedScheduler,
    local::{LocalScheduler, MapOutputRecovery},
};

pub enum Sequence<T> {
    Range(std::ops::Range<T>),
    Vec(Vec<T>),
}

#[derive(Clone)]
pub enum Schedulers {
    Local(Arc<LocalScheduler>),
    Distributed(Arc<DistributedScheduler>),
}

impl Default for Schedulers {
    fn default() -> Schedulers {
        Schedulers::Local(Arc::new(LocalScheduler::new(20, true)))
    }
}

impl Schedulers {
    pub fn run_approximate_job<T: Data, U: Data + Clone, R, F, E>(
        &self,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        evaluator: E,
        timeout: std::time::Duration,
    ) -> LibResult<PartialResult<R>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
        E: ApproximateEvaluator<U, R> + Send + Sync + 'static,
        R: Clone + std::fmt::Debug + Send + Sync + 'static,
    {
        let op_name = final_rdd.get_op_name();
        log::info!("starting `{}` job", op_name);
        let start = std::time::Instant::now();
        let res = match self {
            Schedulers::Distributed(distributed) => distributed
                .clone()
                .run_approximate_job(func, final_rdd, evaluator, timeout),
            Schedulers::Local(local) => local
                .clone()
                .run_approximate_job(func, final_rdd, evaluator, timeout),
        };
        log::info!(
            "`{}` job finished, took {}s",
            op_name,
            start.elapsed().as_secs()
        );
        res
    }
}
