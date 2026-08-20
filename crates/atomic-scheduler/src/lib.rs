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
use atomic_data::{data::Data, rdd::Rdd, task_context::PartitionTask};
use std::sync::Arc;

pub use crate::distributed::{
    ActiveShuffleStage, AllocatorError, AllocatorResult, RegisterRequest, ResourceProfile,
    StaticAllocator, WorkerAllocator, start_register_server,
};
pub use crate::job::PipelineJobData;
pub use crate::{base::NativeScheduler, error::LibResult, planner::StagePlanner};
pub use crate::{
    distributed::DistributedScheduler,
    local::{LocalScheduler, MapOutputRecovery},
};

#[derive(Clone)]
pub enum Schedulers {
    Local(Arc<LocalScheduler>),
    Distributed(Arc<DistributedScheduler>),
}

impl Default for Schedulers {
    fn default() -> Schedulers {
        Schedulers::Local(Arc::new(LocalScheduler::new(20)))
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
        F: PartitionTask<T, U>,
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

    /// Dispatch a `Vec<Step>` pipeline job — `Context::dispatch_pipeline`'s one entry point
    /// into the `Stage`-tracked scheduling layer, regardless of mode. `final_rdd` supplies
    /// only the shape the planner walks for shuffle boundaries (its `RddBase`/split count);
    /// no closure is involved. `Local`'s `submit_task` runs each task on a blocking thread;
    /// `Distributed`'s ships it to a worker — that's the only difference between the two.
    pub fn run_pipeline_job<T: Data>(
        &self,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        pipeline_data: crate::job::PipelineJobData,
        partitions: Vec<usize>,
    ) -> LibResult<Vec<Vec<u8>>> {
        match self {
            Schedulers::Local(local) => {
                local
                    .clone()
                    .run_pipeline_job(final_rdd, pipeline_data, partitions)
            }
            Schedulers::Distributed(distributed) => futures::executor::block_on(
                distributed
                    .clone()
                    .run_pipeline_job(final_rdd, pipeline_data, partitions),
            ),
        }
    }
}
