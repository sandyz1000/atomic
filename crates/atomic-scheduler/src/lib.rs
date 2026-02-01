pub mod base;
pub mod dag;
pub mod distributed;
pub mod error;
pub mod job;
pub mod listener;
pub mod local;
pub mod stage;

use atomic_data::partial::{ApproximateEvaluator, result::PartialResult};
use atomic_data::{task_context::TaskContext, data::Data, rdd::Rdd};
use std::sync::Arc;

pub use crate::{distributed::DistributedScheduler, error::LibResult, local::LocalScheduler, base::NativeScheduler};

pub trait Scheduler {
    fn start(&self);

    fn wait_for_register(&self);

    fn run_job<T: Data, U: Data, F>(
        &self,
        rdd: &dyn Rdd<Item = T>,
        func: F,
        partitions: Vec<i64>,
        allow_local: bool,
    ) -> Vec<U>
    where
        F: Fn(Box<dyn Iterator<Item = T>>) -> U;

    fn stop(&self);

    fn default_parallelism(&self) -> i64;
}

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
    pub fn run_job<T: Data, U: Data + Clone, F>(
        &self,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> LibResult<Vec<U>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + Send + Sync + 'static,
    {
        let op_name = final_rdd.get_op_name();
        log::info!("starting `{}` job", op_name);
        let start = std::time::Instant::now();
        match self {
            Schedulers::Distributed(distributed) => {
                let res = distributed
                    .clone()
                    .run_job(func, final_rdd, partitions, allow_local);
                log::info!(
                    "`{}` job finished, took {}s",
                    op_name,
                    start.elapsed().as_secs()
                );
                res
            }
            Schedulers::Local(local) => {
                let res = local
                    .clone()
                    .run_job(func, final_rdd, partitions, allow_local);
                log::info!(
                    "`{}` job finished, took {}s",
                    op_name,
                    start.elapsed().as_secs()
                );
                res
            }
        }
    }

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
