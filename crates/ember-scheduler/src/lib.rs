use std::sync::Arc;

pub enum Sequence<T> {
    Range(Range<T>),
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
    fn run_job<T: Data, U: Data, F>(
        &self,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> Result<Vec<U>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {
        let op_name = final_rdd.get_op_name();
        log::info!("starting `{}` job", op_name);
        let start = Instant::now();
        match self {
            Distributed(distributed) => {
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
            Local(local) => {
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

    fn run_approximate_job<T: Data, U: Data, R, F, E>(
        &self,
        func: Arc<F>,
        final_rdd: Arc<dyn Rdd<Item = T>>,
        evaluator: E,
        timeout: Duration,
    ) -> Result<PartialResult<R>>
    where
        F: SerFunc((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
        E: ApproximateEvaluator<U, R> + Send + Sync + 'static,
        R: Clone + Debug + Send + Sync + 'static,
    {
        let op_name = final_rdd.get_op_name();
        log::info!("starting `{}` job", op_name);
        let start = Instant::now();
        let res = match self {
            Distributed(distributed) => distributed
                .clone()
                .run_approximate_job(func, final_rdd, evaluator, timeout),
            Local(local) => local
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

