use std::fmt::Display;
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::{
    data::Data,
    rdd::Rdd,
    task::{Task, TaskBase, TaskMeta},
    task_context::TaskContext,
};

pub struct ResultTask<T: Data, U: Data, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
    pub meta: TaskMeta,
    pub rdd: Arc<dyn Rdd<Item = T>>,
    pub func: Arc<F>,
    pub output_id: usize,
    _marker: PhantomData<T>,
}

impl<T: Data, U: Data, F> Display for ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ResultTask({}, {})",
            self.meta.stage_id, self.meta.partition
        )
    }
}

impl<T: Data, U: Data, F> Clone for ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
    fn clone(&self) -> Self {
        // Re-derive `pinned` from the RDD (matches construction in `new`).
        let mut meta = self.meta.clone();
        meta.pinned = self.rdd.is_pinned();
        ResultTask {
            meta,
            rdd: self.rdd.clone(),
            func: self.func.clone(),
            output_id: self.output_id,
            _marker: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: usize,
        run_id: usize,
        stage_id: usize,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: Arc<F>,
        partition: usize,
        locs: Vec<Ipv4Addr>,
        output_id: usize,
    ) -> Self {
        let pinned = rdd.is_pinned();
        ResultTask {
            meta: TaskMeta::new(task_id, run_id, stage_id, partition, locs, pinned),
            rdd,
            func,
            output_id,
            _marker: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> TaskBase for ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
    fn meta(&self) -> &TaskMeta {
        &self.meta
    }
}

impl<T: Data, U: Data, F> Task for ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
    fn run(&self, id: usize) -> Result<Box<dyn Data>, Box<dyn std::error::Error>> {
        let split = self.rdd.splits()[self.meta.partition].clone();
        let context = TaskContext::new(self.meta.stage_id, self.meta.partition, id);
        // Propagate compute errors (e.g. a lost-map-output `BaseError::FetchFailed`)
        // instead of panicking, so the scheduler can recover from lineage.
        let s = Box::new((self.func)((context, self.rdd.iterator(split)?)));
        Ok(s)
    }
}

/// Type-erased wrapper for ResultTask that can be cloned and stored in TaskOption
/// This eliminates the need for trait objects and downcasting
#[derive(Clone)]
pub struct ResultTaskBox {
    pub meta: TaskMeta,
    pub output_id: usize,
    // Type-erased runner: stores a closure that runs the actual ResultTask
    runner: TaskRunner,
}

/// Type-erased runner closure that executes a `ResultTask` for a given partition.
type TaskRunner =
    Arc<dyn Fn(usize) -> Result<Box<dyn Data>, Box<dyn std::error::Error>> + Send + Sync>;

impl<T: Data, U: Data, F> From<ResultTask<T, U, F>> for ResultTaskBox
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
    fn from(task: ResultTask<T, U, F>) -> Self {
        let task = Arc::new(task);
        let runner = Arc::clone(&task);

        ResultTaskBox {
            meta: task.meta.clone(),
            output_id: task.output_id,
            runner: Arc::new(move |id| runner.run(id)),
        }
    }
}
impl TaskBase for ResultTaskBox {
    fn meta(&self) -> &TaskMeta {
        &self.meta
    }
}

impl Task for ResultTaskBox {
    fn run(&self, id: usize) -> Result<Box<dyn Data>, Box<dyn std::error::Error>> {
        (self.runner)(id)
    }
}
