use std::fmt::Display;
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::{
    context::TaskContext,
    data::Data,
    rdd::Rdd,
    task::{Task, TaskBase},
};

pub struct ResultTask<T: Data, U: Data, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    pinned: bool,
    pub rdd: Arc<dyn Rdd<Item = T>>,
    pub func: Arc<F>,
    pub partition: usize,
    pub locs: Vec<Ipv4Addr>,
    pub output_id: usize,
    _marker: PhantomData<T>,
}

impl<T: Data, U: Data, F> Display for ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResultTask({}, {})", self.stage_id, self.partition)
    }
}

impl<T: Data, U: Data, F> ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
    pub fn clone(&self) -> Self {
        ResultTask {
            task_id: self.task_id,
            run_id: self.run_id,
            stage_id: self.stage_id,
            pinned: self.rdd.is_pinned(),
            rdd: self.rdd.clone(),
            func: self.func.clone(),
            partition: self.partition,
            locs: self.locs.clone(),
            output_id: self.output_id,
            _marker: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
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
        ResultTask {
            task_id,
            run_id,
            stage_id,
            pinned: rdd.is_pinned(),
            rdd,
            func,
            partition,
            locs,
            output_id,
            _marker: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> TaskBase for ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
    fn get_run_id(&self) -> usize {
        self.run_id
    }

    fn get_stage_id(&self) -> usize {
        self.stage_id
    }

    fn get_task_id(&self) -> usize {
        self.task_id
    }

    fn is_pinned(&self) -> bool {
        self.pinned
    }

    fn preferred_locations(&self) -> Vec<Ipv4Addr> {
        self.locs.clone()
    }

    fn generation(&self) -> Option<i64> {
        // Some(env::Env::get().map_output_tracker.get_generation())
        None
    }
}

impl<T: Data, U: Data, F> Task for ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
    fn run(&self, id: usize) -> Result<Box<dyn Data>, Box<dyn std::error::Error>> {
        let split = self.rdd.splits()[self.partition].clone();
        let context = TaskContext::new(self.stage_id, self.partition, id);
        let s = Box::new((self.func)((context, self.rdd.iterator(split).unwrap())));
        Ok(s)
    }
}

/// Type-erased wrapper for ResultTask that can be cloned and stored in TaskOption
/// This eliminates the need for trait objects and downcasting
#[derive(Clone)]
pub struct ResultTaskBox {
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    pub partition: usize,
    pub output_id: usize,
    pub locs: Vec<Ipv4Addr>,
    pub is_pinned: bool,
    // Type-erased runner: stores a closure that runs the actual ResultTask
    runner: Arc<dyn Fn(usize) -> Result<Box<dyn Data>, Box<dyn std::error::Error>> + Send + Sync>,
}

impl<T: Data, U: Data, F> From<ResultTask<T, U, F>> for ResultTaskBox
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U + 'static + Send + Sync,
{
    fn from(task: ResultTask<T, U, F>) -> Self {
        let task = Arc::new(task);
        let runner = Arc::clone(&task);

        ResultTaskBox {
            task_id: task.task_id,
            run_id: task.run_id,
            stage_id: task.stage_id,
            partition: task.partition,
            output_id: task.output_id,
            locs: task.locs.clone(),
            is_pinned: task.pinned,
            runner: Arc::new(move |id| runner.run(id)),
        }
    }
}
impl TaskBase for ResultTaskBox {
    fn get_run_id(&self) -> usize {
        self.run_id
    }

    fn get_stage_id(&self) -> usize {
        self.stage_id
    }

    fn get_task_id(&self) -> usize {
        self.task_id
    }

    fn is_pinned(&self) -> bool {
        self.is_pinned
    }

    fn preferred_locations(&self) -> Vec<Ipv4Addr> {
        self.locs.clone()
    }
}

impl Task for ResultTaskBox {
    fn run(&self, id: usize) -> Result<Box<dyn Data>, Box<dyn std::error::Error>> {
        (self.runner)(id)
    }
}
