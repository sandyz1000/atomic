use std::fmt::Display;
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::Arc;
use serde_derive::{Serialize, Deserialize};
use crate::env;
use crate::rdd::rdd::Rdd;
use crate::scheduler::{Task, TaskBase, TaskContext};
use crate::ser_data::{AnyData, Data};
use crate::split::Split;

#[derive(Serialize, Deserialize)]
pub(crate) struct ResultTask<T, U, F, R>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        // + serde::ser::Serialize
        // + serde::de::DeserializeOwned
        + Clone,
    T : Data,
    U : Data,
    R: Rdd<Item = T>
{
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    pinned: bool,
    pub rdd: Arc<R>,
    pub func: Arc<F>,
    pub partition: usize,
    pub locs: Vec<Ipv4Addr>,
    pub output_id: usize,
    _marker: PhantomData<T>,
}

impl<T, U, F> ResultTask<T, U, F, R>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        // + serde::Serialize
        // + serde::Deserialize
        + Clone,
    T : Data,
    U : Data
{
    
}

impl<T: Data, U: Data, F> Display for ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        // + Serialize
        // + Deserialize
        + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResultTask({}, {})", self.stage_id, self.partition)
    }
}

impl<T: Data, U: Data, F> ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        // + Serialize
        // + Deserialize
        + Clone,
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
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        // + Serialize
        // + Deserialize
        + Clone,
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
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        // + Serialize
        // + Deserialize
        + Clone,
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
        Some(env::Env::get().map_output_tracker.get_generation())
    }
}

impl<T: Data, U: Data, F> Task for ResultTask<T, U, F>
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U
        + 'static
        + Send
        + Sync
        // + Serialize
        // + Deserialize
        + Clone,
{
    /// NOTE: This method run the actual defined task in executor
    fn run(&self, id: usize) -> Box<dyn AnyData> {
        let split: Box<dyn Split> = self.rdd.splits()[self.partition].clone();
        let context: TaskContext = TaskContext::new(self.stage_id, self.partition, id);
        Box::new((self.func)((context, self.rdd.iterator(split).unwrap()))) as Box<dyn AnyData>
    }
}
