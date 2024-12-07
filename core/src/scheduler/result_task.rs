use std::fmt::Display;
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::Arc;
use serde_derive::{Serialize, Deserialize};
use crate::env;
use crate::rdd::rdd::Rdd;
use crate::scheduler::{Task, TaskBase, TaskContext};
use crate::ser_data::{AnyData, Data, SerFunc};

// #[derive(Serialize, Deserialize)]
#[derive(Serialize)]
pub(crate) struct ResultTask<T, U, F, R, Ad>
where
    F: SerFunc<(TaskContext, Box<dyn Iterator<Item = T>>), Output=U>
        + 'static
        + Send
        + Sync
        + Clone,
    T : Data,
    U : Data,
    R: Rdd<Item = T>,
    Ad: AnyData
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
    _marker_d: PhantomData<Ad>
}

impl<T, U, F, R, Ad: AnyData> ResultTask<T, U, F, R, Ad>
where
    F: SerFunc<(TaskContext, Box<dyn Iterator<Item = T>>), Output=U>
        + 'static
        + Send
        + Sync
        + Clone,
    T : Data,
    U : Data,
    R: Rdd<Item = T>
{
    
}

impl<T: Data, U: Data, F, R, Ad: AnyData> Display for ResultTask<T, U, F, R, Ad>
where
    F: SerFunc<(TaskContext, Box<dyn Iterator<Item = T>>), Output=U>
        + 'static
        + Send
        + Sync
        + Clone,
    R: Rdd<Item = T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResultTask({}, {})", self.stage_id, self.partition)
    }
}

impl<T: Data, U: Data, F, R, Ad: AnyData> ResultTask<T, U, F, R, Ad>
where
    F: SerFunc<(TaskContext, Box<dyn Iterator<Item = T>>), Output=U>
        + 'static
        + Send
        + Sync
        + Clone,
    R: Rdd<Item = T>
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
            _marker_d: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F, R, Ad: AnyData> ResultTask<T, U, F, R, Ad>
where
    F: SerFunc<(TaskContext, Box<dyn Iterator<Item = T>>), Output=U>
        + 'static
        + Send
        + Sync
        // + Serialize
        // + Deserialize
        + Clone,
    R: Rdd<Item = T>
{
    pub fn new(
        task_id: usize,
        run_id: usize,
        stage_id: usize,
        rdd: Arc<R>,
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
            _marker_d: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F, R, Ad: AnyData> TaskBase for ResultTask<T, U, F, R, Ad>
where
    F: SerFunc<(TaskContext, Box<dyn Iterator<Item = T>>), Output=U>
        + 'static
        + Send
        + Sync
        + Clone,
    R: Rdd<Item = T>
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

impl<T: Data, U: Data, F, R, Ad> Task for ResultTask<T, U, F, R, Ad>
where
    F: SerFunc<(TaskContext, Box<dyn Iterator<Item = T>>), Output=U>
        + 'static
        + Send
        + Sync
        // + Serialize
        // + Deserialize
        + Clone,
    R: Rdd<Item = T>,
    Ad: AnyData
{
    type Data = Ad;  // TODO: Fix Me

    /// NOTE: This method run the actual defined task in executor
    fn run(&self, id: usize) -> Box<Self::Data> {
        let split = self.rdd.splits()[self.partition].clone();
        let context: TaskContext = TaskContext::new(self.stage_id, self.partition, id);
        Box::new((self.func)((context, self.rdd.iterator(split).unwrap())))
    }
}
