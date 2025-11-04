use std::sync::Arc;
use std::net::Ipv4Addr;
use crate::{data::Data, error::BaseResult, rdd::Rdd};

pub struct TaskContext {
    pub stage_id: usize,
    pub split_id: usize,
    pub attempt_id: usize,
}

impl TaskContext {
    pub fn new(stage_id: usize, split_id: usize, attempt_id: usize) -> Self {
        TaskContext {
            stage_id,
            split_id,
            attempt_id,
        }
    }
}


pub trait TaskBase: Send + Sync {
    fn get_run_id(&self) -> usize;
    
    fn get_stage_id(&self) -> usize;
    
    fn get_task_id(&self) -> usize;
    
    fn is_pinned(&self) -> bool {
        false
    }
    
    fn preferred_locations(&self) -> Vec<Ipv4Addr> {
        Vec::new()
    }
    
    fn generation(&self) -> Option<i64> {
        None
    }
}


impl PartialOrd for dyn TaskBase {
    fn partial_cmp(&self, other: &dyn TaskBase) -> Option<std::cmp::Ordering> {
        Some(self.get_task_id().cmp(&other.get_task_id()))
    }
}

impl PartialEq for dyn TaskBase {
    fn eq(&self, other: &dyn TaskBase) -> bool {
        self.get_task_id() == other.get_task_id()
    }
}

impl Eq for dyn TaskBase {}

impl Ord for dyn TaskBase {
    fn cmp(&self, other: &dyn TaskBase) -> std::cmp::Ordering {
        self.get_task_id().cmp(&other.get_task_id())
    }
}


pub trait Task: TaskBase + Send + Sync {
    fn run(&self, id: usize) -> Result<Box<dyn Data>, Box<dyn std::error::Error>>;
}


pub struct Context {
    
}

impl Context {
    pub fn run_job_with_context<T: Data, U: Data, F>(
        self: &Arc<Self>,
        rdd: Arc<dyn Rdd<Item = T>>,
        func: F,
    ) -> BaseResult<Vec<U>>
    where
        F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
    {

        todo!()
    }
}
