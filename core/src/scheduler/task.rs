use std::cmp::Ordering;
use std::net::Ipv4Addr;

use crate::rdd::rdd::RddBase;
use crate::scheduler::ResultTask;
use crate::ser_data::{AnyData, Data, SerFunc};
use crate::shuffle::ShuffleMapTask;
use crate::split::Split;
use crate::Rdd;
use downcast_rs::{impl_downcast, Downcast};
use serde::{Deserialize, Serialize};


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

pub trait TaskBase: Downcast + Send + Sync {
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

impl_downcast!(TaskBase);

impl PartialOrd for dyn TaskBase {
    fn partial_cmp(&self, other: &dyn TaskBase) -> Option<Ordering> {
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
    fn cmp(&self, other: &dyn TaskBase) -> Ordering {
        self.get_task_id().cmp(&other.get_task_id())
    }
}

pub(crate) trait Task: TaskBase + Send + Sync + Downcast {
    type Data: AnyData;
    fn run(&self, id: usize) -> Box<Self::Data>;
}

// impl_downcast!(Task);

// pub(crate) trait TaskBox: Task + serde::Serialize + serde::de::Deserialize<'de> + 'static + Downcast {
// }

// impl<'de, K> TaskBox for K where K: Task + serde::Serialize + serde::de::Deserializer<'de> + 'static {
// }

// #[typetag::serde(tag = "type")]
pub trait TaskBox: Task + 'static + Downcast {
}

// #[typetag::serde]  // TODO: Fix me
impl<K> TaskBox for K 
    where K: Task + 'static 
{
}

// impl_downcast!(TaskBox);

#[derive(Serialize, Deserialize)]
pub enum TaskOption<T: TaskBox> {
    ResultTask(Box<T>),
    ShuffleMapTask(Box<T>),
}

impl<T: Data, U: Data, F, R, Tb, Ad> From<ResultTask<T, U, F, R, Ad>> for TaskOption<Tb>
where
    F: SerFunc<(TaskContext, Box<dyn Iterator<Item = T>>), Output =  U>,
    R: Rdd<Item = T>,
    Tb: TaskBox,
    Ad: AnyData
{
    fn from(t: ResultTask<T, U, F, R, Ad>) -> Self {
        TaskOption::ResultTask(Box::new(t) as Box<dyn TaskBox>)
    }
}

impl<Tb: TaskBox, RDD, S> From<ShuffleMapTask<RDD, S>> for TaskOption<Tb> 
where 
    S: Split,
    RDD: RddBase,
{
    fn from(t: ShuffleMapTask<RDD, S>) -> Self {
        TaskOption::ResultTask(Box::new(t))
    }
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
pub enum TaskResult {
    ResultTask(Box<dyn AnyData>),
    ShuffleTask(Box<dyn AnyData>),
}

impl<Tb: TaskBox> TaskOption<Tb> {
    pub fn run(&self, id: usize) -> TaskResult {
        match self {
            TaskOption::ResultTask(tsk) => TaskResult::ResultTask(tsk.run(id)),
            TaskOption::ShuffleMapTask(tsk) => TaskResult::ShuffleTask(tsk.run(id)),
        }
    }

    pub fn get_task_id(&self) -> usize {
        match self {
            TaskOption::ResultTask(tsk) => tsk.get_task_id(),
            TaskOption::ShuffleMapTask(tsk) => tsk.get_task_id(),
        }
    }

    pub fn get_run_id(&self) -> usize {
        match self {
            TaskOption::ResultTask(tsk) => tsk.get_run_id(),
            TaskOption::ShuffleMapTask(tsk) => tsk.get_run_id(),
        }
    }

    pub fn get_stage_id(&self) -> usize {
        match self {
            TaskOption::ResultTask(tsk) => tsk.get_stage_id(),
            TaskOption::ShuffleMapTask(tsk) => tsk.get_stage_id(),
        }
    }
}
