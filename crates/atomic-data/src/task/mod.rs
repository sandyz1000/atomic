pub mod result;
pub mod shuffle_map;

use crate::data::Data;
pub use result::{ResultTask, ResultTaskBox};
pub use shuffle_map::ShuffleMapTask;
use std::{error, net::Ipv4Addr};

/// Base trait for all tasks - provides common task metadata
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

/// Trait for task execution
pub trait Task: TaskBase + Send + Sync {
    fn run(&self, id: usize) -> Result<Box<dyn Data>, Box<dyn std::error::Error>>;
}

/// Simplified task enum - holds concrete types instead of trait objects
/// This eliminates the need for downcasting!
#[derive(Clone)]
pub enum TaskOption {
    /// A result task that computes final results (type-erased)
    ResultTask(ResultTaskBox),
    /// A shuffle map task that produces shuffle output
    ShuffleMapTask(ShuffleMapTask),
}

impl TaskOption {
    /// Run the task and return the result
    pub fn run(&self, id: usize) -> Result<TaskResult, Box<dyn error::Error>> {
        match self {
            TaskOption::ResultTask(tsk) => Ok(TaskResult::ResultTask(tsk.run(id)?)),
            TaskOption::ShuffleMapTask(tsk) => Ok(TaskResult::ShuffleTask(tsk.run(id)?)),
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

    pub fn is_pinned(&self) -> bool {
        match self {
            TaskOption::ResultTask(tsk) => tsk.is_pinned(),
            TaskOption::ShuffleMapTask(tsk) => tsk.is_pinned(),
        }
    }

    pub fn preferred_locations(&self) -> Vec<Ipv4Addr> {
        match self {
            TaskOption::ResultTask(tsk) => tsk.preferred_locations(),
            TaskOption::ShuffleMapTask(tsk) => tsk.preferred_locations(),
        }
    }
}

/// Result of task execution
pub enum TaskResult {
    ResultTask(Box<dyn Data>),
    ShuffleTask(Box<dyn Data>),
}

/// Implement PartialEq, Eq, PartialOrd, Ord for TaskOption based on task_id
impl PartialEq for TaskOption {
    fn eq(&self, other: &Self) -> bool {
        self.get_task_id() == other.get_task_id()
    }
}

impl Eq for TaskOption {}

impl PartialOrd for TaskOption {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaskOption {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.get_task_id().cmp(&other.get_task_id())
    }
}
