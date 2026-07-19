pub mod result;
pub mod shuffle_map;

use crate::data::Data;
pub use result::{ResultTask, ResultTaskBox};
pub use shuffle_map::ShuffleMapTask;
use std::{error, net::Ipv4Addr};

/// Scheduler metadata common to every task, regardless of its kind.
///
/// Both `ResultTaskBox` and `ShuffleMapTask` (and the generic `ResultTask`)
/// embed this so the shared identity/placement fields are defined once.
#[derive(Clone)]
pub struct TaskMeta {
    pub task_id: usize,
    pub run_id: usize,
    pub stage_id: usize,
    pub partition: usize,
    pub locs: Vec<Ipv4Addr>,
    pub pinned: bool,
}

impl TaskMeta {
    pub fn new(
        task_id: usize,
        run_id: usize,
        stage_id: usize,
        partition: usize,
        locs: Vec<Ipv4Addr>,
        pinned: bool,
    ) -> Self {
        TaskMeta {
            task_id,
            run_id,
            stage_id,
            partition,
            locs,
            pinned,
        }
    }
}

/// Base trait for all tasks - exposes the shared [`TaskMeta`] and derives every
/// common accessor from it, so implementors only supply `meta()`.
pub trait TaskBase: Send + Sync {
    fn meta(&self) -> &TaskMeta;

    fn get_run_id(&self) -> usize {
        self.meta().run_id
    }

    fn get_stage_id(&self) -> usize {
        self.meta().stage_id
    }

    fn get_task_id(&self) -> usize {
        self.meta().task_id
    }

    fn partition(&self) -> usize {
        self.meta().partition
    }

    fn is_pinned(&self) -> bool {
        self.meta().pinned
    }

    fn preferred_locations(&self) -> Vec<Ipv4Addr> {
        self.meta().locs.clone()
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
    /// Borrow the active variant as its shared [`TaskBase`] so metadata accessors
    /// forward through one place instead of a match per getter.
    fn as_task_base(&self) -> &dyn TaskBase {
        match self {
            TaskOption::ResultTask(tsk) => tsk,
            TaskOption::ShuffleMapTask(tsk) => tsk,
        }
    }

    /// Run the task and return the result
    pub fn run(&self, id: usize) -> Result<TaskResult, Box<dyn error::Error>> {
        match self {
            TaskOption::ResultTask(tsk) => Ok(TaskResult::ResultTask(tsk.run(id)?)),
            TaskOption::ShuffleMapTask(tsk) => Ok(TaskResult::ShuffleTask(tsk.run(id)?)),
        }
    }

    pub fn get_task_id(&self) -> usize {
        self.as_task_base().get_task_id()
    }

    pub fn get_run_id(&self) -> usize {
        self.as_task_base().get_run_id()
    }

    pub fn get_stage_id(&self) -> usize {
        self.as_task_base().get_stage_id()
    }

    pub fn is_pinned(&self) -> bool {
        self.as_task_base().is_pinned()
    }

    pub fn preferred_locations(&self) -> Vec<Ipv4Addr> {
        self.as_task_base().preferred_locations()
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
