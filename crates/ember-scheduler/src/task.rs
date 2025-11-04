use ember_data::context::{Task, TaskContext};
use ember_data::data::Data;
use crate::scheduler::ResultTask;
use crate::shuffle::ShuffleMapTask;


pub(crate) trait TaskBox: Task + 'static {}

impl<K> TaskBox for K where K: Task + 'static {}


pub(crate) enum TaskOption {
    ResultTask(Box<dyn TaskBox>),
    ShuffleMapTask(Box<dyn TaskBox>),
}

impl<T: Data, U: Data, F> From<ResultTask<T, U, F>> for TaskOption
where
    F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U,
{
    fn from(t: ResultTask<T, U, F>) -> Self {
        TaskOption::ResultTask(Box::new(t) as Box<dyn TaskBox>)
    }
}

impl From<ShuffleMapTask> for TaskOption {
    fn from(t: ShuffleMapTask) -> Self {
        TaskOption::ResultTask(Box::new(t))
    }
}

pub(crate) enum TaskResult {
    ResultTask(Box<dyn Data>),
    ShuffleTask(Box<dyn Data>),
}

impl TaskOption {
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
