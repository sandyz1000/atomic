use std::any::Any;
use std::collections::HashMap;
use std::error::Error;

use atomic_data::{data::Data, task::TaskOption};

use crate::Scheduler;

#[derive(Debug, Clone)]
pub struct FetchFailedVals {
    pub server_uri: String,
    pub shuffle_id: usize,
    pub map_id: usize,
    pub reduce_id: usize,
}

/// Task-completion event consumed by the local scheduler's event loop.
/// Accumulator deltas travel in `TaskResultEnvelope::accumulator_deltas` on the
/// task-dispatch path, not here.
pub struct CompletionEvent {
    pub task: TaskOption,
    pub reason: TaskEndReason,
    pub result: Option<Box<dyn Data>>,
}

pub enum TaskEndReason {
    Success,
    FetchFailed(FetchFailedVals),
    Error(Box<dyn Error + Send + Sync>),
    OtherFailure(String),
}

pub trait DAGScheduler: Scheduler {
    fn submit_tasks(&self, tasks: Vec<TaskOption>, run_id: i64);

    fn task_ended(
        task: TaskOption,
        reason: TaskEndReason,
        result: Box<dyn Any>,
        accum_updates: HashMap<i64, Box<dyn Any>>,
    );
}
