use std::error::Error;

use atomic_data::{data::Data, task::TaskOption};

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
