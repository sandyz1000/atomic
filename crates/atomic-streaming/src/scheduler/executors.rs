/// ExecutorAllocationManager — dynamic executor scaling based on streaming load.
///
/// In local mode this is a no-op.
use std::time::Duration;

pub struct ExecutorAllocationManager {
    batch_duration: Duration,
}

impl ExecutorAllocationManager {
    pub fn new(batch_duration: Duration) -> Self {
        ExecutorAllocationManager { batch_duration }
    }

    pub fn manage_allocation(&self) {
        // no-op in local mode
    }

    pub fn request_executors(&self, _num: usize) {
        unimplemented!("ExecutorAllocationManager::request_executors — implement in Phase 4")
    }

    pub fn kill_executor(&self, _executor_id: &str) {
        unimplemented!("ExecutorAllocationManager::kill_executor — implement in Phase 4")
    }
}
