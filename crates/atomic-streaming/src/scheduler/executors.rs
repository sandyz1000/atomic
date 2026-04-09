/// ExecutorAllocationManager — dynamic executor scaling based on streaming load.
///
/// TODO Phase 4 (distributed): implement PID-based scaling.
/// In local mode this is a no-op.
use std::time::Duration;

pub struct ExecutorAllocationManager {
    batch_duration: Duration,
}

impl ExecutorAllocationManager {
    pub fn new(batch_duration: Duration) -> Self {
        ExecutorAllocationManager { batch_duration }
    }

    /// TODO Phase 4: request additional executors based on backlog.
    pub fn manage_allocation(&self) {
        // no-op in local mode
    }

    /// TODO Phase 4: request N more executors from the cluster manager.
    pub fn request_executors(&self, _num: usize) {
        unimplemented!("ExecutorAllocationManager::request_executors — implement in Phase 4")
    }

    /// TODO Phase 4: kill idle executor.
    pub fn kill_executor(&self, _executor_id: &str) {
        unimplemented!("ExecutorAllocationManager::kill_executor — implement in Phase 4")
    }
}
