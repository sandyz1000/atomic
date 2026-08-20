use crate::error::{LibResult, SchedulerError};
use atomic_data::data::Data;

/// Interface used to listen for job completion or failure events after submitting a job to the
/// scheduler. The listener is notified each time a task succeeds, as well as if the whole
/// job fails (and no further task-succeeded events will happen).
#[async_trait::async_trait]
pub trait JobListener: Send + Sync {
    async fn task_succeeded(&self, _index: usize, _result: &dyn Data) -> LibResult<()> {
        Ok(())
    }
    async fn job_failed(&self, err: SchedulerError) {
        log::debug!("job failed with error: {}", err);
    }
}

/// A listener which produces no action whatsoever.
pub struct NoOpListener;

impl JobListener for NoOpListener {}
