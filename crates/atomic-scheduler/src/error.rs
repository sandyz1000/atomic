use atomic_data::error::BaseError;
use atomic_data::partial::PartialJobError;

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("Other error")]
    Other,

    #[error("operation not supported: {0}")]
    UnsupportedOperation(&'static str),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("no compatible worker available for {0}")]
    NoCompatibleWorker(String),

    #[error("artifact resolution failed for {0}")]
    ArtifactResolution(String),

    #[error("Downcast failure {0}")]
    DowncastFailure(String),

    /// A task returned `FatalFailure` or exhausted retries with `RetryableFailure`.
    #[error("task failed: {0}")]
    TaskFailed(String),

    /// A task (or stage) exceeded the per-task failure limit.
    #[error("max task failures reached: {0}")]
    MaxTaskFailures(String),

    /// A job was aborted because lost shuffle output could not be recovered.
    #[error("job aborted: {0}")]
    JobAborted(String),

    #[error(transparent)]
    PartialJobError(#[from] PartialJobError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Base(#[from] BaseError),
}

pub type LibResult<T> = Result<T, SchedulerError>;
