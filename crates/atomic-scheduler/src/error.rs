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

    #[error(transparent)]
    PartialJobError(#[from] PartialJobError)
}

pub type LibResult<T> = Result<T, SchedulerError>;
