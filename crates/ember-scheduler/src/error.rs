use ember_data::partial::PartialJobError;



#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("Other error")]
    Other,

    #[error("Downcast failure {0}")]
    DowncastFailure(String),

    #[error(transparent)]
    PartialJobError(#[from] PartialJobError)
}

pub type LibResult<T> = Result<T, SchedulerError>;
