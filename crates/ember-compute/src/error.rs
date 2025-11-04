use thiserror::Error;

#[derive(Debug, Error)]
pub enum PartialJobError {
    #[error("on_fail cannot be called twice")]
    SetOnFailTwice,

    #[error("set_failure called twice on a PartialResult")]
    SetFailureValTwice,

    #[error("set_final_value called twice on a PartialResult")]
    SetFinalValTwice,

    #[error("on_complete cannot be called twice")]
    SetOnCompleteTwice,

    #[error("unreachable")]
    None,

    #[error("failure while downcasting an object to a concrete type: {0}")]
    DowncastFailure(String),
}

pub type PartialResult<T> = Result<T, PartialJobError>;
