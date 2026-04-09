use thiserror::Error;

#[derive(Debug, Error)]
pub enum StreamingError {
    #[error("StreamingContext already started")]
    AlreadyStarted,
    #[error("StreamingContext already stopped")]
    AlreadyStopped,
    #[error("No batch duration set on DStreamGraph")]
    NoBatchDuration,
    #[error("No output operations registered — call foreach_rdd/print before start()")]
    NoOutputOperations,
    #[error("Checkpoint error: {0}")]
    CheckpointError(String),
    #[error("Receiver error: {0}")]
    ReceiverError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Internal error: {0}")]
    Internal(String),
}

pub type StreamingResult<T> = Result<T, StreamingError>;
