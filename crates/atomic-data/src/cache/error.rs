use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Network error: {0}")]
    Network(#[from] crate::shuffle::error::NetworkError),

    #[error("No message received")]
    NoMessageReceived,

    #[error("Executor shutdown")]
    ExecutorShutdown,

    #[error("Other error")]
    Other,
}

pub type Result<T> = std::result::Result<T, CacheError>;
