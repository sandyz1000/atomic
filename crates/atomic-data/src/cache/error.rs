use thiserror::Error;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Bincode encode error: {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),

    #[error("Bincode decode error: {0}")]
    BincodeDecode(#[from] bincode::error::DecodeError),

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
