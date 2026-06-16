use std::ffi::OsString;
use std::path::PathBuf;

use atomic_data::error::BaseError;
use atomic_data::partial::PartialJobError;
use atomic_data::shuffle::{
    error::{NetworkError, ShuffleError},
    map_output::MapOutputError,
};
use thiserror::Error;

pub type ComputeResult<T, E = ComputeError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum ComputeError {
    #[error(transparent)]
    AsyncJoinError(#[from] tokio::task::JoinError),

    #[error("failed to run {command}")]
    CommandOutput {
        source: std::io::Error,
        command: String,
    },

    #[error("failed to create the log file")]
    CreateLogFile(#[source] std::io::Error),

    #[error("failed to create the terminal logger")]
    CreateTerminalLogger,

    #[error("couldn't determine the current binary's name")]
    CurrentBinaryName,

    #[error("couldn't determine the path to the current binary")]
    CurrentBinaryPath,

    #[error("failed trying converting to type {0}")]
    ConversionError(&'static str),

    #[error("invalid distributed payload: {0}")]
    InvalidPayload(String),

    #[error("invalid transport frame: {0}")]
    InvalidTransportFrame(String),

    #[error("worker handshake failed: {0}")]
    WorkerHandshake(String),

    #[error("failure while downcasting an object to a concrete type: {0}")]
    DowncastFailure(&'static str),

    #[error("executor shutdown signal")]
    ExecutorShutdown,

    #[error("configuration failure: {0}")]
    GetOrCreateConfig(&'static str),

    #[error("invalid configuration: {0}")]
    InvalidConfig(#[from] crate::env::ConfigError),

    #[error("partitioner not set")]
    LackingPartitioner,

    #[error("failed to load hosts file from {}", path.display())]
    LoadHosts {
        source: std::io::Error,
        path: PathBuf,
    },

    #[error(transparent)]
    MapOutputError(#[from] MapOutputError),

    #[error("network error")]
    NetworkError(#[from] NetworkError),

    #[error("unwrapped a non-unique shared reference")]
    NotUniqueSharedRef,

    #[error("failed to determine the home directory")]
    NoHome,

    #[error("failed to convert {:?} to a String", .0)]
    OsStringToString(OsString),

    #[error("failed writing to output destination")]
    OutputWrite(#[source] std::io::Error),

    #[error("failed to parse hosts file at {}", path.display())]
    ParseHosts {
        source: toml::de::Error,
        path: PathBuf,
    },

    #[error(transparent)]
    PartialJobError(#[from] PartialJobError),

    #[error("failed to convert {} to a String", .0.display())]
    PathToString(PathBuf),

    #[error("failed to parse slave address {0}")]
    ParseHostAddress(String),

    #[error("failed reading from input source")]
    InputRead(#[source] std::io::Error),

    #[error(transparent)]
    ShuffleError(#[from] ShuffleError),

    #[error("operation not supported: {0}")]
    UnsupportedOperation(&'static str),

    #[error("unknown operation id: {0}")]
    UnknownOperation(String),

    #[error("no files for the given path")]
    NoFilesFound,

    #[error(transparent)]
    SchedulerError(#[from] atomic_scheduler::error::SchedulerError),

    #[error("internal error: {0}")]
    Other(String),
}

impl ComputeError {
    pub fn executor_shutdown(&self) -> bool {
        matches!(self, ComputeError::ExecutorShutdown)
    }
}

impl From<ComputeError> for BaseError {
    fn from(err: ComputeError) -> Self {
        BaseError::Other(format!("{}", err))
    }
}

impl From<BaseError> for ComputeError {
    fn from(e: BaseError) -> Self {
        ComputeError::InvalidPayload(e.to_string())
    }
}

impl From<String> for ComputeError {
    fn from(s: String) -> Self {
        ComputeError::InvalidPayload(s)
    }
}

impl From<std::str::Utf8Error> for ComputeError {
    fn from(e: std::str::Utf8Error) -> Self {
        ComputeError::InvalidPayload(format!("payload is not valid UTF-8: {e}"))
    }
}
