use std::ffi::OsString;
use std::path::PathBuf;

use atomic_data::distributed::{ArtifactKind, ExecutionBackend};
use atomic_data::partial::PartialJobError;
use atomic_data::shuffle::{
    error::{NetworkError, ShuffleError},
    map_output::MapOutputError,
};
use thiserror::Error;

pub type LibResult<T> = std::result::Result<T, Error>;
pub type Result<T> = std::result::Result<T, Error>;
pub type StdResult<T, E> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
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

    #[error("docker runtime error: {0}")]
    DockerRuntime(String),

    #[error("artifact load error: {0}")]
    ArtifactLoad(String),

    #[error("artifact validation error: {0}")]
    ArtifactValidation(String),

    #[error("invalid distributed payload: {0}")]
    InvalidPayload(String),

    #[error("invalid transport frame: {0}")]
    InvalidTransportFrame(String),

    #[error("task container exited with code {exit_code} for image {image}")]
    ContainerExit { image: String, exit_code: i64 },

    #[error("worker is configured for backend {configured:?} but task requested {requested:?}")]
    UnsupportedExecutionBackend {
        configured: ExecutionBackend,
        requested: ExecutionBackend,
    },

    #[error("worker backend {backend:?} does not support artifact kind {artifact_kind:?}")]
    UnsupportedArtifact {
        backend: ExecutionBackend,
        artifact_kind: ArtifactKind,
    },

    #[error("worker handshake failed: {0}")]
    WorkerHandshake(String),

    #[error("wasm runtime error: {0}")]
    WasmRuntime(String),

    // #[error(transparent)]
    // BincodeDeserialization(#[from] bincode::Error),

    // #[error(transparent)]
    // CapnpDeserialization(#[from] capnp::Error),
    #[error("failure while downcasting an object to a concrete type: {0}")]
    DowncastFailure(&'static str),

    #[error("executor shutdown signal")]
    ExecutorShutdown,

    #[error("configuration failure: {0}")]
    GetOrCreateConfig(&'static str),

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

    #[error("no files for the given path")]
    NoFilesFound,

    #[error("unrecognized error (todo!)")]
    Other,
}

impl Error {
    pub fn executor_shutdown(&self) -> bool {
        match self {
            Error::ExecutorShutdown => true,
            _ => false,
        }
    }
}

impl From<atomic_scheduler::error::SchedulerError> for Error {
    fn from(err: atomic_scheduler::error::SchedulerError) -> Self {
        Error::InvalidPayload(err.to_string())
    }
}

impl From<Error> for atomic_data::error::BaseError {
    fn from(err: Error) -> Self {
        atomic_data::error::BaseError::Other(format!("{}", err))
    }
}

// #[derive(Debug, Error)]
// pub enum NetworkError {
//     #[error("disconnected from address")]
//     ConnectionFailure,

//     #[error("failed to find free port {0}, tried {1} times")]
//     FreePortNotFound(u16, usize),

//     #[error("no message received")]
//     NoMessageReceived,

//     #[error(transparent)]
//     TcpListener(#[from] tokio::io::Error),
// }

// #[derive(Debug, Error)]
// pub enum MapOutputError {
//     #[error("Shuffle id output #{0} not found in the map")]
//     ShuffleIdNotFound(usize),
// }
