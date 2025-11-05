use http::{Response, StatusCode};
use http_body_util::Full;
use hyper::body::Bytes;
use thiserror::Error;

use crate::Body;

#[derive(Debug, Error)]
pub enum ShuffleError {
    #[error("failed to create local shuffle dir after 10 attempts")]
    CouldNotCreateShuffleDir,

    // #[error("deserialization error")]
    // DeserializationError(#[from] bincode::Error),
    #[error("gRPC transport error")]
    TransportError(#[from] tonic::transport::Error),

    #[error("gRPC status error: {0}")]
    GrpcStatus(#[from] tonic::Status),

    #[error("incorrect URI sent in the request")]
    IncorrectUri(#[from] http::uri::InvalidUri),

    #[error("internal server error")]
    InternalError,

    #[error("shuffle fetcher failed while fetching chunk")]
    FailedFetchOp,

    #[error("failed to start shuffle server")]
    FailedToStart,

    #[error(transparent)]
    NetworkError(#[from] NetworkError),

    #[error("not valid request")]
    NotValidRequest,

    #[error("cached data not found")]
    RequestedCacheNotFound,

    #[error("unexpected shuffle server problem")]
    UnexpectedServerError(#[from] hyper::Error),

    #[error("unexpected URI sent in the request: {0}")]
    UnexpectedUri(String),

    #[error("failed fetching shuffle data uris")]
    FailFetchingShuffleUris {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("unrecognized error (todo!)")]
    Other,
}

#[derive(thiserror::Error, Debug)]
pub enum NetworkError {
    #[error(transparent)]
    TcpListener(#[from] tokio::io::Error),

    #[error("failed to find free port {0}, tried {1} times")]
    FreePortNotFound(u16, usize),

    #[error("bincode serialization error: {0}")]
    BincodeError(String),

    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("invalid URI: {0}")]
    InvalidUri(String),
}

impl From<ShuffleError> for Response<Body> {
    fn from(err: ShuffleError) -> Response<Body> {
        match err {
            ShuffleError::UnexpectedUri(uri) => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from(format!("Failed to parse: {}", uri))))
                .unwrap(),
            ShuffleError::RequestedCacheNotFound => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::new()))
                .unwrap(),
            _ => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::new(Bytes::from(err.to_string())))
                .unwrap(),
        }
    }
}

impl ShuffleError {
    pub fn no_port(&self) -> bool {
        match self {
            ShuffleError::NetworkError(NetworkError::FreePortNotFound(_, _)) => true,
            _ => false,
        }
    }
}

impl From<ShuffleError> for tonic::Status {
    fn from(err: ShuffleError) -> Self {
        match err {
            ShuffleError::NotValidRequest => tonic::Status::invalid_argument(err.to_string()),
            ShuffleError::RequestedCacheNotFound => tonic::Status::not_found(err.to_string()),
            _ => tonic::Status::internal(err.to_string()),
        }
    }
}
