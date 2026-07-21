use http::{Response, StatusCode};
use http_body_util::Full;
use hyper::body::Bytes;
use thiserror::Error;

use crate::shuffle::Body;

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

    /// A specific map output could not be fetched because its host is
    /// unreachable. Carries the identity needed to recompute that map partition.
    #[error("fetch failed: shuffle {shuffle_id} map {map_id} on {server_uri}")]
    FetchFailed {
        shuffle_id: usize,
        map_id: usize,
        server_uri: String,
    },

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

    #[error("bincode encode error: {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),

    #[error("bincode decode error: {0}")]
    BincodeDecode(#[from] bincode::error::DecodeError),

    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("hyper error: {0}")]
    Hyper(#[from] hyper::Error),

    #[error("HTTP client error: {0}")]
    Http(#[from] http::Error),

    #[error("invalid URI: {0}")]
    InvalidUri(#[from] http::uri::InvalidUri),
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

impl From<ShuffleError> for crate::error::DataError {
    fn from(err: ShuffleError) -> Self {
        match err {
            ShuffleError::FetchFailed {
                shuffle_id,
                map_id,
                server_uri,
            } => crate::error::DataError::FetchFailed {
                shuffle_id,
                map_id,
                server_uri,
            },
            other => crate::error::DataError::Other(other.to_string()),
        }
    }
}

impl ShuffleError {
    pub fn no_port(&self) -> bool {
        matches!(
            self,
            ShuffleError::NetworkError(NetworkError::FreePortNotFound(_, _))
        )
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

#[cfg(test)]
mod tests {
    use super::ShuffleError;
    use crate::error::DataError;

    #[test]
    fn fetch_failed_preserved() {
        let base: DataError = ShuffleError::FetchFailed {
            shuffle_id: 7,
            map_id: 3,
            server_uri: "http://10.0.0.2:9000".to_string(),
        }
        .into();
        match base {
            DataError::FetchFailed {
                shuffle_id,
                map_id,
                server_uri,
            } => {
                assert_eq!((shuffle_id, map_id), (7, 3));
                assert_eq!(server_uri, "http://10.0.0.2:9000");
            }
            other => panic!("expected FetchFailed, got {other:?}"),
        }
    }

    #[test]
    fn other_errors_stringify() {
        let base: DataError = ShuffleError::FailedFetchOp.into();
        assert!(matches!(base, DataError::Other(_)));
    }
}
