#[derive(thiserror::Error, Debug)]
pub enum BaseError {
    #[error("{0}")]
    DowncastFailure(String),

    #[error("{0}")]
    Other(String),

    /// A shuffle reduce task could not fetch a specific map output because its
    /// host is unreachable (e.g. the producing worker died). Carries enough
    /// identity for the scheduler to recompute exactly that lost map partition
    /// from lineage instead of failing the job.
    #[error("fetch failed: shuffle {shuffle_id} map {map_id} on {server_uri}")]
    FetchFailed {
        shuffle_id: usize,
        map_id: usize,
        server_uri: String,
    },

    /// Rkyv serialization/deserialization error (wire encoding).
    #[error("wire encoding error: {0}")]
    WireError(#[from] rkyv::rancor::Error),

    /// Filesystem or other I/O failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type BaseResult<T> = Result<T, BaseError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn other_error_displays_message() {
        let err = BaseError::Other("something went wrong".into());
        assert_eq!(err.to_string(), "something went wrong");
    }

    #[test]
    fn downcast_failure_displays_message() {
        let err = BaseError::DowncastFailure("type mismatch".into());
        assert_eq!(err.to_string(), "type mismatch");
    }

    #[test]
    fn base_result_ok_round_trip() {
        let result: BaseResult<u32> = Ok(42);
        assert_eq!(result.ok(), Some(42));
    }

    #[test]
    fn base_result_err_propagates() {
        let result: BaseResult<u32> = Err(BaseError::Other("fail".into()));
        match result {
            Err(e) => assert_eq!(e.to_string(), "fail"),
            Ok(_) => panic!("expected Err"),
        }
    }
}
