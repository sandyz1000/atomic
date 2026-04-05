

#[derive(thiserror::Error, Debug)]
pub enum BaseError {
    #[error("{0}")]
    DowncastFailure(String),
    
    #[error("{0}")]
    Other(String)
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
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn base_result_err_propagates() {
        let result: BaseResult<u32> = Err(BaseError::Other("fail".into()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "fail");
    }
}
