

#[derive(thiserror::Error, Debug)]
pub enum BaseError {
    #[error("{0}")]
    DowncastFailure(String),
    
    #[error("{0}")]
    Other(String)
}

pub(crate) type BaseResult<T> = Result<T, BaseError>;
