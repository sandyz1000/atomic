

#[derive(thiserror::Error, Debug)]
pub enum BaseError {

}

pub(crate) type BaseResult<T> = Result<T, BaseError>;
