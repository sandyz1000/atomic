use thiserror::Error;

#[derive(Debug, Error)]
pub enum AtomicSqlError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("SQL parse error: {0}")]
    Parse(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, AtomicSqlError>;
