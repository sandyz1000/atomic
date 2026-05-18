use thiserror::Error;

#[derive(Debug, Error)]
pub enum NlqError {
    #[error("Anthropic API error (status {status}): {body}")]
    Api { status: u16, body: String },

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON plan parse error: {0}")]
    PlanParse(String),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("vector index error: {0}")]
    VectorIndex(String),

    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, NlqError>;
