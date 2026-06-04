use thiserror::Error;

#[derive(Debug, Error)]
pub enum NlqError {
    #[error("OpenAI API error: {0}")]
    Api(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("workflow plan parse error: {0}")]
    PlanParse(String),

    #[error("workflow execution error: {0}")]
    WorkflowExecution(String),

    #[error("tool not found: {0}")]
    ToolNotFound(String),

    #[error("agent exceeded max rounds ({0})")]
    MaxRoundsExceeded(usize),

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
