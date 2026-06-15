//! Error type for structured streaming.

use std::fmt;

/// Result alias for structured-streaming operations.
pub type StructuredResult<T> = Result<T, StructuredError>;

/// Errors raised while building or running a structured streaming query.
#[derive(Debug)]
pub enum StructuredError {
    /// A SQL/plan error from DataFusion or the SQL layer.
    Sql(String),
    /// A sink failed to accept a batch.
    Sink(String),
    /// A source failed to produce data.
    Source(String),
    /// The query used an unsupported feature (e.g. a non-mergeable aggregate).
    Unsupported(String),
    /// Checkpoint read/write failure.
    Checkpoint(String),
}

impl fmt::Display for StructuredError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StructuredError::Sql(m) => write!(f, "sql error: {m}"),
            StructuredError::Sink(m) => write!(f, "sink error: {m}"),
            StructuredError::Source(m) => write!(f, "source error: {m}"),
            StructuredError::Unsupported(m) => write!(f, "unsupported: {m}"),
            StructuredError::Checkpoint(m) => write!(f, "checkpoint error: {m}"),
        }
    }
}

impl std::error::Error for StructuredError {}

impl From<datafusion::error::DataFusionError> for StructuredError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        StructuredError::Sql(e.to_string())
    }
}
