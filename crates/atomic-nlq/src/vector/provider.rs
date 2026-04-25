use std::fmt;

use async_trait::async_trait;

use crate::errors::Result;

/// Pluggable vector index backend.
#[async_trait]
pub trait VectorIndexProvider: fmt::Debug + Send + Sync {
    fn name(&self) -> &str;

    /// Embedding dimensionality this index expects. Validated at plan time.
    fn dimension(&self) -> usize;

    /// Return the top-k (row_id, score) pairs nearest to `query`.
    /// `query` must have length == `self.dimension()`.
    async fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<(u64, f32)>>;

    /// Insert or update a vector for `id`.
    async fn upsert(&self, id: u64, vector: Vec<f32>) -> Result<()>;
}
