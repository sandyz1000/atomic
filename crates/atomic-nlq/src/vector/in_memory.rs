use std::collections::HashMap;
use std::sync::RwLock;

use async_trait::async_trait;

use crate::errors::Result;

use super::provider::VectorIndexProvider;

/// Brute-force cosine-similarity vector index for testing.
///
/// Not suitable for large datasets. Use as a `VectorIndexProvider` in unit
/// tests or small prototypes only.
#[derive(Debug)]
pub struct InMemoryVectorIndex {
    name: String,
    dim: usize,
    store: RwLock<HashMap<u64, Vec<f32>>>,
}

impl InMemoryVectorIndex {
    pub fn new(name: impl Into<String>, dim: usize) -> Self {
        Self {
            name: name.into(),
            dim,
            store: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl VectorIndexProvider for InMemoryVectorIndex {
    fn name(&self) -> &str {
        &self.name
    }

    fn dimension(&self) -> usize {
        self.dim
    }

    async fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<(u64, f32)>> {
        if query.len() != self.dim {
            return Err(crate::errors::NlqError::VectorIndex(format!(
                "query dimension {} != index dimension {}",
                query.len(),
                self.dim
            )));
        }
        let store = self.store.read().unwrap();
        let mut scores: Vec<(u64, f32)> = store
            .iter()
            .map(|(id, vec)| (*id, cosine_similarity(query, vec)))
            .collect();
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(top_k);
        Ok(scores)
    }

    async fn upsert(&self, id: u64, vector: Vec<f32>) -> Result<()> {
        if vector.len() != self.dim {
            return Err(crate::errors::NlqError::VectorIndex(format!(
                "upsert vector dimension {} != index dimension {}",
                vector.len(),
                self.dim
            )));
        }
        self.store.write().unwrap().insert(id, vector);
        Ok(())
    }
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}
