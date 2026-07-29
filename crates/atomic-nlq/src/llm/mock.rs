use async_trait::async_trait;

use crate::errors::Result;

use super::LlmClient;

/// Deterministic, network-free [`LlmClient`]. Every `chat`/`chat_with_retry` call
/// returns `response` immediately; `embed` returns a fixed-length zero vector per
/// input text. Activated via the `ATOMIC_NLQ_MOCK_LLM` env var — see
/// `PartitionAgentRunner::build_client` — so tests never make a live API call.
#[derive(Clone)]
pub struct MockLlmClient {
    response: String,
}

impl MockLlmClient {
    pub fn new(response: impl Into<String>) -> Self {
        Self {
            response: response.into(),
        }
    }
}

impl Default for MockLlmClient {
    fn default() -> Self {
        Self::new("mock response")
    }
}

#[async_trait]
impl LlmClient for MockLlmClient {
    async fn chat(
        &self,
        _model: &str,
        _system: &str,
        _user: &str,
        _max_tokens: u32,
    ) -> Result<String> {
        Ok(self.response.clone())
    }

    async fn chat_with_retry(
        &self,
        model: &str,
        system: &str,
        user: &str,
        max_tokens: u32,
    ) -> Result<String> {
        self.chat(model, system, user, max_tokens).await
    }

    async fn embed(&self, _model: &str, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|_| vec![0.0_f32; 8]).collect())
    }
}
