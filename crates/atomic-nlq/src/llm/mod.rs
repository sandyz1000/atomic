use async_trait::async_trait;

use crate::errors::Result;

pub mod anthropic;

/// Abstraction over LLM providers used for planning, evaluation, and SQL extension nodes.
///
/// Implement this trait to add a new provider. Both `OpenAiClient` and `AnthropicClient`
/// implement it; callers hold `Arc<dyn LlmClient>` and are provider-agnostic.
#[async_trait]
pub trait LlmClient: Send + Sync {
    /// Single chat completion — system prompt + user message → text response.
    async fn chat(
        &self,
        model: &str,
        system: &str,
        user: &str,
        max_tokens: u32,
    ) -> Result<String>;

    /// `chat` with exponential-backoff retry on transient failures (429, 529, timeout).
    async fn chat_with_retry(
        &self,
        model: &str,
        system: &str,
        user: &str,
        max_tokens: u32,
    ) -> Result<String>;

    /// Batch text embedding. Returns one vector per input string, preserving order.
    ///
    /// Providers that do not support embeddings (e.g. Anthropic) must return
    /// `Err(NlqError::Config(...))` with a clear message.
    async fn embed(&self, model: &str, texts: Vec<String>) -> Result<Vec<Vec<f32>>>;
}
