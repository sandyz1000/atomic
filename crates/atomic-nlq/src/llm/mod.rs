use async_trait::async_trait;

use crate::errors::{NlqError, Result};

pub mod anthropic;

/// Retry `attempt` with exponential backoff (500ms, doubling, capped at 30s) for up to
/// `max_retries` extra tries after the first. Shared by every `LlmClient::chat_with_retry`
/// impl; `provider` only affects the warning log text.
pub(crate) async fn chat_with_retry<F, Fut>(
    provider: &str,
    max_retries: u32,
    mut attempt: F,
) -> Result<String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<String>>,
{
    let mut delay = std::time::Duration::from_millis(500);
    let mut last_err = String::new();
    for i in 0..=max_retries {
        match attempt().await {
            Ok(text) => return Ok(text),
            Err(e) => {
                last_err = e.to_string();
                log::warn!("{provider} chat attempt {}: {last_err}", i + 1);
                if i < max_retries {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(std::time::Duration::from_secs(30));
                }
            }
        }
    }
    Err(NlqError::Api(last_err))
}

/// Abstraction over LLM providers used for planning, evaluation, and SQL extension nodes.
///
/// Implement this trait to add a new provider. Both `OpenAiClient` and `AnthropicClient`
/// implement it; callers hold `Arc<dyn LlmClient>` and are provider-agnostic.
#[async_trait]
pub trait LlmClient: Send + Sync {
    /// Single chat completion — system prompt + user message → text response.
    async fn chat(&self, model: &str, system: &str, user: &str, max_tokens: u32) -> Result<String>;

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
