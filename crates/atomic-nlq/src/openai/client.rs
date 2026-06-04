use std::time::Duration;

use async_openai::Client;
use async_openai::config::OpenAIConfig;
use async_openai::types::chat::{
    ChatCompletionRequestMessage, ChatCompletionRequestSystemMessage,
    ChatCompletionRequestSystemMessageContent, ChatCompletionRequestUserMessage,
    ChatCompletionRequestUserMessageContent, CreateChatCompletionRequest,
};
use async_openai::types::embeddings::{CreateEmbeddingRequest, EmbeddingInput};

use crate::errors::{NlqError, Result};

pub struct OpenAiClient {
    inner: Client<OpenAIConfig>,
    timeout: Duration,
    max_retries: u32,
}

impl OpenAiClient {
    pub fn new(api_key: &str, base_url: &str, timeout_secs: u64, max_retries: u32) -> Self {
        let config = OpenAIConfig::new()
            .with_api_key(api_key)
            .with_api_base(base_url);
        Self {
            inner: Client::with_config(config),
            timeout: Duration::from_secs(timeout_secs),
            max_retries,
        }
    }

    /// Send a chat completion and return the first text response.
    pub async fn chat(
        &self,
        model: &str,
        system: &str,
        user: &str,
        max_tokens: u32,
    ) -> Result<String> {
        let request = CreateChatCompletionRequest {
            model: model.to_string(),
            messages: vec![
                ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
                    content: ChatCompletionRequestSystemMessageContent::Text(system.to_string()),
                    name: None,
                }),
                ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
                    content: ChatCompletionRequestUserMessageContent::Text(user.to_string()),
                    name: None,
                }),
            ],
            max_completion_tokens: Some(max_tokens),
            ..Default::default()
        };

        let chat = self.inner.chat();
        let fut = chat.create(request);
        let response = tokio::time::timeout(self.timeout, fut)
            .await
            .map_err(|_| {
                NlqError::Api(format!(
                    "request timed out after {}s",
                    self.timeout.as_secs()
                ))
            })?
            .map_err(|e| NlqError::Api(e.to_string()))?;

        response
            .choices
            .into_iter()
            .next()
            .and_then(|c| c.message.content)
            .ok_or_else(|| NlqError::Api("empty response from OpenAI".to_string()))
    }

    /// `chat` with exponential-backoff retry on transient failures.
    pub async fn chat_with_retry(
        &self,
        model: &str,
        system: &str,
        user: &str,
        max_tokens: u32,
    ) -> Result<String> {
        let mut delay = Duration::from_millis(500);
        let mut last_err = String::new();
        for attempt in 0..=self.max_retries {
            match self.chat(model, system, user, max_tokens).await {
                Ok(text) => return Ok(text),
                Err(e) => {
                    last_err = e.to_string();
                    log::warn!("OpenAI chat attempt {}: {last_err}", attempt + 1);
                    if attempt < self.max_retries {
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_secs(30));
                    }
                }
            }
        }
        Err(NlqError::Api(last_err))
    }

    /// Generate embeddings for a batch of texts.
    pub async fn embed(&self, model: &str, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        let request = CreateEmbeddingRequest {
            model: model.to_string(),
            input: EmbeddingInput::StringArray(texts),
            ..Default::default()
        };

        let embeddings = self.inner.embeddings();
        let fut = embeddings.create(request);
        let response = tokio::time::timeout(self.timeout, fut)
            .await
            .map_err(|_| NlqError::Api("embedding request timed out".to_string()))?
            .map_err(|e| NlqError::Api(e.to_string()))?;

        let mut out: Vec<(usize, Vec<f32>)> = response
            .data
            .into_iter()
            .map(|e| (e.index as usize, e.embedding))
            .collect();
        out.sort_by_key(|(i, _)| *i);
        Ok(out.into_iter().map(|(_, v)| v).collect())
    }
}
