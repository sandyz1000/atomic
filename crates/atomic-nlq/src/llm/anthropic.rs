use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::errors::{NlqError, Result};

use super::LlmClient;

const ANTHROPIC_VERSION: &str = "2023-06-01";

pub struct AnthropicClient {
    api_key: String,
    base_url: String,
    timeout: Duration,
    max_retries: u32,
    client: Client,
}

impl AnthropicClient {
    pub fn new(api_key: &str, base_url: &str, timeout_secs: u64, max_retries: u32) -> Self {
        Self {
            api_key: api_key.to_owned(),
            base_url: base_url.to_owned(),
            timeout: Duration::from_secs(timeout_secs),
            max_retries,
            client: Client::new(),
        }
    }

    async fn do_chat(
        &self,
        model: &str,
        system: &str,
        user: &str,
        max_tokens: u32,
    ) -> Result<String> {
        let url = format!("{}/messages", self.base_url.trim_end_matches('/'));
        let body = AnthropicRequest {
            model: model.to_owned(),
            max_tokens,
            system: system.to_owned(),
            messages: vec![Message {
                role: "user".to_owned(),
                content: user.to_owned(),
            }],
        };

        let fut = self
            .client
            .post(&url)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .json(&body)
            .send();

        let resp = tokio::time::timeout(self.timeout, fut)
            .await
            .map_err(|_| {
                NlqError::Api(format!(
                    "Anthropic request timed out after {}s",
                    self.timeout.as_secs()
                ))
            })?
            .map_err(|e| NlqError::Api(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(NlqError::Api(format!("Anthropic API {status}: {body}")));
        }

        let parsed: AnthropicResponse = resp
            .json()
            .await
            .map_err(|e| NlqError::Api(format!("Anthropic response parse error: {e}")))?;

        parsed
            .content
            .into_iter()
            .find(|c| c.r#type == "text")
            .map(|c| c.text)
            .ok_or_else(|| NlqError::Api("empty text response from Anthropic".to_string()))
    }
}

#[async_trait]
impl LlmClient for AnthropicClient {
    async fn chat(
        &self,
        model: &str,
        system: &str,
        user: &str,
        max_tokens: u32,
    ) -> Result<String> {
        self.do_chat(model, system, user, max_tokens).await
    }

    async fn chat_with_retry(
        &self,
        model: &str,
        system: &str,
        user: &str,
        max_tokens: u32,
    ) -> Result<String> {
        let mut delay = Duration::from_millis(500);
        let mut last_err = String::new();
        for attempt in 0..=self.max_retries {
            match self.do_chat(model, system, user, max_tokens).await {
                Ok(text) => return Ok(text),
                Err(e) => {
                    last_err = e.to_string();
                    log::warn!("Anthropic chat attempt {}: {last_err}", attempt + 1);
                    if attempt < self.max_retries {
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_secs(30));
                    }
                }
            }
        }
        Err(NlqError::Api(last_err))
    }

    /// Anthropic does not provide an embeddings API.
    ///
    /// Configure `LLM_PROVIDER=openai` (with `OPENAI_API_KEY`) to use embeddings.
    async fn embed(&self, _model: &str, _texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        Err(NlqError::Config(
            "embeddings are not supported with the Anthropic provider; \
             set LLM_PROVIDER=openai and supply OPENAI_API_KEY to use vector search"
                .to_string(),
        ))
    }
}

// ── Anthropic wire types ─────────────────────────────────────────────────────

#[derive(Serialize)]
struct AnthropicRequest {
    model: String,
    max_tokens: u32,
    system: String,
    messages: Vec<Message>,
}

#[derive(Serialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct AnthropicResponse {
    content: Vec<ContentBlock>,
}

#[derive(Deserialize)]
struct ContentBlock {
    r#type: String,
    text: String,
}
