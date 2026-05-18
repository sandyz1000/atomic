use std::time::Duration;

use reqwest::Client;

use crate::errors::{NlqError, Result};

use super::types::{MessagesRequest, MessagesResponse};

pub struct AnthropicClient {
    pub(crate) http: Client,
    pub(crate) api_key: String,
    pub(crate) base_url: String,
}

impl std::fmt::Debug for AnthropicClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnthropicClient")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl AnthropicClient {
    pub fn new(api_key: impl Into<String>, base_url: impl Into<String>, timeout_secs: u64) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .expect("failed to build reqwest client");
        Self { http, api_key: api_key.into(), base_url: base_url.into() }
    }

    pub async fn messages(&self, req: MessagesRequest) -> Result<MessagesResponse> {
        let url = format!("{}/v1/messages", self.base_url);
        let resp = self
            .http
            .post(&url)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&req)
            .send()
            .await?;

        let status = resp.status().as_u16();
        if status != 200 {
            let body = resp.text().await.unwrap_or_default();
            return Err(NlqError::Api { status, body });
        }

        let response: MessagesResponse = resp.json().await?;
        Ok(response)
    }
}
