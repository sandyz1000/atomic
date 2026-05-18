use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::errors::{NlqError, Result};

/// Thin client for the Voyage AI `/embeddings` endpoint.
pub struct EmbedClient {
    http: Client,
    api_key: String,
    base_url: String,
}

impl std::fmt::Debug for EmbedClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbedClient").field("base_url", &self.base_url).finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct EmbedRequest<'a> {
    input: &'a [String],
    model: &'a str,
}

#[derive(Deserialize)]
struct EmbedResponse {
    data: Vec<EmbedObject>,
}

#[derive(Deserialize)]
struct EmbedObject {
    embedding: Vec<f32>,
}

impl EmbedClient {
    pub fn new(api_key: impl Into<String>, base_url: impl Into<String>, timeout_secs: u64) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .expect("failed to build embed reqwest client");
        Self { http, api_key: api_key.into(), base_url: base_url.into() }
    }

    /// Embed a batch of texts. Returns one `Vec<f32>` per input string.
    pub async fn embed(&self, texts: &[String], model: &str) -> Result<Vec<Vec<f32>>> {
        let url = format!("{}/embeddings", self.base_url);
        let resp = self
            .http
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("content-type", "application/json")
            .json(&EmbedRequest { input: texts, model })
            .send()
            .await?;

        let status = resp.status().as_u16();
        if status != 200 {
            let body = resp.text().await.unwrap_or_default();
            return Err(NlqError::Api { status, body });
        }

        let parsed: EmbedResponse = resp.json().await?;
        Ok(parsed.data.into_iter().map(|o| o.embedding).collect())
    }
}
