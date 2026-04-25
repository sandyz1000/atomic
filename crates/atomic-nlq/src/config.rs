/// Configuration for NlqContext.
#[derive(Debug, Clone)]
pub struct NlqConfig {
    /// Anthropic API key. Defaults to `ANTHROPIC_API_KEY` env var if not set.
    pub anthropic_api_key: String,
    /// API key for the embedding provider (Voyage AI). Defaults to `VOYAGE_API_KEY`.
    pub embed_api_key: String,
    /// Model used for NL-to-plan translation (e.g. "claude-sonnet-4-6").
    pub default_model: String,
    /// Model used for embedding generation (e.g. "voyage-3").
    pub embed_model: String,
    /// Override for embedding dimension when model is not in the built-in table.
    pub embed_model_dim: Option<usize>,
    /// Base URL for the embed API (Voyage AI).
    pub embed_base_url: String,
    /// Rows per LLM API call in LlmFilter/LlmMap execution.
    pub llm_batch_size: usize,
    /// Max serialized bytes per chunk sent to the LLM (~200KB ≈ 50k tokens).
    pub max_chunk_bytes: usize,
    /// Max retries on 429 / 503 / 529 responses.
    pub max_retries: u32,
    /// Request timeout in seconds.
    pub timeout_secs: u64,
    /// Anthropic API base URL.
    pub anthropic_base_url: String,
}

impl Default for NlqConfig {
    fn default() -> Self {
        let api_key = std::env::var("ANTHROPIC_API_KEY").unwrap_or_default();
        let embed_api_key = std::env::var("VOYAGE_API_KEY").unwrap_or_default();
        Self {
            anthropic_api_key: api_key,
            embed_api_key,
            default_model: "claude-sonnet-4-6".to_string(),
            embed_model: "voyage-3".to_string(),
            embed_model_dim: None,
            embed_base_url: "https://api.voyageai.com/v1".to_string(),
            llm_batch_size: 50,
            max_chunk_bytes: 200_000,
            max_retries: 3,
            timeout_secs: 60,
            anthropic_base_url: "https://api.anthropic.com".to_string(),
        }
    }
}

impl NlqConfig {
    /// Validate required fields. Call this in `NlqContext::build`.
    pub fn validate(&self) -> crate::errors::Result<()> {
        if self.anthropic_api_key.is_empty() {
            return Err(crate::errors::NlqError::Internal(
                "ANTHROPIC_API_KEY is not set; set the env var or supply it in NlqConfig"
                    .to_string(),
            ));
        }
        if self.llm_batch_size == 0 {
            return Err(crate::errors::NlqError::Internal(
                "llm_batch_size must be > 0".to_string(),
            ));
        }
        if self.max_chunk_bytes == 0 {
            return Err(crate::errors::NlqError::Internal(
                "max_chunk_bytes must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}
