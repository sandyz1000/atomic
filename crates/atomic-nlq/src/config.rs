/// Configuration for NlqContext.
#[derive(Debug, Clone)]
pub struct NlqConfig {
    /// OpenAI API key. Defaults to `OPENAI_API_KEY` env var if not set.
    pub openai_api_key: String,
    /// OpenAI API base URL (override for proxies / Azure OpenAI).
    pub openai_base_url: String,
    /// Model used for planning and agent evaluation (e.g. "gpt-4o").
    pub model: String,
    /// Model used for embedding generation (e.g. "text-embedding-3-small").
    pub embed_model: String,
    /// Maximum agent rounds before returning the best result found so far.
    pub max_rounds: usize,
    /// Rows per LLM API call in LlmFilter/LlmMap builtin tool execution.
    pub llm_batch_size: usize,
    /// Max serialized bytes per chunk sent to the LLM (~200KB ≈ 50k tokens).
    pub max_chunk_bytes: usize,
    /// Max retries on 429 / 503 / 529 responses.
    pub max_retries: u32,
    /// Request timeout in seconds.
    pub timeout_secs: u64,
}

impl Default for NlqConfig {
    fn default() -> Self {
        let api_key = std::env::var("OPENAI_API_KEY").unwrap_or_default();
        Self {
            openai_api_key: api_key,
            openai_base_url: "https://api.openai.com/v1".to_string(),
            model: "gpt-4o".to_string(),
            embed_model: "text-embedding-3-small".to_string(),
            max_rounds: 5,
            llm_batch_size: 50,
            max_chunk_bytes: 200_000,
            max_retries: 3,
            timeout_secs: 60,
        }
    }
}

impl NlqConfig {
    pub fn validate(&self) -> crate::errors::Result<()> {
        if self.openai_api_key.is_empty() {
            return Err(crate::errors::NlqError::Config(
                "OPENAI_API_KEY is not set; set the env var or supply it in NlqConfig".to_string(),
            ));
        }
        if self.llm_batch_size == 0 {
            return Err(crate::errors::NlqError::Config(
                "llm_batch_size must be > 0".to_string(),
            ));
        }
        if self.max_chunk_bytes == 0 {
            return Err(crate::errors::NlqError::Config(
                "max_chunk_bytes must be > 0".to_string(),
            ));
        }
        if self.max_rounds == 0 {
            return Err(crate::errors::NlqError::Config(
                "max_rounds must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}
