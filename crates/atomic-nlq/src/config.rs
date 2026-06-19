/// Which LLM provider to use for planning, evaluation, and SQL extension nodes.
///
/// Selected via the `LLM_PROVIDER` environment variable (default: `openai`).
/// The Anthropic provider does not support embeddings — `EmbedExec` / `VectorSearchExec`
/// return an error when it is active.
#[derive(Debug, Clone, Default)]
pub enum LlmProvider {
    #[default]
    OpenAi,
    Anthropic,
}

impl LlmProvider {
    fn from_env() -> Self {
        match std::env::var("LLM_PROVIDER")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "anthropic" | "claude" => LlmProvider::Anthropic,
            _ => LlmProvider::OpenAi,
        }
    }

    /// Default API base URL for this provider.
    pub fn default_base_url(&self) -> &'static str {
        match self {
            LlmProvider::OpenAi => "https://api.openai.com/v1",
            LlmProvider::Anthropic => "https://api.anthropic.com",
        }
    }

    /// Default chat model for this provider.
    pub fn default_model(&self) -> &'static str {
        match self {
            LlmProvider::OpenAi => "gpt-4o",
            LlmProvider::Anthropic => "claude-sonnet-4-6",
        }
    }

    /// Default embedding model (only meaningful for OpenAI).
    pub fn default_embed_model(&self) -> &'static str {
        "text-embedding-3-small"
    }

    fn env_key_var(&self) -> &'static str {
        match self {
            LlmProvider::OpenAi => "OPENAI_API_KEY",
            LlmProvider::Anthropic => "ANTHROPIC_API_KEY",
        }
    }
}

/// Configuration for NlqContext.
#[derive(Debug, Clone)]
pub struct NlqConfig {
    /// Which LLM provider to use. Defaults to OpenAI; set `LLM_PROVIDER=anthropic` for Claude.
    pub provider: LlmProvider,
    /// API key for the selected provider (`OPENAI_API_KEY` or `ANTHROPIC_API_KEY`).
    pub api_key: String,
    /// API base URL. Defaults to the provider's standard endpoint; override for proxies.
    pub base_url: String,
    /// Model used for planning and agent evaluation.
    pub model: String,
    /// Model used for embedding generation (OpenAI only).
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
        let provider = LlmProvider::from_env();
        let api_key = std::env::var(provider.env_key_var()).unwrap_or_default();
        let base_url = provider.default_base_url().to_string();
        let model = provider.default_model().to_string();
        let embed_model = provider.default_embed_model().to_string();
        Self {
            provider,
            api_key,
            base_url,
            model,
            embed_model,
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
        if self.api_key.is_empty() {
            let var = match self.provider {
                LlmProvider::OpenAi => "OPENAI_API_KEY",
                LlmProvider::Anthropic => "ANTHROPIC_API_KEY",
            };
            return Err(crate::errors::NlqError::Config(format!(
                "{var} is not set; set the env var or supply it in NlqConfig"
            )));
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
