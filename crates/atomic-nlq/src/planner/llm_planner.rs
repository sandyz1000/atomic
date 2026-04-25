use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use serde_json::Value;

use crate::anthropic::client::AnthropicClient;
use crate::anthropic::retry::messages_with_retry;
use crate::anthropic::types::{MessagesRequest, RequestMessage};
use crate::config::NlqConfig;
use crate::errors::{NlqError, Result};
use crate::registry::UdfDescription;

use super::{prompt::build_system_prompt, schema_encoder::encode_schema};

/// Truncate to 2000 chars and strip ASCII control characters to prevent prompt injection.
fn sanitize_nl_query(q: &str) -> String {
    q.chars()
        .filter(|c| !c.is_ascii_control() || *c == '\n' || *c == '\t')
        .take(2000)
        .collect()
}

pub struct LlmPlanner {
    client: Arc<AnthropicClient>,
    config: Arc<NlqConfig>,
}

impl LlmPlanner {
    pub fn new(client: Arc<AnthropicClient>, config: Arc<NlqConfig>) -> Self {
        Self { client, config }
    }

    /// Translate a natural language query into a raw JSON plan tree.
    pub async fn plan(
        &self,
        nl_query: &str,
        tables: &[(String, SchemaRef)],
        udfs: &[UdfDescription],
    ) -> Result<Value> {
        let schema_json = encode_schema(tables);
        let system = build_system_prompt(&schema_json, udfs);

        // Sanitize: truncate to 2000 chars, strip control chars.
        let nl_query = sanitize_nl_query(nl_query);
        let nl_query = nl_query.as_str();

        let mut last_err: Option<String> = None;
        for attempt in 0..=2u32 {
            // On retry, add the previous parse error as feedback so the LLM can self-correct.
            let user_content = match &last_err {
                None => nl_query.to_string(),
                Some(prev) => format!(
                    "{nl_query}\n\n[Previous attempt produced invalid JSON: {prev}. Output ONLY valid JSON this time.]"
                ),
            };

            let req = MessagesRequest {
                model: self.config.default_model.clone(),
                max_tokens: 4096,
                system: Some(system.clone()),
                messages: vec![RequestMessage {
                    role: "user".to_string(),
                    content: user_content,
                }],
            };

            log::debug!("LlmPlanner: plan attempt {}", attempt + 1);
            let resp = messages_with_retry(&self.client, req, self.config.max_retries).await?;
            let text = resp
                .content
                .into_iter()
                .find_map(|block| match block {
                    crate::anthropic::types::ContentBlock::Text { text } => Some(text),
                    _ => None,
                })
                .unwrap_or_default();

            match serde_json::from_str::<Value>(&text) {
                Ok(json) => {
                    log::debug!("LlmPlanner: plan parsed successfully on attempt {}", attempt + 1);
                    return Ok(json);
                }
                Err(e) => {
                    let msg = format!("{e}; got: {text}");
                    log::warn!("LlmPlanner: attempt {} JSON parse error: {e}", attempt + 1);
                    last_err = Some(msg);
                }
            }
        }
        Err(NlqError::PlanParse(last_err.unwrap_or_default()))
    }
}
