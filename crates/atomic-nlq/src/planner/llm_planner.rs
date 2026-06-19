use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;

use crate::config::NlqConfig;
use crate::errors::{NlqError, Result};
use crate::llm::LlmClient;
use crate::registry::ToolRegistry;
use crate::workflow::{AgentContext, WorkflowPlan};

use super::prompt::build_plan_prompt;

fn sanitize_nl_query(q: &str) -> String {
    q.chars()
        .filter(|c| !c.is_ascii_control() || *c == '\n' || *c == '\t')
        .take(2000)
        .collect()
}

pub struct LlmPlanner {
    client: Arc<dyn LlmClient>,
    config: Arc<NlqConfig>,
}

impl LlmPlanner {
    pub fn new(client: Arc<dyn LlmClient>, config: Arc<NlqConfig>) -> Self {
        Self { client, config }
    }

    /// Expose the client so `AgentLoop` can reuse it for the evaluation step.
    pub fn client(&self) -> &dyn LlmClient {
        self.client.as_ref()
    }

    /// Translate a natural language query into a `WorkflowPlan`.
    ///
    /// The LLM receives the table schemas, registered tools, and any prior-round
    /// results, then outputs a JSON dependency graph of tool calls.
    pub async fn plan(
        &self,
        nl_query: &str,
        tables: &HashMap<String, SchemaRef>,
        tools: &ToolRegistry,
        ctx: &AgentContext,
    ) -> Result<WorkflowPlan> {
        let system = build_plan_prompt(tables, tools, ctx);
        let user = sanitize_nl_query(nl_query);

        let mut last_err = String::new();
        for attempt in 0..=2u32 {
            let user_msg = if attempt == 0 {
                user.clone()
            } else {
                format!(
                    "{user}\n\n[Previous attempt produced invalid JSON: {last_err}. Output ONLY valid JSON this time.]"
                )
            };

            log::debug!("LlmPlanner: plan attempt {}", attempt + 1);
            let text = self
                .client
                .chat_with_retry(&self.config.model, &system, &user_msg, 4096)
                .await?;

            // Strip markdown code fences if the model wrapped the JSON.
            let json_str = strip_code_fence(&text);

            match serde_json::from_str::<WorkflowPlan>(json_str) {
                Ok(plan) => {
                    log::debug!(
                        "LlmPlanner: plan with {} steps on attempt {}",
                        plan.steps.len(),
                        attempt + 1
                    );
                    return Ok(plan);
                }
                Err(e) => {
                    last_err = format!("{e}; got: {}", &text[..text.len().min(300)]);
                    log::warn!("LlmPlanner: attempt {} parse error: {e}", attempt + 1);
                }
            }
        }
        Err(NlqError::PlanParse(last_err))
    }
}

fn strip_code_fence(s: &str) -> &str {
    let s = s.trim();
    let s = s.strip_prefix("```json").unwrap_or(s);
    let s = s.strip_prefix("```").unwrap_or(s);
    let s = s.strip_suffix("```").unwrap_or(s);
    s.trim()
}
