//! Node.js bindings for `atomic-nlq` — natural-language queries over registered tables.

use std::sync::Arc;

use atomic_nlq::config::LlmProvider;
use atomic_nlq::{NlqConfig, NlqContext};
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::sql::{JsSqlContext, run_sql_async, to_js_err};

fn provider_from_str(name: &str) -> Result<LlmProvider> {
    LlmProvider::parse(name).ok_or_else(|| {
        Error::from_reason(format!(
            "unknown provider {name:?}; expected \"openai\" or \"anthropic\""
        ))
    })
}

/// Options for constructing an `NlqContext`. All fields optional; unset ones fall back
/// to `NlqConfig`'s defaults (which read `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` /
/// `LLM_PROVIDER` from the environment).
#[napi(object)]
#[derive(Default)]
pub struct NlqContextOptions {
    pub api_key: Option<String>,
    /// `"openai"` (default) or `"anthropic"`.
    pub provider: Option<String>,
    pub model: Option<String>,
    pub base_url: Option<String>,
    pub max_rounds: Option<u32>,
}

/// Entry point for natural-language queries against Atomic.
///
/// ```javascript
/// const { NlqContext } = require('atomic-compute');
///
/// const ctx = new NlqContext({ apiKey: 'sk-...' });
/// ctx.sqlCtx().registerCsv('orders', 'orders.csv');
/// const result = ctx.query('what is the total revenue per category');
/// console.log(result.answer);
/// ```
#[napi(js_name = "NlqContext")]
pub struct JsNlqContext {
    inner: Arc<NlqContext>,
}

#[napi]
impl JsNlqContext {
    /// Build against a fresh local compute context.
    #[napi(constructor)]
    pub fn new(options: Option<NlqContextOptions>) -> Result<Self> {
        let options = options.unwrap_or_default();
        let provider = provider_from_str(options.provider.as_deref().unwrap_or("openai"))?;
        let mut config = NlqConfig {
            provider,
            ..NlqConfig::default()
        };
        if let Some(key) = options.api_key {
            config.api_key = key;
        }
        if let Some(m) = options.model {
            config.model = m;
        }
        if let Some(u) = options.base_url {
            config.base_url = u;
        }
        if let Some(r) = options.max_rounds {
            config.max_rounds = r as usize;
        }
        let inner = NlqContext::build(config).map_err(to_js_err)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// The underlying `SqlContext` — register tables here before calling `query`.
    #[napi]
    pub fn sql_ctx(&self) -> JsSqlContext {
        JsSqlContext::from_context(self.inner.sql_ctx.clone())
    }

    /// Translate a natural-language query into an executed result:
    /// `{ answer, rounds, steps: [{ stepId, text }] }`.
    #[napi]
    pub fn query(&self, nl: String) -> Result<serde_json::Value> {
        let inner = self.inner.clone();
        let result = run_sql_async(async move { inner.query(&nl).await }).map_err(to_js_err)?;

        let steps: Vec<serde_json::Value> = result
            .steps
            .iter()
            .map(|step| {
                let text = match &step.output {
                    atomic_nlq::workflow::StepOutput::Text(t) => {
                        Some(serde_json::Value::String(t.clone()))
                    }
                    atomic_nlq::workflow::StepOutput::DataFrame(batches) => {
                        Some(serde_json::Value::String(format!(
                            "<DataFrame: {} partition(s)>",
                            batches.len()
                        )))
                    }
                    atomic_nlq::workflow::StepOutput::Empty => None,
                };
                serde_json::json!({
                    "stepId": step.step_id,
                    "text": text,
                })
            })
            .collect();

        Ok(serde_json::json!({
            "answer": result.answer,
            "rounds": result.rounds,
            "steps": steps,
        }))
    }

    /// Dry-run: return the JSON-serialized `WorkflowPlan` the LLM would produce for
    /// `nl`, without executing it. Useful for debugging tool/SQL step selection.
    #[napi]
    pub fn plan(&self, nl: String) -> Result<String> {
        let inner = self.inner.clone();
        let plan = run_sql_async(async move { inner.plan(&nl).await }).map_err(to_js_err)?;
        serde_json::to_string_pretty(&plan).map_err(to_js_err)
    }
}
