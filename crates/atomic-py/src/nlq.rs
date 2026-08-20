//! Python bindings for `atomic-nlq` — natural-language queries over registered tables.

use std::sync::Arc;

use atomic_nlq::config::LlmProvider;
use atomic_nlq::{NlqConfig, NlqContext};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::sql::{PySqlContext, run_sql_async, to_py_err};

fn provider_from_str(name: &str) -> PyResult<LlmProvider> {
    LlmProvider::parse(name).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "unknown provider {name:?}; expected \"openai\" or \"anthropic\""
        ))
    })
}

/// Entry point for natural-language queries against Atomic.
///
/// ```python
/// import atomic_compute as ac
///
/// ctx = ac.NlqContext(api_key="sk-...")
/// ctx.sql_ctx().register_batches("orders", batches)
/// result = ctx.query("what is the total revenue per category")
/// print(result["answer"])
/// ```
#[pyclass(name = "NlqContext")]
pub struct PyNlqContext {
    inner: Arc<NlqContext>,
}

#[pymethods]
impl PyNlqContext {
    /// Build against a fresh local compute context.
    ///
    /// `api_key` defaults to the `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` environment
    /// variable (matching `provider`) when not given. Every other argument falls back
    /// to `NlqConfig`'s defaults.
    #[new]
    #[pyo3(signature = (api_key=None, provider="openai", model=None, base_url=None, max_rounds=None))]
    pub fn new(
        api_key: Option<String>,
        provider: &str,
        model: Option<String>,
        base_url: Option<String>,
        max_rounds: Option<usize>,
    ) -> PyResult<Self> {
        let provider = provider_from_str(provider)?;
        let mut config = NlqConfig {
            provider,
            ..NlqConfig::default()
        };
        if let Some(key) = api_key {
            config.api_key = key;
        }
        if let Some(m) = model {
            config.model = m;
        }
        if let Some(u) = base_url {
            config.base_url = u;
        }
        if let Some(r) = max_rounds {
            config.max_rounds = r;
        }
        let inner = NlqContext::build(config).map_err(to_py_err)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// The underlying `SqlContext` — register tables here before calling `query`.
    pub fn sql_ctx(&self) -> PySqlContext {
        PySqlContext::from_context(self.inner.sql_ctx.clone())
    }

    /// Translate a natural-language query into an executed result.
    ///
    /// Returns a dict: `{"answer": str, "rounds": int, "steps": [{"step_id": str,
    /// "text": str | None}]}`.
    pub fn query(&self, py: Python<'_>, nl: &str) -> PyResult<Py<PyDict>> {
        let inner = self.inner.clone();
        let nl = nl.to_string();
        let result = run_sql_async(async move { inner.query(&nl).await }).map_err(to_py_err)?;

        let out = PyDict::new(py);
        out.set_item("answer", &result.answer)?;
        out.set_item("rounds", result.rounds)?;
        let steps = PyList::empty(py);
        for step in &result.steps {
            let step_dict = PyDict::new(py);
            step_dict.set_item("step_id", &step.step_id)?;
            let text = match &step.output {
                atomic_nlq::workflow::StepOutput::Text(t) => Some(t.clone()),
                atomic_nlq::workflow::StepOutput::DataFrame(batches) => {
                    Some(format!("<DataFrame: {} partition(s)>", batches.len()))
                }
                atomic_nlq::workflow::StepOutput::Empty => None,
            };
            step_dict.set_item("text", text)?;
            steps.append(step_dict)?;
        }
        out.set_item("steps", steps)?;
        Ok(out.unbind())
    }

    /// Dry-run: return the JSON-serialized `WorkflowPlan` the LLM would produce for
    /// `nl`, without executing it. Useful for debugging tool/SQL step selection.
    pub fn plan(&self, nl: &str) -> PyResult<String> {
        let inner = self.inner.clone();
        let nl = nl.to_string();
        let plan = run_sql_async(async move { inner.plan(&nl).await }).map_err(to_py_err)?;
        serde_json::to_string_pretty(&plan).map_err(to_py_err)
    }
}
