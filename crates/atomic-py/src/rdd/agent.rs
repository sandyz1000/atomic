use atomic_data::distributed::{
    AgentFindings, AgentStepPayload, OpKind, PipelineOp, ResolvedTool, ScriptRuntime, StepKind,
    TaskRuntime, WireDecode,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use super::PyRdd;

#[pymethods]
impl PyRdd {
    /// Run a framework-native, multi-round LLM agent loop over each partition.
    ///
    /// `config` is a dict describing the agent:
    ///   - `model` (str, required)            — e.g. `"gpt-4o-mini"`
    ///   - `system_prompt` (str, required)    — the agent's task description
    ///   - `max_rounds` (int, default 2)       — plan→execute→evaluate rounds per input
    ///   - `provider` (str, default "openai")  — `"openai"` or `"anthropic"`
    ///   - `tool_refs` (list[str], default []) — names of Rust `#[task]` tools the agent may call
    ///   - `tools` (list[dict], default [])    — inline Python tools shipped with the job
    ///     (no rebuild). Each: `{"name": str, "source": str}` where `source` defines a
    ///     top-level `run(args)` function. The model calls them via `TOOL_CALL: <name> <json>`.
    ///   - `output_schema` (str, optional)     — JSON schema for best-effort output validation
    ///   - `max_tokens_total` (int, optional)  — token budget across all inputs in a partition
    ///
    /// Each RDD element must be a string. Returns a list of dicts, one per input element:
    ///   `{"input_id": int, "answer": str, "rounds": int, "confidence": float, "budget_exceeded": bool}`
    ///
    /// Requires the agent runner to be registered (done automatically at module import)
    /// and one of `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` set in the environment.
    #[pyo3(signature = (config))]
    pub fn agent_step(&self, py: Python, config: &Bound<'_, PyDict>) -> PyResult<Py<PyAny>> {
        let payload = parse_agent_config(config)?;
        let payload_bytes = serde_json::to_vec(&payload).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "agent_step: failed to serialize config: {e}"
            ))
        })?;

        // Partitions are JSON-encoded; `PartitionAgentRunner::decode_string_partition`
        // accepts both rkyv (Rust callers) and JSON arrays (Python/JS callers).
        let source_partitions = self.encode_source_partitions(py)?;

        let op = PipelineOp {
            op_id: String::new(),
            kind: OpKind::Engine(StepKind::AgentStep),
            runtime: TaskRuntime::Native,
            payload: payload_bytes,
        };

        // `dispatch_pipeline` routes through the same NativeDispatcher in both local
        // and distributed mode, so no separate local-mode fast path is needed here.
        let result_bytes = self
            .context
            .dispatch_pipeline(source_partitions, vec![op])
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("agent_step: {e}")))?;

        let out = PyList::empty(py);
        for bytes in result_bytes {
            let findings = Vec::<AgentFindings>::decode_wire(&bytes).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "agent_step: failed to decode findings: {e}"
                ))
            })?;
            for f in findings {
                let d = PyDict::new(py);
                d.set_item("input_id", f.input_id)?;
                d.set_item("answer", f.answer)?;
                d.set_item("rounds", f.rounds)?;
                d.set_item("confidence", f.confidence)?;
                d.set_item("budget_exceeded", f.budget_exceeded)?;
                out.append(d)?;
            }
        }
        Ok(out.into_any().unbind())
    }
}

fn parse_agent_config(config: &Bound<'_, PyDict>) -> PyResult<AgentStepPayload> {
    let required_str = |key: &str| -> PyResult<String> {
        config
            .get_item(key)?
            .ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(format!(
                    "agent_step: config is missing required key '{key}'"
                ))
            })?
            .extract()
    };
    let optional = |key: &str| -> PyResult<Option<Bound<'_, PyAny>>> { config.get_item(key) };

    let model = required_str("model")?;
    let system_prompt = required_str("system_prompt")?;
    let max_rounds: u32 = optional("max_rounds")?
        .map(|v| v.extract())
        .transpose()?
        .unwrap_or(2);
    let provider: String = optional("provider")?
        .map(|v| v.extract())
        .transpose()?
        .unwrap_or_else(|| "openai".to_string());
    let mut tool_refs: Vec<String> = optional("tool_refs")?
        .map(|v| v.extract())
        .transpose()?
        .unwrap_or_default();

    // Inline Python tools shipped with the job (the scripted lane — no rebuild).
    // Each is `{"name": str, "source": str}`; `source` defines a top-level `run(args)`.
    let mut resolved_tools: Vec<ResolvedTool> = Vec::new();
    if let Some(tools) = optional("tools")? {
        let list = tools.cast::<PyList>().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err("agent_step: 'tools' must be a list of dicts")
        })?;
        for item in list.iter() {
            let tool = item.cast::<PyDict>().map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err("agent_step: each tool must be a dict")
            })?;
            let name: String = tool
                .get_item("name")?
                .ok_or_else(|| {
                    pyo3::exceptions::PyKeyError::new_err("agent_step: tool is missing 'name'")
                })?
                .extract()?;
            let source: String = tool
                .get_item("source")?
                .ok_or_else(|| {
                    pyo3::exceptions::PyKeyError::new_err("agent_step: tool is missing 'source'")
                })?
                .extract()?;
            tool_refs.push(name.clone());
            resolved_tools.push(ResolvedTool {
                name,
                runtime: ScriptRuntime::Python,
                source,
            });
        }
    }

    let output_schema: Option<String> = optional("output_schema")?
        .map(|v| v.extract())
        .transpose()?;
    let max_tokens_total: Option<u64> = optional("max_tokens_total")?
        .map(|v| v.extract())
        .transpose()?;

    Ok(AgentStepPayload {
        model,
        system_prompt,
        max_rounds,
        tool_refs,
        resolved_tools,
        provider,
        output_schema,
        max_tokens_total,
    })
}
