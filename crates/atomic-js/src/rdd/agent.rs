use atomic_data::distributed::{
    AgentFindings, AgentStepPayload, OpKind, PipelineOp, ResolvedTool, ScriptRuntime, StepKind,
    TaskRuntime, WireDecode,
};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde_json::Value as JsonValue;

use super::JsRdd;

#[napi]
impl JsRdd {
    /// Run a framework-native, multi-round LLM agent loop over each partition.
    ///
    /// `config` is an object describing the agent:
    ///   - `model` (string, required)            — e.g. `"gpt-4o-mini"`
    ///   - `systemPrompt` (string, required)      — the agent's task description
    ///   - `maxRounds` (number, default 2)        — plan→execute→evaluate rounds per input
    ///   - `provider` (string, default "openai")  — `"openai"` or `"anthropic"`
    ///   - `toolRefs` (string[], default [])      — names of Rust `#[task]` tools the agent may call
    ///   - `tools` (object[], default [])         — inline JS tools shipped with the job (no rebuild).
    ///     Each: `{ name: string, source: string }` where `source` is a function expression
    ///     `(args) => result`. The model calls them via `TOOL_CALL: <name> <json>`.
    ///   - `outputSchema` (string, optional)      — JSON schema for best-effort output validation
    ///   - `maxTokensTotal` (number, optional)    — token budget across all inputs in a partition
    ///
    /// Each RDD element must be a string. Returns an array of objects, one per input element:
    ///   `{ inputId, answer, rounds, confidence, budgetExceeded }`
    ///
    /// Requires the agent runner to be registered (done automatically at module load)
    /// and one of `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` set in the environment.
    #[napi(
        ts_return_type = "Array<{ inputId: number, answer: string, rounds: number, confidence: number, budgetExceeded: boolean }>"
    )]
    pub fn agent_step(&self, config: JsonValue) -> Result<JsonValue> {
        let payload = parse_agent_config(&config)?;
        let payload_bytes = serde_json::to_vec(&payload).map_err(|e| {
            Error::from_reason(format!("agentStep: failed to serialize config: {e}"))
        })?;

        // Partitions are JSON-encoded; `PartitionAgentRunner::decode_string_partition`
        // accepts both rkyv (Rust callers) and JSON arrays (Python/JS callers).
        let source_partitions = self.encode_source_partitions()?;

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
            .map_err(|e| Error::from_reason(format!("agentStep: {e}")))?;

        let mut out = Vec::new();
        for bytes in result_bytes {
            let findings = Vec::<AgentFindings>::decode_wire(&bytes).map_err(|e| {
                Error::from_reason(format!("agentStep: failed to decode findings: {e}"))
            })?;
            for f in findings {
                out.push(serde_json::json!({
                    "inputId": f.input_id,
                    "answer": f.answer,
                    "rounds": f.rounds,
                    "confidence": f.confidence,
                    "budgetExceeded": f.budget_exceeded,
                }));
            }
        }
        Ok(JsonValue::Array(out))
    }
}

fn parse_agent_config(config: &JsonValue) -> Result<AgentStepPayload> {
    let obj = config
        .as_object()
        .ok_or_else(|| Error::from_reason("agentStep: config must be an object"))?;

    let required_str = |key: &str| -> Result<String> {
        obj.get(key)
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| {
                Error::from_reason(format!("agentStep: config is missing required key '{key}'"))
            })
    };

    let model = required_str("model")?;
    let system_prompt = required_str("systemPrompt")?;
    let max_rounds = obj
        .get("maxRounds")
        .and_then(JsonValue::as_u64)
        .map(|v| v as u32)
        .unwrap_or(2);
    let provider = obj
        .get("provider")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| "openai".to_string());
    let mut tool_refs: Vec<String> = obj
        .get("toolRefs")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    // Inline JS tools shipped with the job (the scripted lane — no rebuild).
    // Each is `{ name, source }`; `source` is a function expression `(args) => result`.
    let mut resolved_tools: Vec<ResolvedTool> = Vec::new();
    if let Some(tools) = obj.get("tools") {
        let arr = tools
            .as_array()
            .ok_or_else(|| Error::from_reason("agentStep: 'tools' must be an array of objects"))?;
        for item in arr {
            let tool = item
                .as_object()
                .ok_or_else(|| Error::from_reason("agentStep: each tool must be an object"))?;
            let name = tool
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::from_reason("agentStep: tool is missing 'name'"))?;
            let source = tool
                .get("source")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::from_reason("agentStep: tool is missing 'source'"))?;
            tool_refs.push(name.to_string());
            resolved_tools.push(ResolvedTool {
                name: name.to_string(),
                runtime: ScriptRuntime::JavaScript,
                source: source.to_string(),
            });
        }
    }

    let output_schema = obj
        .get("outputSchema")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let max_tokens_total = obj.get("maxTokensTotal").and_then(JsonValue::as_u64);

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
