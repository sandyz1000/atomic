use std::sync::Arc;

use atomic_compute::AgentRunner;
use atomic_compute::task_registry;
use atomic_data::distributed::{
    AgentFindings, AgentStepPayload, ScriptRuntime, WireDecode as _, WireEncode as _,
};

use crate::config::{LlmProvider, NlqConfig};
use crate::llm::LlmClient;
use crate::llm::anthropic::AnthropicClient;
use crate::openai::OpenAiClient;

/// Parses a `TOOL_CALL: <tool_ref> <json_args>` marker out of an LLM response.
///
/// Everything after the marker is treated as `<tool_ref>` (first whitespace-delimited
/// token) followed by `<json_args>` (the remainder, trimmed — may itself contain
/// newlines if the model wrapped a JSON object). Returns `None` if no marker is present
/// or the tool ref is empty.
fn parse_tool_call(response: &str) -> Option<(String, String)> {
    const MARKER: &str = "TOOL_CALL:";
    let idx = response.find(MARKER)?;
    let rest = response[idx + MARKER.len()..].trim_start();
    let mut parts = rest.splitn(2, char::is_whitespace);
    let tool_ref = parts.next()?.trim();
    if tool_ref.is_empty() {
        return None;
    }
    let json_args = parts.next().unwrap_or("{}").trim();
    Some((tool_ref.to_string(), json_args.to_string()))
}

// cfg_python!/cfg_not_python!/cfg_js!/cfg_not_js! are defined once in `atomic_data` and
// shared workspace-wide — see that crate's `lib.rs`.
use atomic_data::{cfg_js, cfg_not_js, cfg_not_python, cfg_python};

cfg_python! {
    fn dispatch_python_tool(source: &str, args_json: &str) -> Result<String, String> {
        atomic_compute::runtimes::py::run_tool_call(source, args_json).map_err(|e| e.to_string())
    }
}

cfg_not_python! {
    fn dispatch_python_tool(_source: &str, _args_json: &str) -> Result<String, String> {
        Err("not built with 'python' feature; Python tools unavailable".to_string())
    }
}

cfg_js! {
    fn dispatch_js_tool(source: &str, args_json: &str) -> Result<String, String> {
        atomic_compute::runtimes::js::run_tool_call(source, args_json)
    }
}

cfg_not_js! {
    fn dispatch_js_tool(_source: &str, _args_json: &str) -> Result<String, String> {
        Err("not built with 'js' feature; JavaScript tools unavailable".to_string())
    }
}

/// Dispatch one `TOOL_CALL:` to a Rust `#[task]` (by op_id, via `TASK_REGISTRY`) or a
/// resolved Python/JS tool (via `AgentStepPayload.resolved_tools`). Always returns
/// a string — errors are formatted as `"error: ..."` and fed back into the conversation
/// rather than aborting the partition (per the design: a model that mistypes a tool ref
/// should get a chance to retry or give up, not crash the worker).
fn dispatch_tool(payload: &AgentStepPayload, tool_ref: &str, json_args: &str) -> String {
    if task_registry::TASK_REGISTRY.contains_key(tool_ref) {
        return match task_registry::invoke_str_task(tool_ref, json_args.to_string()) {
            Ok(out) => out,
            Err(e) => format!("error: {e}"),
        };
    }

    if let Some(tool) = payload.resolved_tools.iter().find(|t| t.name == tool_ref) {
        let result = match tool.runtime {
            ScriptRuntime::Python => dispatch_python_tool(&tool.source, json_args),
            ScriptRuntime::JavaScript => dispatch_js_tool(&tool.source, json_args),
        };
        return match result {
            Ok(out) => out,
            Err(e) => format!("error: {e}"),
        };
    }

    format!(
        "error: tool '{tool_ref}' is not registered (not a TASK_REGISTRY op_id and not in \
         resolved_tools) — check the tool name and retry, or proceed without it"
    )
}

/// Concrete [`AgentRunner`] implementation backed by atomic-nlq's LlmClient.
///
/// Registered once at startup via [`register`]; the `NativeDispatcher` looks it
/// up in `AGENT_RUNNER_REGISTRY` when it sees a [`StepKind::AgentStep`] op.
pub struct PartitionAgentRunner;

impl PartitionAgentRunner {
    /// Build an `LlmClient` for the given provider string and API key from env.
    fn build_client(provider: &str) -> Arc<dyn LlmClient> {
        let cfg = NlqConfig {
            provider: if provider == "anthropic" {
                LlmProvider::Anthropic
            } else {
                LlmProvider::OpenAi
            },
            ..NlqConfig::default()
        };
        match cfg.provider {
            LlmProvider::OpenAi => Arc::new(OpenAiClient::new(
                &cfg.api_key,
                &cfg.base_url,
                cfg.timeout_secs,
                cfg.max_retries,
            )),
            LlmProvider::Anthropic => Arc::new(AnthropicClient::new(
                &cfg.api_key,
                &cfg.base_url,
                cfg.timeout_secs,
                cfg.max_retries,
            )),
        }
    }

    async fn run_one_input(
        client: &Arc<dyn LlmClient>,
        payload: &AgentStepPayload,
        input_id: usize,
        input: &str,
        tokens_used: &mut u64,
    ) -> AgentFindings {
        let mut conversation = String::from(input);
        let mut answer = String::new();
        let mut rounds_done = 0u32;
        let mut confidence = 1.0;
        let mut budget_exceeded = false;

        let tool_hint = if payload.tool_refs.is_empty() {
            String::new()
        } else {
            format!(
                "\n\nAvailable tools: {}. To call one, respond with exactly one line: \
                 TOOL_CALL: <tool_name> <json_args>\nThen stop — you'll receive the tool's \
                 result and can continue or call another tool.",
                payload.tool_refs.join(", ")
            )
        };
        let system = format!("{}{}", payload.system_prompt, tool_hint);

        for round in 0..payload.max_rounds {
            if let Some(max) = payload.max_tokens_total
                && *tokens_used >= max
            {
                budget_exceeded = true;
                break;
            }

            let user_msg = if round == 0 {
                format!("Input:\n{input}\n\nProvide your analysis.")
            } else {
                format!("{conversation}\n\nContinue your analysis or provide a final answer.")
            };

            let response = match client
                .chat_with_retry(&payload.model, &system, &user_msg, 1024)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    // failed round: report 0 confidence.
                    answer = format!("error: {e}");
                    rounds_done = round + 1;
                    confidence = 0.0;
                    break;
                }
            };

            // Approximate token accounting: ~4 chars per token.
            *tokens_used += ((user_msg.len() + response.len()) / 4) as u64;

            conversation = format!("{conversation}\n\nRound {}: {response}", round + 1);
            answer = response;
            rounds_done = round + 1;

            // A tool call takes priority over a final answer in the same response —
            // dispatch it, feed the result back, and let the model continue or retry.
            if let Some((tool_ref, json_args)) = parse_tool_call(&answer) {
                let tool_result = dispatch_tool(payload, &tool_ref, &json_args);
                conversation = format!("{conversation}\n\nTool {tool_ref} result: {tool_result}");
                continue;
            }

            // If the response signals completion, stop early.
            if answer.contains("FINAL ANSWER:") || answer.contains("final answer:") {
                break;
            }
        }

        // Strip "FINAL ANSWER:" prefix if present.
        let clean_answer = answer
            .trim_start_matches("FINAL ANSWER:")
            .trim_start_matches("final answer:")
            .trim()
            .to_string();

        // If output_schema is set, validate the answer is JSON (best-effort).
        let validated_answer = if payload.output_schema.is_some() {
            if serde_json::from_str::<serde_json::Value>(&clean_answer).is_ok() {
                clean_answer
            } else {
                format!(
                    "{{\"error\":\"output did not match schema\",\"raw\":{}}}",
                    serde_json::to_string(&clean_answer).unwrap_or_default()
                )
            }
        } else {
            clean_answer
        };

        AgentFindings {
            input_id,
            answer: validated_answer,
            rounds: rounds_done as usize,
            confidence,
            budget_exceeded,
        }
    }

    async fn run_partition_async(
        payload: &AgentStepPayload,
        inputs: Vec<String>,
    ) -> Vec<AgentFindings> {
        let client = Self::build_client(&payload.provider);
        let mut findings = Vec::with_capacity(inputs.len());
        let mut tokens_used: u64 = 0;

        for (idx, input) in inputs.into_iter().enumerate() {
            if let Some(max) = payload.max_tokens_total
                && tokens_used >= max
            {
                findings.push(AgentFindings {
                    input_id: idx,
                    answer: String::new(),
                    rounds: 0,
                    confidence: 0.0,
                    budget_exceeded: true,
                });
                continue;
            }
            let finding =
                Self::run_one_input(&client, payload, idx, &input, &mut tokens_used).await;
            findings.push(finding);
        }
        findings
    }
}

/// Try to decode partition bytes as `Vec<String>`.
/// Rust `TypedRdd<String>` uses rkyv; Python/JS `PyRdd` uses JSON arrays.
fn decode_string_partition(bytes: &[u8]) -> Result<Vec<String>, String> {
    if let Ok(v) = Vec::<String>::decode_wire(bytes) {
        return Ok(v);
    }
    serde_json::from_slice::<Vec<serde_json::Value>>(bytes)
        .map(|vals| {
            vals.into_iter()
                .map(|v| match v {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                })
                .collect()
        })
        .map_err(|e| format!("AgentStep: failed to decode partition inputs as rkyv or JSON: {e}"))
}

impl AgentRunner for PartitionAgentRunner {
    fn run_partition(&self, payload: &AgentStepPayload, inputs: &[u8]) -> Result<Vec<u8>, String> {
        let strings = decode_string_partition(inputs)?;

        // On the worker path, dispatch runs inside spawn_blocking and an ambient
        // runtime is reachable. In local mode `agent_step` is called from a plain
        // sync `main`, so fall back to a temporary current-thread runtime.
        let findings = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.block_on(Self::run_partition_async(payload, strings)),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("AgentStep: failed to build runtime: {e}"))?;
                rt.block_on(Self::run_partition_async(payload, strings))
            }
        };

        // Encode Vec<AgentFindings> back to rkyv bytes.
        findings
            .encode_wire()
            .map_err(|e| format!("AgentStep: failed to encode findings: {e}"))
    }
}

/// Install [`PartitionAgentRunner`] into `AGENT_RUNNER_REGISTRY`.
///
/// Idempotent: safe to call from `#[pymodule]` or `#[module]` init that may run
/// more than once in a test process. Silently skips if already registered.
pub fn register() {
    atomic_compute::register_agent_runner(Box::new(PartitionAgentRunner));
}

#[cfg(test)]
mod tests {
    use super::{decode_string_partition, dispatch_tool, parse_tool_call};
    use atomic_compute::task_traits::UnaryTask;
    use atomic_data::distributed::{
        AgentStepPayload, ResolvedTool, ScriptRuntime, WireEncode as _,
    };

    #[atomic_compute::task]
    fn shout_tool(args: String) -> String {
        format!("{}!", args.trim_matches('"').to_uppercase())
    }

    fn base_payload() -> AgentStepPayload {
        AgentStepPayload {
            model: "test-model".to_string(),
            system_prompt: String::new(),
            max_rounds: 1,
            tool_refs: vec![],
            resolved_tools: vec![],
            provider: "openai".to_string(),
            output_schema: None,
            max_tokens_total: None,
        }
    }

    #[test]
    fn parse_tool_call_extracts_ref_and_args() {
        let response = "I need data.\nTOOL_CALL: my_tool {\"x\": 1}";
        let (tool_ref, json_args) = parse_tool_call(response).expect("should parse");
        assert_eq!(tool_ref, "my_tool");
        assert_eq!(json_args, "{\"x\": 1}");
    }

    #[test]
    fn parse_tool_call_defaults_empty_args() {
        let response = "TOOL_CALL: my_tool";
        let (tool_ref, json_args) = parse_tool_call(response).expect("should parse");
        assert_eq!(tool_ref, "my_tool");
        assert_eq!(json_args, "{}");
    }

    #[test]
    fn parse_tool_call_returns_none_without_marker() {
        assert!(parse_tool_call("FINAL ANSWER: done").is_none());
    }

    #[test]
    fn dispatch_tool_invokes_registered_rust_task() {
        let payload = base_payload();
        let op_id = ShoutTool::NAME;
        let result = dispatch_tool(&payload, op_id, "\"hello\"");
        assert_eq!(result, "HELLO!");
    }

    #[test]
    fn dispatch_tool_unresolved_returns_error_string() {
        let payload = base_payload();
        let result = dispatch_tool(&payload, "no_such_tool", "{}");
        assert!(result.starts_with("error:"), "got: {result}");
    }

    #[test]
    fn dispatch_tool_unresolved_python_runtime_returns_error_string() {
        let mut payload = base_payload();
        payload.resolved_tools.push(ResolvedTool {
            name: "py_tool".to_string(),
            runtime: ScriptRuntime::Python,
            source: "def run(args): return args".to_string(),
        });
        check_dispatch(&payload);
    }

    // Without the `python` feature compiled in, dispatch_python_tool returns an error
    // string rather than panicking — exactly the "binary not built with this feature"
    // path real deployments hit when features are off.
    atomic_data::cfg_not_python! {
        fn check_dispatch(payload: &AgentStepPayload) {
            let result = dispatch_tool(payload, "py_tool", "{}");
            assert!(result.starts_with("error:"), "got: {result}");
        }
    }
    atomic_data::cfg_python! {
        fn check_dispatch(_payload: &AgentStepPayload) {
            // exercised by the python feature's own integration tests
        }
    }

    #[test]
    fn decode_string_partition_rkyv_roundtrip() {
        let inputs = vec!["alpha".to_string(), "beta".to_string()];
        let encoded = inputs.encode_wire().expect("encode_wire failed");
        let decoded = decode_string_partition(&encoded).expect("decode_wire failed");
        assert_eq!(decoded, inputs);
    }

    #[test]
    fn decode_string_partition_json_fallback() {
        let json_bytes = serde_json::to_vec(&["doc_a", "doc_b"]).unwrap();
        let decoded = decode_string_partition(&json_bytes).expect("json decode failed");
        assert_eq!(decoded, vec!["doc_a".to_string(), "doc_b".to_string()]);
    }

    #[test]
    fn decode_string_partition_garbage_errors() {
        let garbage = vec![0xFF, 0x00, 0x13, 0x37];
        assert!(decode_string_partition(&garbage).is_err());
    }
}
