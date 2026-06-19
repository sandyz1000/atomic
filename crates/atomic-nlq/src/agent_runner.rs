use std::sync::Arc;

use atomic_compute::AgentRunner;
use atomic_data::distributed::{AgentFindings, AgentStepPayload, WireDecode as _, WireEncode as _};

use crate::config::{LlmProvider, NlqConfig};
use crate::llm::LlmClient;
use crate::llm::anthropic::AnthropicClient;
use crate::openai::OpenAiClient;

/// Concrete [`AgentRunner`] implementation backed by atomic-nlq's LlmClient.
///
/// Registered once at startup via [`register`]; the `NativeDispatcher` looks it
/// up in `AGENT_RUNNER_REGISTRY` when it sees a [`TaskAction::AgentStep`] op.
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
        let mut budget_exceeded = false;

        let tool_hint = if payload.tool_refs.is_empty() {
            String::new()
        } else {
            format!(
                "\n\nAvailable tools you may reference: {}.",
                payload.tool_refs.join(", ")
            )
        };
        let system = format!("{}{}", payload.system_prompt, tool_hint);

        for round in 0..payload.max_rounds {
            if let Some(max) = payload.max_tokens_total {
                if *tokens_used >= max {
                    budget_exceeded = true;
                    break;
                }
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
                    answer = format!("error: {e}");
                    break;
                }
            };

            // Approximate token accounting: ~4 chars per token.
            *tokens_used += ((user_msg.len() + response.len()) / 4) as u64;

            conversation = format!("{conversation}\n\nRound {}: {response}", round + 1);
            answer = response;
            rounds_done = round + 1;

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
            confidence: 1.0,
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
            if let Some(max) = payload.max_tokens_total {
                if tokens_used >= max {
                    findings.push(AgentFindings {
                        input_id: idx,
                        answer: String::new(),
                        rounds: 0,
                        confidence: 0.0,
                        budget_exceeded: true,
                    });
                    continue;
                }
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

        // Dispatch runs inside spawn_blocking; a Tokio runtime is reachable.
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|e| format!("AgentStep: no tokio runtime available: {e}"))?;

        let findings = handle.block_on(Self::run_partition_async(payload, strings));

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
    use super::decode_string_partition;
    use atomic_data::distributed::WireEncode as _;

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
