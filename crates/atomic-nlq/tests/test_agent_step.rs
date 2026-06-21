use atomic_compute::context::Context;
use atomic_compute::task_registry::AGENT_RUNNER_REGISTRY;
use atomic_data::distributed::{AgentFindings, AgentStepPayload, WireDecode as _, WireEncode as _};
use atomic_nlq::agent_runner;

fn register_once() {
    agent_runner::register();
}

fn has_api_key() -> bool {
    std::env::var("OPENAI_API_KEY").is_ok() || std::env::var("ANTHROPIC_API_KEY").is_ok()
}

/// Build a tokio single-threaded runtime and enter its scope so that
/// `Handle::try_current()` succeeds inside `run_partition` — then call
/// `run_partition` synchronously (it blocks inside on the handle).
fn with_runtime<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let _guard = rt.enter();
    f()
}

/// Registry wiring only — no network call, must always run fast and offline.
#[test]
fn agent_step_runner_registered() {
    register_once();
    assert!(
        AGENT_RUNNER_REGISTRY.get().is_some(),
        "AGENT_RUNNER_REGISTRY must be populated after register()"
    );
}

/// Exercises the full dispatch path (decode -> LLM call -> encode). Requires a
/// real API key since `PartitionAgentRunner` always builds a live LLM client
/// (no provider-mock seam exists yet — see CLAUDE.md atomic-nlq guardrails).
#[test]
fn agent_step_dispatch_via_runner() {
    if !has_api_key() {
        eprintln!("agent_step_dispatch_via_runner: skipped (no API key)");
        return;
    }
    register_once();
    let runner = AGENT_RUNNER_REGISTRY.get().unwrap();

    let provider = if std::env::var("OPENAI_API_KEY").is_ok() {
        "openai"
    } else {
        "anthropic"
    };
    let model = if provider == "anthropic" {
        "claude-haiku-4-5-20251001"
    } else {
        "gpt-4o-mini"
    };

    let payload = AgentStepPayload {
        model: model.to_string(),
        system_prompt: "Echo the input back in one short sentence.".to_string(),
        max_rounds: 1,
        tool_refs: vec![],
        resolved_tools: vec![],
        provider: provider.to_string(),
        output_schema: None,
        max_tokens_total: None,
    };

    let inputs: Vec<String> = vec!["hello".to_string(), "world".to_string()];
    let encoded = inputs
        .encode_wire()
        .expect("encode_wire for Vec<String> failed");

    let result_bytes = with_runtime(|| runner.run_partition(&payload, &encoded))
        .expect("run_partition returned Err");

    let findings =
        Vec::<AgentFindings>::decode_wire(&result_bytes).expect("decode AgentFindings failed");
    assert_eq!(findings.len(), 2, "one finding per input");
    assert_eq!(findings[0].input_id, 0);
    assert_eq!(findings[1].input_id, 1);
}

/// JSON-encoded inputs (Python/JS PyRdd format) — exercises the fallback decode
/// path through the real dispatch; requires an API key for the same reason as above.
#[test]
fn agent_step_json_partition_decode() {
    if !has_api_key() {
        eprintln!("agent_step_json_partition_decode: skipped (no API key)");
        return;
    }
    register_once();
    let runner = AGENT_RUNNER_REGISTRY.get().expect("runner registered");

    let provider = if std::env::var("OPENAI_API_KEY").is_ok() {
        "openai"
    } else {
        "anthropic"
    };
    let model = if provider == "anthropic" {
        "claude-haiku-4-5-20251001"
    } else {
        "gpt-4o-mini"
    };

    let payload = AgentStepPayload {
        model: model.to_string(),
        system_prompt: "Reply with one short sentence.".to_string(),
        max_rounds: 1,
        tool_refs: vec![],
        resolved_tools: vec![],
        provider: provider.to_string(),
        output_schema: None,
        max_tokens_total: None,
    };

    let json_bytes = serde_json::to_vec(&["doc_a", "doc_b"]).unwrap();

    let result_bytes = with_runtime(|| runner.run_partition(&payload, &json_bytes))
        .expect("run_partition with JSON partition failed");
    let findings = Vec::<AgentFindings>::decode_wire(&result_bytes).unwrap();
    assert_eq!(findings.len(), 2);
}

/// Output-schema validation requires a real model response to validate against.
#[test]
fn agent_step_output_schema_validation() {
    if !has_api_key() {
        eprintln!("agent_step_output_schema_validation: skipped (no API key)");
        return;
    }
    register_once();
    let runner = AGENT_RUNNER_REGISTRY.get().expect("runner registered");

    let provider = if std::env::var("OPENAI_API_KEY").is_ok() {
        "openai"
    } else {
        "anthropic"
    };
    let model = if provider == "anthropic" {
        "claude-haiku-4-5-20251001"
    } else {
        "gpt-4o-mini"
    };

    let payload = AgentStepPayload {
        model: model.to_string(),
        system_prompt: "Respond with free text.".to_string(),
        max_rounds: 1,
        tool_refs: vec![],
        resolved_tools: vec![],
        provider: provider.to_string(),
        output_schema: Some(r#"{"type":"object"}"#.to_string()),
        max_tokens_total: None,
    };

    let inputs = vec!["some document".to_string()];
    let encoded = inputs.encode_wire().expect("encode_wire failed");

    let result_bytes =
        with_runtime(|| runner.run_partition(&payload, &encoded)).expect("run_partition failed");
    let findings = Vec::<AgentFindings>::decode_wire(&result_bytes).unwrap();
    assert_eq!(findings.len(), 1);
    assert!(!findings[0].answer.is_empty());
}

/// Full local-mode pipeline test (requires an API key; auto-skips when absent).
#[test]
fn agent_step_local_mode_e2e() {
    if !has_api_key() {
        eprintln!("agent_step_local_mode_e2e: skipped (no API key)");
        return;
    }
    register_once();

    let provider = if std::env::var("OPENAI_API_KEY").is_ok() {
        "openai"
    } else {
        "anthropic"
    };
    let model = if provider == "anthropic" {
        "claude-haiku-4-5-20251001"
    } else {
        "gpt-4o-mini"
    };

    let ctx = Context::local().expect("failed to build local context");
    let docs = vec![
        "Party A shall pay $100 within 30 days.".to_string(),
        "Party B shall deliver goods by December 31.".to_string(),
    ];
    let config = AgentStepPayload {
        model: model.to_string(),
        system_prompt: "Extract the obligation as one sentence.".to_string(),
        max_rounds: 2,
        tool_refs: vec![],
        resolved_tools: vec![],
        provider: provider.to_string(),
        output_schema: None,
        max_tokens_total: Some(10_000),
    };

    let findings = ctx
        .parallelize_typed(docs, 2)
        .agent_step(config)
        .collect()
        .expect("agent_step collect failed");

    assert_eq!(findings.len(), 2);
    for f in &findings {
        assert!(!f.answer.is_empty(), "answer must not be empty");
        assert!(f.rounds >= 1);
    }
}
