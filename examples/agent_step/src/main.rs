/// Distributed subagent example using `TypedRdd::agent_step`.
///
/// Runs a framework-native LLM agent loop over each partition of a document set.
/// Each partition is processed by a subagent that extracts key obligations from
/// a legal clause, returning one `AgentFindings` per input string.
///
/// # Prerequisites
///
/// Set one of the supported provider keys in the environment:
///
/// ```bash
/// export OPENAI_API_KEY=sk-...     # OpenAI (default provider)
/// export ANTHROPIC_API_KEY=sk-...  # Anthropic Claude
/// ```
///
/// If neither key is set, the example exits with a clear message.
///
/// # Run
///
/// ```bash
/// cargo run --example agent_step
/// ```
use atomic_compute::context::Context;
use atomic_data::distributed::AgentStepPayload;
use std::env;

fn main() {
    // Register the agent runner before constructing the context.
    atomic_nlq::agent_runner::register();

    let provider = if env::var("OPENAI_API_KEY").is_ok() {
        "openai"
    } else if env::var("ANTHROPIC_API_KEY").is_ok() {
        "anthropic"
    } else {
        eprintln!("agent_step example: skipped — set OPENAI_API_KEY or ANTHROPIC_API_KEY to run.");
        return;
    };

    let model = if provider == "anthropic" {
        "claude-haiku-4-5-20251001"
    } else {
        "gpt-4o-mini"
    };

    let ctx = Context::local().expect("failed to build context");

    let docs = vec![
        "The Supplier shall deliver all goods within 30 days of purchase order receipt."
            .to_string(),
        "Late deliveries incur a penalty of 2% per week, capped at 10% of contract value."
            .to_string(),
        "Either party may terminate this agreement with 90 days written notice.".to_string(),
        "The Buyer warrants payment within 45 days of invoice date.".to_string(),
    ];

    let config = AgentStepPayload {
        model: model.to_string(),
        system_prompt: "Extract the key obligation (who must do what by when) as one sentence."
            .to_string(),
        max_rounds: 2,
        tool_refs: vec![],
        provider: provider.to_string(),
        output_schema: None,
        max_tokens_total: Some(20_000),
    };

    println!(
        "Running agent_step over {} documents ({provider}/{model})…",
        docs.len()
    );

    let rdd = ctx.parallelize_typed(docs, 2);
    let findings = rdd.agent_step(config).collect().expect("agent_step failed");

    for f in &findings {
        println!(
            "[input {}] rounds={} budget_exceeded={}\n  → {}\n",
            f.input_id, f.rounds, f.budget_exceeded, f.answer
        );
    }

    assert_eq!(findings.len(), 4, "expected one finding per input document");
    println!(
        "agent_step example completed successfully ({} findings).",
        findings.len()
    );
}
