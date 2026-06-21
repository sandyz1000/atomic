/// Distributed code audit using `TypedRdd::agent_step`.
///
/// Each partition holds a Rust code snippet. A subagent performs a two-round
/// review: first pass identifies potential issues; second pass cross-checks and
/// rates severity. The driver collects all findings and prints a consolidated
/// audit report ordered by severity.
///
/// This example demonstrates:
/// - Multi-round agent analysis (`max_rounds = 2`)
/// - Early termination via `FINAL ANSWER:` marker
/// - Structured severity classification on the driver side
/// - **Real tool calling**: the subagent may emit `TOOL_CALL: <name> <json_args>`
///   mid-round to invoke `lookup_cve_severity`, a Rust `#[task]` function dispatched
///   through `TASK_REGISTRY` — the same mechanism distributed `map_task`/`filter_task`
///   use, applied to tools.
///
/// This is the **Rust task** lane: tools are compiled `#[task]` functions in the
/// binary. The **scripted** lane (Python/JS tool source shipped dynamically, no
/// rebuild) is shown from a Python driver in `examples/py-demo/src/agent_step.py` —
/// embedding Python source in a Rust binary behind a Cargo feature would misrepresent
/// it as a compile-time concern, which it isn't.
///
/// # Prerequisites
///
/// ```bash
/// export OPENAI_API_KEY=sk-...     # OpenAI (default)
/// export ANTHROPIC_API_KEY=sk-...  # Anthropic Claude
/// ```
///
/// # Run
///
/// ```bash
/// cargo run --example agent_code_audit
/// ```
use atomic_compute::context::Context;
use atomic_compute::task_traits::UnaryTask;
use atomic_data::distributed::AgentStepPayload;

/// A real Rust tool: looks up the typical CVSS severity band for a known issue class.
/// Registered into `TASK_REGISTRY` at compile time by `#[task]`; dispatched by op_id
/// when the subagent emits `TOOL_CALL: <op_id> "<issue_class>"`.
#[atomic_compute::task]
fn lookup_cve_severity(issue_class: String) -> String {
    let key = issue_class.trim_matches('"').to_lowercase();
    let severity = match key.as_str() {
        s if s.contains("sql") || s.contains("injection") => "CRITICAL",
        s if s.contains("race") || s.contains("toctou") => "HIGH",
        s if s.contains("overflow") || s.contains("panic") || s.contains("unwrap") => "MEDIUM",
        _ => "UNKNOWN",
    };
    format!("{{\"issue_class\":{issue_class:?},\"typical_severity\":\"{severity}\"}}")
}

fn main() {
    atomic_nlq::agent_runner::register();

    let (provider, model) = detect_provider();

    // Each string is one code snippet to be audited.
    let snippets = vec![
        // Snippet A: unchecked indexing + silent integer overflow
        r#"
fn calculate_average(values: &[i32]) -> i32 {
    let mut sum = 0i32;
    for &v in values {
        sum += v;  // silent overflow if sum exceeds i32::MAX
    }
    sum / values.len() as i32  // panics on empty slice; truncating cast
}
"#
        .to_string(),
        // Snippet B: mutex poisoning + unwrap cascade
        r#"
static DATA: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

fn append(s: String) {
    DATA.lock().unwrap().push(s);  // poisons mutex on panic; unwrap propagates poison
}

fn read_first() -> String {
    DATA.lock().unwrap().first().unwrap().clone()  // panics if empty
}
"#
        .to_string(),
        // Snippet C: SQL injection surface via string interpolation
        r#"
fn build_query(table: &str, user_id: &str) -> String {
    // user_id comes directly from HTTP request parameters
    format!("SELECT * FROM {} WHERE user_id = '{}'", table, user_id)
}

fn fetch_user(conn: &Connection, user_id: &str) -> Vec<Row> {
    let q = build_query("users", user_id);
    conn.execute(&q).unwrap()  // also: unwrap on DB error
}
"#
        .to_string(),
        // Snippet D: time-of-check / time-of-use race on a temp file
        r#"
use std::path::Path;

fn process_upload(path: &Path, data: &[u8]) -> std::io::Result<()> {
    if !path.exists() {           // TOCTOU: file may be created between check and write
        std::fs::write(path, data)?;
    }
    // further processing assumes file now contains `data`
    let contents = std::fs::read(path)?;  // may read attacker-controlled content
    validate(&contents);
    Ok(())
}
"#
        .to_string(),
    ];

    if provider.is_empty() {
        eprintln!("agent_code_audit: skipped — set OPENAI_API_KEY or ANTHROPIC_API_KEY to run.");
        eprintln!(
            "\n{} code snippets would be audited in 2 partitions.",
            snippets.len()
        );
        return;
    }

    let tool_refs = vec![LookupCveSeverity::NAME.to_string()];
    let tool_hint = format!(
        "\n\nYou may call the tool `{}` with a JSON string naming the issue class \
         (e.g. TOOL_CALL: {} \"sql injection\") to check its typical severity band \
         before finalizing your rating.",
        LookupCveSeverity::NAME,
        LookupCveSeverity::NAME
    );

    let config = AgentStepPayload {
        model: model.to_string(),
        system_prompt: format!(
            r#"You are an expert Rust security auditor.

Round 1: list every potential bug, panic, race condition, or security issue you see,
one per line, prefixed with the severity label [CRITICAL], [HIGH], [MEDIUM], or [LOW].

Round 2: re-read your findings. If any are false positives or if you missed something
significant, correct them. Then output:

FINAL ANSWER:
[CRITICAL/HIGH/MEDIUM/LOW] <issue title> — <one-sentence explanation>
(one line per finding, most severe first){tool_hint}"#
        ),
        max_rounds: 4,
        tool_refs,
        resolved_tools: vec![],
        provider: provider.to_string(),
        output_schema: None,
        max_tokens_total: Some(40_000),
    };

    let ctx = Context::local().expect("failed to build context");

    // 2 partitions — snippets are split evenly.
    let rdd = ctx.parallelize_typed(snippets, 2);

    println!(
        "Auditing {} code snippets across 2 partitions ({provider}/{model})…\n",
        rdd.count().unwrap_or(0)
    );

    let findings = rdd.agent_step(config).collect().expect("agent_step failed");

    println!("=== Audit Report ===\n");

    let severity_rank = |line: &str| -> u8 {
        if line.contains("[CRITICAL]") {
            4
        } else if line.contains("[HIGH]") {
            3
        } else if line.contains("[MEDIUM]") {
            2
        } else if line.contains("[LOW]") {
            1
        } else {
            0
        }
    };

    let mut all_findings: Vec<(u8, String)> = Vec::new();

    for f in &findings {
        println!("--- Snippet {} ({} round(s)) ---", f.input_id + 1, f.rounds);
        if f.budget_exceeded {
            println!("  [budget exceeded — partial results]\n");
        }

        // Collect severity-tagged lines for the aggregate summary.
        for line in f.answer.lines() {
            let rank = severity_rank(line);
            if rank > 0 {
                all_findings.push((rank, line.trim().to_string()));
            }
        }

        println!("{}\n", f.answer.trim());
    }

    // Print aggregate severity summary.
    all_findings.sort_by_key(|(r, _)| std::cmp::Reverse(*r));

    let critical = all_findings.iter().filter(|(r, _)| *r == 4).count();
    let high = all_findings.iter().filter(|(r, _)| *r == 3).count();
    let medium = all_findings.iter().filter(|(r, _)| *r == 2).count();
    let low = all_findings.iter().filter(|(r, _)| *r == 1).count();

    println!("=== Summary ===");
    println!(
        "  CRITICAL: {critical}  HIGH: {high}  MEDIUM: {medium}  LOW: {low}  (total: {})",
        all_findings.len()
    );

    if critical > 0 || high > 0 {
        println!("\nTop issues:");
        for (_, line) in all_findings.iter().take(5) {
            println!("  {line}");
        }
    }
}

fn detect_provider() -> (String, String) {
    if std::env::var("OPENAI_API_KEY").is_ok() {
        ("openai".to_string(), "gpt-4o-mini".to_string())
    } else if std::env::var("ANTHROPIC_API_KEY").is_ok() {
        (
            "anthropic".to_string(),
            "claude-haiku-4-5-20251001".to_string(),
        )
    } else {
        (String::new(), String::new())
    }
}
