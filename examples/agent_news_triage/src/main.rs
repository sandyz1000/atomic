/// Distributed news triage using `TypedRdd::agent_step`.
///
/// Each partition holds a batch of news headlines. A subagent processes each
/// batch and returns structured JSON: urgency score (1–5), category, and a
/// one-line summary. The driver aggregates findings to surface the most urgent
/// stories and a category breakdown.
///
/// # Prerequisites
///
/// ```bash
/// export OPENAI_API_KEY=sk-...     # OpenAI (default)
/// export ANTHROPIC_API_KEY=sk-...  # Anthropic Claude
/// ```
///
/// If neither key is set, the example prints the raw headlines and exits.
///
/// # Run
///
/// ```bash
/// cargo run --example agent_news_triage
/// ```
use std::collections::HashMap;

use atomic_compute::context::Context;
use atomic_data::distributed::AgentStepPayload;

fn main() {
    atomic_nlq::agent_runner::register();

    let (provider, model) = detect_provider();

    let headlines = vec![
        // Batch 0 — infrastructure / security
        "Major cloud provider reports 6-hour outage affecting 40% of its global regions.".to_string(),
        "Critical zero-day in popular open-source TLS library; patch available but not yet deployed widely.".to_string(),
        "New ransomware strain targets hospital networks; three US facilities taken offline.".to_string(),
        // Batch 1 — markets / economy
        "Central bank raises interest rates by 50 bps; bond yields spike to 15-year high.".to_string(),
        "Semiconductor shortage eases as new fab capacity comes online in Q3.".to_string(),
        "Tech layoffs slow: industry adds 12,000 jobs in latest monthly report.".to_string(),
        // Batch 2 — climate / energy
        "Category 4 hurricane forecast to make landfall in 48 hours; mandatory evacuations ordered.".to_string(),
        "Solar panel efficiency record broken at 47.6%; commercialization timeline unclear.".to_string(),
    ];

    if provider.is_empty() {
        eprintln!("agent_news_triage: skipped — set OPENAI_API_KEY or ANTHROPIC_API_KEY to run.");
        eprintln!("\nHeadlines that would be triaged:");
        for (i, h) in headlines.iter().enumerate() {
            eprintln!("  [{i}] {h}");
        }
        return;
    }

    let config = AgentStepPayload {
        model: model.to_string(),
        system_prompt: r#"You are a news triage assistant. For each headline provided, return a
JSON object with these exact keys:
  "urgency": integer 1–5 (5 = breaking emergency, 1 = low-interest)
  "category": one of "security", "infrastructure", "economy", "climate", "energy", "other"
  "summary": one sentence explaining why this urgency score was assigned

Return a JSON array — one object per headline — and nothing else."#
            .to_string(),
        max_rounds: 1,
        tool_refs: vec![],
        provider: provider.to_string(),
        output_schema: Some(r#"{"type":"array","items":{"type":"object","required":["urgency","category","summary"]}}"#.to_string()),
        max_tokens_total: Some(30_000),
    };

    let ctx = Context::local().expect("failed to build context");

    // 3 partitions — each subagent gets ~3 headlines.
    let rdd = ctx.parallelize_typed(headlines, 3);

    println!(
        "Triaging {} headlines across 3 partitions ({provider}/{model})…\n",
        rdd.count().unwrap_or(0)
    );

    let findings = rdd.agent_step(config).collect().expect("agent_step failed");

    // Aggregate across partitions.
    let mut by_category: HashMap<String, Vec<(u64, &str)>> = HashMap::new();

    for finding in &findings {
        if finding.budget_exceeded {
            eprintln!("[partition {}] budget exceeded", finding.input_id);
            continue;
        }

        // Each agent returns a JSON array of scored headlines.
        let items: Vec<serde_json::Value> =
            serde_json::from_str(&finding.answer).unwrap_or_default();

        for item in &items {
            let urgency = item.get("urgency").and_then(|v| v.as_u64()).unwrap_or(0);
            let category = item
                .get("category")
                .and_then(|v| v.as_str())
                .unwrap_or("other")
                .to_string();
            let summary = item
                .get("summary")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            by_category
                .entry(category)
                .or_default()
                .push((urgency, Box::leak(summary.into_boxed_str())));
        }
    }

    // Print category breakdown sorted by max urgency.
    let mut categories: Vec<_> = by_category.iter().collect();
    categories.sort_by_key(|(_, items)| {
        std::cmp::Reverse(items.iter().map(|(u, _)| *u).max().unwrap_or(0))
    });

    println!("=== Triage results by category ===\n");
    for (category, items) in &categories {
        let max_urgency = items.iter().map(|(u, _)| *u).max().unwrap_or(0);
        let urgency_bar = "█".repeat(max_urgency as usize);
        println!("[{category}]  urgency peak: {urgency_bar} ({max_urgency}/5)");
        for (u, summary) in items.iter() {
            println!("  [{u}] {summary}");
        }
        println!();
    }

    // Identify the single most urgent headline.
    if let Some((category, items)) = categories.first()
        && let Some((u, summary)) = items.iter().max_by_key(|(u, _)| u)
    {
        println!(">>> Most urgent: [{u}/5] ({category})");
        println!("    {summary}");
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
