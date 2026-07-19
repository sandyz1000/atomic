/// Multi-step agentic workflow DAG using `WorkflowExecutor`.
///
/// Builds a dependency graph of tool calls that executes in three waves:
///
///   Wave 1 (parallel):  [revenue_by_category]  [top_customers]
///   Wave 2 (parallel):  [interpret_revenue]    [interpret_customers]   (each depends on wave 1)
///   Wave 3:             [executive_summary]    (depends on both wave-2 steps)
///
/// The SQL steps in waves 1 and 2 run against an in-memory Arrow table.
/// The LLM steps in waves 2–3 require an API key; without one the example
/// executes waves 1–2 SQL-only and prints the raw query results.
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
/// cargo run --example agent_workflow_dag
/// ```
use std::collections::HashMap;
use std::sync::Arc;

use atomic_nlq::workflow::{WorkflowPlan, WorkflowStep};
use atomic_nlq::{NlqConfig, NlqContext};
use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (provider, model) = detect_provider();

    // ── Sample data: 20 orders across 4 categories, 6 customers ────────────────
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("region", DataType::Utf8, false),
    ]));

    #[rustfmt::skip]
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(vec![1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20])),
        Arc::new(Int64Array::from(vec![1,2,1,3,2,4,5,6,1,3, 4, 2, 5, 6, 1, 3, 4, 2, 5, 6])),
        Arc::new(StringArray::from(vec![
            "electronics","clothing","electronics","books","clothing",
            "electronics","books","clothing","electronics","books",
            "clothing","electronics","books","electronics","clothing",
            "books","electronics","clothing","books","electronics",
        ])),
        Arc::new(Float64Array::from(vec![
            320.0, 85.0, 450.0, 25.0, 190.0,
            870.0, 40.0, 130.0, 210.0, 60.0,
            95.0, 560.0, 35.0, 440.0, 175.0,
            15.0, 730.0, 220.0, 55.0, 390.0,
        ])),
        Arc::new(StringArray::from(vec![
            "west","east","west","east","east",
            "west","east","west","west","east",
            "west","east","west","east","west",
            "east","west","east","east","west",
        ])),
    ])?;

    // ── Build NlqContext ────────────────────────────────────────────────────────
    let api_key = match provider.as_str() {
        "openai" => std::env::var("OPENAI_API_KEY").unwrap_or_default(),
        "anthropic" => std::env::var("ANTHROPIC_API_KEY").unwrap_or_default(),
        _ => String::new(),
    };

    let config = NlqConfig {
        model: model.clone(),
        provider: if provider == "anthropic" {
            atomic_nlq::config::LlmProvider::Anthropic
        } else {
            atomic_nlq::config::LlmProvider::OpenAi
        },
        api_key: if api_key.is_empty() {
            "dummy-no-key".to_string()
        } else {
            api_key
        },
        ..NlqConfig::default()
    };

    let ctx = NlqContext::build(config)?;
    ctx.sql_ctx().register_batches("orders", vec![batch])?;

    // ── Construct the workflow DAG manually ─────────────────────────────────────
    //
    // Wave 1 (no deps — run in parallel):
    //   a: revenue_by_category
    //   b: top_customers
    //
    // Wave 2 (each depends on one wave-1 step — run in parallel):
    //   c: interpret_revenue    (depends_on: ["a"])
    //   d: interpret_customers  (depends_on: ["b"])
    //
    // Wave 3 (depends on both wave-2 steps):
    //   e: executive_summary   (depends_on: ["c", "d"])

    let sql_a = "SELECT category, ROUND(SUM(amount), 2) AS revenue, COUNT(*) AS orders \
                 FROM orders GROUP BY category ORDER BY revenue DESC";

    let sql_b = "SELECT customer_id, ROUND(SUM(amount), 2) AS total_spend, \
                 COUNT(*) AS order_count \
                 FROM orders GROUP BY customer_id ORDER BY total_spend DESC LIMIT 5";

    let plan = WorkflowPlan {
        steps: vec![
            // Wave 1 — parallel SQL queries
            WorkflowStep {
                id: "a".to_string(),
                tool: "sql_query".to_string(),
                args: HashMap::from([("query".to_string(), serde_json::json!(sql_a))]),
                depends_on: vec![],
            },
            WorkflowStep {
                id: "b".to_string(),
                tool: "sql_query".to_string(),
                args: HashMap::from([("query".to_string(), serde_json::json!(sql_b))]),
                depends_on: vec![],
            },
            // Wave 2 — LLM analysis (one per SQL result, both run in parallel)
            WorkflowStep {
                id: "c".to_string(),
                tool: "sql_query".to_string(),
                args: HashMap::from([(
                    "query".to_string(),
                    serde_json::json!(
                        "SELECT category, revenue, orders, \
                         ROUND(revenue / SUM(revenue) OVER (), 4) AS revenue_share \
                         FROM (SELECT category, ROUND(SUM(amount),2) AS revenue, COUNT(*) AS orders \
                               FROM orders GROUP BY category) \
                         ORDER BY revenue_share DESC"
                    ),
                )]),
                depends_on: vec!["a".to_string()],
            },
            WorkflowStep {
                id: "d".to_string(),
                tool: "sql_query".to_string(),
                args: HashMap::from([(
                    "query".to_string(),
                    serde_json::json!(
                        "SELECT region, ROUND(SUM(amount),2) AS regional_revenue, \
                         COUNT(DISTINCT customer_id) AS unique_customers \
                         FROM orders GROUP BY region ORDER BY regional_revenue DESC"
                    ),
                )]),
                depends_on: vec!["b".to_string()],
            },
            // Wave 3 — synthesis (waits for both c and d)
            WorkflowStep {
                id: "e".to_string(),
                tool: "sql_query".to_string(),
                args: HashMap::from([(
                    "query".to_string(),
                    serde_json::json!(
                        "SELECT \
                         (SELECT category FROM (SELECT category, SUM(amount) AS r \
                          FROM orders GROUP BY category ORDER BY r DESC LIMIT 1)) AS top_category, \
                         (SELECT region FROM (SELECT region, SUM(amount) AS r \
                          FROM orders GROUP BY region ORDER BY r DESC LIMIT 1)) AS top_region, \
                         (SELECT customer_id FROM (SELECT customer_id, SUM(amount) AS r \
                          FROM orders GROUP BY customer_id ORDER BY r DESC LIMIT 1)) AS top_customer, \
                         ROUND(SUM(amount), 2) AS total_revenue, \
                         COUNT(*) AS total_orders \
                         FROM orders"
                    ),
                )]),
                depends_on: vec!["c".to_string(), "d".to_string()],
            },
        ],
    };

    // Print the plan structure so the DAG is visible.
    println!("=== Workflow DAG ===\n");
    println!("  Wave 1 (parallel):  [a: revenue_by_category]  [b: top_customers]");
    println!("  Wave 2 (parallel):  [c: revenue_share]         [d: regional_breakdown]");
    println!("                         ↑ depends on a               ↑ depends on b");
    println!("  Wave 3:             [e: executive_summary]");
    println!("                         ↑ depends on c AND d\n");

    println!("Executing workflow ({} steps)…\n", plan.steps.len());

    // Execute the plan. The executor respects the dependency waves —
    // [a, b] start immediately; [c, d] start after their respective deps;
    // [e] starts only after both c and d complete.
    let results = ctx.execute_plan(plan).await?;

    // ── Print results in wave order ─────────────────────────────────────────────
    print_step(&results, "a", "Revenue by category");
    print_step(&results, "b", "Top 5 customers by spend");
    print_step(&results, "c", "Revenue share per category");
    print_step(&results, "d", "Regional breakdown");
    print_step(&results, "e", "Executive summary");

    // If an API key is available, also run the natural-language query path.
    if !provider.is_empty() {
        println!("\n=== NLQ query (same data, LLM plans the DAG) ===\n");
        println!("Question: which category and region should we focus on to grow revenue?\n");
        match ctx
            .query("which category and region should we focus on to grow revenue?")
            .await
        {
            Ok(result) => {
                println!("Answer: {}", result.answer);
                println!("  ({} rounds, {} steps)", result.rounds, result.steps.len());
            }
            Err(e) => eprintln!("NLQ error: {e}"),
        }
    }

    Ok(())
}

fn print_step(
    results: &std::collections::HashMap<String, atomic_nlq::StepResult>,
    id: &str,
    label: &str,
) {
    use atomic_nlq::StepOutput;

    let Some(result) = results.get(id) else {
        println!("[{id}] {label}: (not executed)\n");
        return;
    };

    println!("--- [{id}] {label} ---");
    match &result.output {
        StepOutput::DataFrame(ipc_bufs) => {
            // Decode IPC and print a text table.
            use datafusion::arrow::ipc::reader::FileReader;
            for buf in ipc_bufs {
                if let Ok(reader) = FileReader::try_new(std::io::Cursor::new(buf.as_slice()), None)
                {
                    for batch in reader.flatten() {
                        let schema = batch.schema();
                        // Header
                        let headers: Vec<_> =
                            schema.fields().iter().map(|f| f.name().as_str()).collect();
                        println!("  {}", headers.join("  |  "));
                        println!("  {}", "-".repeat(headers.join("  |  ").len()));
                        // Rows
                        for row_idx in 0..batch.num_rows() {
                            let cols: Vec<String> = (0..batch.num_columns())
                                .map(|c| {
                                    let col = batch.column(c);
                                    datafusion::arrow::util::display::array_value_to_string(
                                        col, row_idx,
                                    )
                                    .unwrap_or_else(|_| "?".to_string())
                                })
                                .collect();
                            println!("  {}", cols.join("    "));
                        }
                    }
                }
            }
        }
        StepOutput::Text(t) => println!("  {t}"),
        StepOutput::Empty => println!("  (empty)"),
    }
    println!();
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
