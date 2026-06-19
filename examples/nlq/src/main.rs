/// Natural language query example.
///
/// Demonstrates querying an in-memory Arrow table using plain English via
/// the `atomic-nlq` crate. The LLM translates the query into a workflow plan
/// executed on the Atomic compute engine.
///
/// # Prerequisites
///
/// Set the OpenAI API key in the environment:
///
/// ```bash
/// export OPENAI_API_KEY=sk-...
/// ```
///
/// If `OPENAI_API_KEY` is not set, the example falls back to a direct SQL
/// query to demonstrate the DataFusion integration without an API call.
use std::sync::Arc;

use atomic_nlq::{NlqConfig, NlqContext};
use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("category", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 1, 3, 2, 4, 3])),
            Arc::new(Float64Array::from(vec![
                120.0, 450.0, 300.0, 80.0, 200.0, 560.0, 150.0,
            ])),
            Arc::new(StringArray::from(vec![
                "electronics",
                "clothing",
                "electronics",
                "books",
                "clothing",
                "electronics",
                "electronics",
            ])),
        ],
    )?;

    let api_key = std::env::var("OPENAI_API_KEY").unwrap_or_default();
    let has_key = !api_key.is_empty();

    let config = NlqConfig {
        api_key: if has_key {
            api_key
        } else {
            "dummy-key-no-api-call".to_string()
        },
        ..NlqConfig::default()
    };

    let ctx = NlqContext::build(config);
    ctx.sql_ctx().register_batches("orders", vec![batch])?;

    if has_key {
        println!("Running natural language queries via OpenAI...\n");

        let queries = [
            "how many orders are there in total",
            "what is the total revenue per category",
            "show the top 3 customers by total spend",
        ];

        for nl in queries {
            println!("Query: {nl}");
            match ctx.query(nl).await {
                Ok(result) => {
                    println!("Answer: {}", result.answer);
                    println!(
                        "  ({} rounds, {} steps executed)",
                        result.rounds,
                        result.steps.len()
                    );
                }
                Err(e) => {
                    eprintln!("  Error: {e}");
                }
            }
            println!();
        }
    } else {
        println!(
            "OPENAI_API_KEY not set — running direct SQL queries to demonstrate DataFusion integration.\n"
        );

        let queries = [
            (
                "Total order count",
                "SELECT COUNT(*) AS total_orders FROM orders",
            ),
            (
                "Revenue by category",
                "SELECT category, SUM(amount) AS revenue FROM orders GROUP BY category ORDER BY revenue DESC",
            ),
            (
                "Top 3 customers by spend",
                "SELECT customer_id, SUM(amount) AS total_spend FROM orders GROUP BY customer_id ORDER BY total_spend DESC LIMIT 3",
            ),
        ];

        for (label, sql) in queries {
            println!("{label}:");
            let df = ctx.sql_ctx().sql(sql).await?;
            df.show().await?;
            println!();
        }
    }

    Ok(())
}
