/// Smoke test: register an in-memory table and run a few NL queries.
///
/// Run with:
///   ANTHROPIC_API_KEY=sk-... cargo run -p atomic-nlq --example nlq_demo
use std::sync::Arc;

use atomic_nlq::{NlqConfig, NlqContext};
use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── 1. Build a small in-memory orders table ───────────────────────────────
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("category", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 1, 2, 3, 1, 2, 3, 4])),
            Arc::new(Float64Array::from(vec![
                120.0, 45.0, 999.0, 30.0, 250.0, 80.0, 500.0, 15.0, 1200.0, 60.0,
            ])),
            Arc::new(StringArray::from(vec![
                Some("electronics"),
                Some("clothing"),
                Some("jewelry"),
                Some("books"),
                Some("electronics"),
                Some("clothing"),
                Some("watches"),
                Some("books"),
                Some("jewelry"),
                Some("electronics"),
            ])),
        ],
    )?;

    // ── 2. Build NlqContext and register the table ────────────────────────────
    let config = NlqConfig::default();
    let ctx = NlqContext::build(config);

    ctx.sql_ctx()
        .register_partitioned_batches("orders", vec![vec![batch]])?;

    // ── 3. Run a relational NL query ──────────────────────────────────────────
    println!("Query 1: count all orders");
    let df = ctx.query("count all orders").await?;
    df.show().await?;

    println!("\nQuery 2: top 3 customers by total spend");
    let df = ctx.query("show the top 3 customers by total spend").await?;
    df.show().await?;

    println!("\nQuery 3: orders with amount greater than 200");
    let df = ctx.query("orders where amount is greater than 200").await?;
    df.show().await?;

    Ok(())
}
