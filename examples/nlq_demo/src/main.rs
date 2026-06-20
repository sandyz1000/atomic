/// NLQ smoke test: register an in-memory table and run natural language queries.
///
/// # Prerequisites
///
/// ```bash
/// export OPENAI_API_KEY=sk-...
/// ```
///
/// # Run
///
/// ```bash
/// cargo run --example nlq_demo
/// ```
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

    let config = NlqConfig::default();
    let ctx = NlqContext::build(config);
    ctx.sql_ctx()
        .register_partitioned_batches("orders", vec![vec![batch]])?;

    for query in [
        "count all orders",
        "show the top 3 customers by total spend",
        "orders where amount is greater than 200",
    ] {
        println!("Query: {query}");
        match ctx.query(query).await {
            Ok(result) => println!("  Answer: {}\n", result.answer),
            Err(e) => eprintln!("  Error: {e}\n"),
        }
    }

    Ok(())
}
