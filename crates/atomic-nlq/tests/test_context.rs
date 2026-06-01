use std::sync::Arc;

use atomic_nlq::{NlqConfig, NlqContext};
use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

fn make_config() -> NlqConfig {
    NlqConfig {
        anthropic_api_key: "test-dummy-key".to_string(),
        embed_api_key: "test-dummy-embed-key".to_string(),
        ..NlqConfig::default()
    }
}

fn orders_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("category", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 1, 3, 2])),
            Arc::new(Float64Array::from(vec![100.0, 200.0, 150.0, 50.0, 300.0])),
            Arc::new(StringArray::from(vec!["A", "B", "A", "C", "B"])),
        ],
    )
    .expect("batch creation failed")
}

#[tokio::test]
async fn test_nlq_context_builds_and_registers_table() {
    let ctx = NlqContext::build(make_config());
    let batch = orders_batch();

    ctx.sql_ctx()
        .register_batches("orders", vec![batch])
        .expect("register_batches failed");

    let df = ctx
        .sql_ctx()
        .sql("SELECT COUNT(*) AS n FROM orders")
        .await
        .expect("sql failed");
    let results = df.collect().await.expect("collect failed");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 1);
}

#[tokio::test]
async fn test_sql_filter_and_aggregation() {
    let ctx = NlqContext::build(make_config());
    ctx.sql_ctx()
        .register_batches("orders", vec![orders_batch()])
        .expect("register_batches failed");

    let df = ctx
        .sql_ctx()
        .sql("SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id ORDER BY customer_id")
        .await
        .expect("sql failed");
    let results = df.collect().await.expect("collect failed");

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "expected one row per distinct customer_id");
}

#[tokio::test]
async fn test_ir_parser_roundtrip_via_context() {
    use std::collections::HashMap;
    use atomic_nlq::ir::json_plan::{JsonExpr, JsonPlanNode};
    use atomic_nlq::ir::parser::IrParser;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::LogicalPlan;

    let ctx = SessionContext::new();
    let state_ref = ctx.state_ref();

    let schema: SchemaRef = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let mut schemas = HashMap::new();
    schemas.insert("orders".to_string(), schema);

    let parser = IrParser::new(
        state_ref,
        schemas,
        "claude-sonnet-4-6".to_string(),
        "voyage-3".to_string(),
        None,
        50,
    );

    // TableScan → Filter → Projection
    let plan_node = JsonPlanNode::Projection {
        exprs: vec![JsonExpr::Col { name: "customer_id".to_string(), table: None }],
        input: Box::new(JsonPlanNode::Filter {
            predicate: JsonExpr::BinOp {
                op: ">".to_string(),
                left: Box::new(JsonExpr::Col { name: "amount".to_string(), table: None }),
                right: Box::new(JsonExpr::LitNum {
                    value: serde_json::Number::from_f64(100.0).unwrap(),
                }),
            },
            input: Box::new(JsonPlanNode::TableScan {
                table: "orders".to_string(),
                projection: None,
            }),
        }),
    };

    let logical_plan = parser.parse(&plan_node).expect("IrParser::parse failed");
    assert!(
        matches!(logical_plan, LogicalPlan::Projection(_)),
        "top-level node should be Projection"
    );
}

/// Full end-to-end test with a real Anthropic API key.
///
/// Skipped automatically if ANTHROPIC_API_KEY is not set in the environment.
/// Run manually with:
///   ANTHROPIC_API_KEY=sk-... cargo test -p atomic-nlq test_full_nlq_pipeline
#[tokio::test]
async fn test_full_nlq_pipeline() {
    let api_key = match std::env::var("ANTHROPIC_API_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => {
            eprintln!("ANTHROPIC_API_KEY not set — skipping test_full_nlq_pipeline");
            return;
        }
    };

    let config = NlqConfig {
        anthropic_api_key: api_key,
        ..NlqConfig::default()
    };
    let ctx = NlqContext::build(config);

    ctx.sql_ctx()
        .register_batches("orders", vec![orders_batch()])
        .expect("register_batches failed");

    let df = ctx
        .query("count how many orders there are")
        .await
        .expect("NlqContext::query failed");
    let results = df.collect().await.expect("collect failed");
    assert!(!results.is_empty(), "expected at least one result batch");
}
