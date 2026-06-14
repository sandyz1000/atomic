use atomic_nlq::nodes::llm_filter::{LlmFilterNode, record_batch_to_json_rows};
use atomic_nlq::optimizer::llm_batching_rule::LlmBatchingRule;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use std::sync::Arc;

fn make_orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("item", DataType::Utf8, false),
    ]))
}

#[test]
fn test_filter_schema_passthrough() {
    let _session = SessionContext::new();
    let empty_plan = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(
            datafusion::common::DFSchema::try_from(make_orders_schema().as_ref().clone()).unwrap(),
        ),
    });
    let node = LlmFilterNode::new(
        "is_luxury_item".to_string(),
        "gpt-4o".to_string(),
        empty_plan.clone(),
        50,
    );
    assert_eq!(node.schema(), empty_plan.schema());
    assert_eq!(node.name(), "LlmFilter");
}

#[test]
fn test_batching_rule_size() {
    let rule = LlmBatchingRule::new(25);
    assert_eq!(rule.batch_size(), 25);
}

#[test]
fn test_batch_to_json() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();

    let rows = record_batch_to_json_rows(&batch);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["name"], serde_json::json!("alice"));
    assert_eq!(rows[0]["score"], serde_json::json!(10));
    assert_eq!(rows[1]["name"], serde_json::json!("bob"));
}

#[test]
fn test_record_batch_empty() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(Vec::<i64>::new()))]).unwrap();
    let rows = record_batch_to_json_rows(&batch);
    assert!(rows.is_empty());
}
