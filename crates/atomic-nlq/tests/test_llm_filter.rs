use std::collections::HashMap;
use std::sync::Arc;

use atomic_nlq::ir::json_plan::{JsonExpr, JsonPlanNode};
use atomic_nlq::ir::parser::IrParser;
use atomic_nlq::nodes::llm_filter::LlmFilterNode;
use atomic_nlq::optimizer::llm_batching_rule::LlmBatchingRule;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::optimizer::{OptimizerContext, OptimizerRule};

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]))
}

fn make_parser_with_batch(batch_size: usize) -> IrParser {
    let ctx = SessionContext::new();
    let mut schemas = HashMap::new();
    schemas.insert("orders".to_string(), orders_schema());
    IrParser::new(ctx.state_ref(), schemas, "claude-sonnet-4-6".to_string(), "voyage-3".to_string(), None, batch_size)
}

fn build_llm_filter_plan(batch_size: usize) -> LogicalPlan {
    let parser = make_parser_with_batch(batch_size);
    let node = JsonPlanNode::LlmFilter {
        prompt: "Is this a high-value order?".to_string(),
        model: None,
        input: Box::new(JsonPlanNode::TableScan {
            table: "orders".to_string(),
            projection: None,
        }),
    };
    parser.parse(&node).expect("should build LlmFilter plan")
}

#[test]
fn test_llm_filter_node_schema_matches_input() {
    let plan = build_llm_filter_plan(50);
    match &plan {
        LogicalPlan::Extension(Extension { node }) => {
            let filter_node = node.as_any().downcast_ref::<LlmFilterNode>().unwrap();
            // LlmFilter schema == input schema (filter preserves all columns)
            assert_eq!(filter_node.schema().fields().len(), 2);
            assert_eq!(filter_node.schema().field(0).name(), "customer_id");
            assert_eq!(filter_node.schema().field(1).name(), "amount");
        }
        other => panic!("expected Extension, got {other:?}"),
    }
}

#[test]
fn test_llm_filter_node_fields() {
    let plan = build_llm_filter_plan(25);
    match &plan {
        LogicalPlan::Extension(Extension { node }) => {
            let filter_node = node.as_any().downcast_ref::<LlmFilterNode>().unwrap();
            assert_eq!(filter_node.prompt, "Is this a high-value order?");
            assert_eq!(filter_node.model, "claude-sonnet-4-6");
            assert_eq!(filter_node.batch_size, 25);
            assert_eq!(node.name(), "LlmFilter");
        }
        other => panic!("expected Extension, got {other:?}"),
    }
}

#[test]
fn test_batching_rule_noop_when_already_correct() {
    let plan = build_llm_filter_plan(50);
    let rule = LlmBatchingRule::new(50);
    let config = OptimizerContext::new();
    let result = rule.rewrite(plan.clone(), &config).expect("rule should succeed");
    assert!(!result.transformed, "plan should not change when batch_size already matches");
}

#[test]
fn test_batching_rule_rewrites_when_batch_size_differs() {
    let plan = build_llm_filter_plan(10);
    let rule = LlmBatchingRule::new(50);
    let config = OptimizerContext::new();
    let result = rule.rewrite(plan, &config).expect("rule should succeed");
    assert!(result.transformed, "plan should be rewritten when batch_size differs");
    match &result.data {
        LogicalPlan::Extension(Extension { node }) => {
            let filter_node = node.as_any().downcast_ref::<LlmFilterNode>().unwrap();
            assert_eq!(filter_node.batch_size, 50, "batch_size should be updated to 50");
        }
        other => panic!("expected Extension, got {other:?}"),
    }
}

#[test]
fn test_batching_rule_on_non_extension_is_noop() {
    let parser = make_parser_with_batch(50);
    let plan = parser
        .parse(&JsonPlanNode::TableScan { table: "orders".to_string(), projection: None })
        .unwrap();
    let rule = LlmBatchingRule::new(50);
    let config = OptimizerContext::new();
    let result = rule.rewrite(plan, &config).expect("rule should succeed");
    assert!(!result.transformed);
}

#[test]
fn test_llm_filter_inputs_single_child() {
    let plan = build_llm_filter_plan(50);
    match &plan {
        LogicalPlan::Extension(Extension { node }) => {
            let filter_node = node.as_any().downcast_ref::<LlmFilterNode>().unwrap();
            assert_eq!(filter_node.inputs().len(), 1);
        }
        other => panic!("expected Extension, got {other:?}"),
    }
}

#[test]
fn test_llm_filter_with_explicit_model() {
    let ctx = SessionContext::new();
    let mut schemas = HashMap::new();
    schemas.insert("orders".to_string(), orders_schema());
    let parser = IrParser::new(
        ctx.state_ref(),
        schemas,
        "claude-sonnet-4-6".to_string(),
        "voyage-3".to_string(),
        None,
        50,
    );
    let node = JsonPlanNode::LlmFilter {
        prompt: "Is this luxury?".to_string(),
        model: Some("claude-haiku-4-5-20251001".to_string()),
        input: Box::new(JsonPlanNode::TableScan {
            table: "orders".to_string(),
            projection: None,
        }),
    };
    let plan = parser.parse(&node).unwrap();
    match &plan {
        LogicalPlan::Extension(Extension { node }) => {
            let filter_node = node.as_any().downcast_ref::<LlmFilterNode>().unwrap();
            assert_eq!(filter_node.model, "claude-haiku-4-5-20251001");
        }
        other => panic!("expected Extension, got {other:?}"),
    }
}
