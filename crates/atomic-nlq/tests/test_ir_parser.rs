use std::collections::HashMap;
use std::sync::Arc;

use atomic_nlq::ir::json_plan::{JsonAgg, JsonExpr, JsonPlanNode, JsonSortExpr};
use atomic_nlq::ir::parser::IrParser;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("category", DataType::Utf8, true),
    ]))
}

fn customers_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn make_parser() -> IrParser {
    let ctx = SessionContext::new();
    let state_ref = ctx.state_ref();
    let mut schemas = HashMap::new();
    schemas.insert("orders".to_string(), orders_schema());
    schemas.insert("customers".to_string(), customers_schema());
    IrParser::new(state_ref, schemas, "claude-sonnet-4-6".to_string(), "voyage-3".to_string(), None, 50)
}

#[test]
fn test_table_scan_no_projection() {
    let parser = make_parser();
    let node = JsonPlanNode::TableScan { table: "orders".to_string(), projection: None };
    let plan = parser.parse(&node).expect("parse should succeed");
    assert!(
        matches!(plan, LogicalPlan::TableScan(_)),
        "expected TableScan, got {plan:?}"
    );
}

#[test]
fn test_table_scan_with_projection() {
    let parser = make_parser();
    let node = JsonPlanNode::TableScan {
        table: "orders".to_string(),
        projection: Some(vec!["customer_id".to_string(), "amount".to_string()]),
    };
    let plan = parser.parse(&node).expect("parse should succeed");
    match &plan {
        LogicalPlan::TableScan(ts) => {
            assert_eq!(ts.projected_schema.fields().len(), 2);
        }
        other => panic!("expected TableScan, got {other:?}"),
    }
}

#[test]
fn test_table_scan_unknown_table() {
    let parser = make_parser();
    let node = JsonPlanNode::TableScan { table: "nonexistent".to_string(), projection: None };
    assert!(parser.parse(&node).is_err());
}

#[test]
fn test_filter_plan() {
    let parser = make_parser();
    let node = JsonPlanNode::Filter {
        predicate: JsonExpr::BinOp {
            op: ">".to_string(),
            left: Box::new(JsonExpr::Col { name: "amount".to_string(), table: None }),
            right: Box::new(JsonExpr::LitNum { value: serde_json::Number::from(100) }),
        },
        input: Box::new(JsonPlanNode::TableScan {
            table: "orders".to_string(),
            projection: None,
        }),
    };
    let plan = parser.parse(&node).expect("parse should succeed");
    assert!(
        matches!(plan, LogicalPlan::Filter(_)),
        "expected Filter, got {plan:?}"
    );
}

#[test]
fn test_projection_plan() {
    let parser = make_parser();
    let node = JsonPlanNode::Projection {
        exprs: vec![
            JsonExpr::Col { name: "customer_id".to_string(), table: None },
            JsonExpr::Alias {
                expr: Box::new(JsonExpr::Col { name: "amount".to_string(), table: None }),
                name: "total".to_string(),
            },
        ],
        input: Box::new(JsonPlanNode::TableScan {
            table: "orders".to_string(),
            projection: None,
        }),
    };
    let plan = parser.parse(&node).expect("parse should succeed");
    assert!(
        matches!(plan, LogicalPlan::Projection(_)),
        "expected Projection, got {plan:?}"
    );
}

#[test]
fn test_aggregate_count_all() {
    let parser = make_parser();
    let node = JsonPlanNode::Aggregate {
        group_by: vec![JsonExpr::Col { name: "customer_id".to_string(), table: None }],
        aggs: vec![JsonAgg {
            func: "count".to_string(),
            expr: JsonExpr::Col { name: "amount".to_string(), table: None },
            alias: "order_count".to_string(),
        }],
        input: Box::new(JsonPlanNode::TableScan {
            table: "orders".to_string(),
            projection: None,
        }),
    };
    let plan = parser.parse(&node).expect("parse should succeed");
    assert!(
        matches!(plan, LogicalPlan::Aggregate(_)),
        "expected Aggregate, got {plan:?}"
    );
}

#[test]
fn test_aggregate_sum() {
    let parser = make_parser();
    let node = JsonPlanNode::Aggregate {
        group_by: vec![],
        aggs: vec![JsonAgg {
            func: "sum".to_string(),
            expr: JsonExpr::Col { name: "amount".to_string(), table: None },
            alias: "total_revenue".to_string(),
        }],
        input: Box::new(JsonPlanNode::TableScan {
            table: "orders".to_string(),
            projection: None,
        }),
    };
    let plan = parser.parse(&node).expect("parse should succeed");
    assert!(matches!(plan, LogicalPlan::Aggregate(_)));
}

#[test]
fn test_sort_plan() {
    let parser = make_parser();
    let node = JsonPlanNode::Sort {
        exprs: vec![JsonSortExpr {
            expr: JsonExpr::Col { name: "amount".to_string(), table: None },
            asc: false,
            nulls_first: false,
        }],
        input: Box::new(JsonPlanNode::TableScan {
            table: "orders".to_string(),
            projection: None,
        }),
    };
    let plan = parser.parse(&node).expect("parse should succeed");
    assert!(matches!(plan, LogicalPlan::Sort(_)), "expected Sort, got {plan:?}");
}

#[test]
fn test_limit_plan() {
    let parser = make_parser();
    let node = JsonPlanNode::Limit {
        skip: 0,
        fetch: Some(10),
        input: Box::new(JsonPlanNode::TableScan {
            table: "orders".to_string(),
            projection: None,
        }),
    };
    let plan = parser.parse(&node).expect("parse should succeed");
    assert!(matches!(plan, LogicalPlan::Limit(_)), "expected Limit, got {plan:?}");
}

#[test]
fn test_llm_filter_produces_extension() {
    let parser = make_parser();
    let node = JsonPlanNode::LlmFilter {
        prompt: "Is this a luxury item?".to_string(),
        model: None,
        input: Box::new(JsonPlanNode::TableScan {
            table: "orders".to_string(),
            projection: None,
        }),
    };
    let plan = parser.parse(&node).expect("parse should succeed");
    match &plan {
        LogicalPlan::Extension(Extension { node }) => {
            assert!(
                node.name() == "LlmFilter",
                "expected LlmFilter extension, got {}",
                node.name()
            );
        }
        other => panic!("expected Extension(LlmFilter), got {other:?}"),
    }
}

#[test]
fn test_llm_map_produces_extension() {
    let parser = make_parser();
    let node = JsonPlanNode::LlmMap {
        prompt: "Classify the category sentiment".to_string(),
        output_col: "sentiment".to_string(),
        output_type: "Utf8".to_string(),
        model: Some("claude-haiku-4-5-20251001".to_string()),
        input: Box::new(JsonPlanNode::TableScan {
            table: "orders".to_string(),
            projection: None,
        }),
    };
    let plan = parser.parse(&node).expect("parse should succeed");
    match &plan {
        LogicalPlan::Extension(Extension { node }) => {
            assert_eq!(node.name(), "LlmMap");
        }
        other => panic!("expected Extension(LlmMap), got {other:?}"),
    }
}

#[test]
fn test_embed_node_schema_has_embedding_col() {
    let parser = make_parser();
    let node = JsonPlanNode::Embed {
        input_col: "category".to_string(),
        output_col: "embedding".to_string(),
        model: None,
        input: Box::new(JsonPlanNode::TableScan {
            table: "orders".to_string(),
            projection: None,
        }),
    };
    let plan = parser.parse(&node).expect("Embed node should build successfully");
    match &plan {
        LogicalPlan::Extension(Extension { node }) => {
            let embed_node = node
                .as_any()
                .downcast_ref::<atomic_nlq::nodes::embed::EmbedNode>()
                .unwrap();
            // Schema should have input cols + the new embedding column
            let fields = embed_node.schema().fields();
            assert!(fields.iter().any(|f: &_| f.name() == "embedding"), "schema must contain 'embedding' column");
        }
        other => panic!("expected Extension(Embed), got {other:?}"),
    }
}

#[test]
fn test_expr_binop_operators() {
    let parser = make_parser();
    let schema = orders_schema();
    let df_schema = Arc::new(
        datafusion::common::DFSchema::try_from(schema.as_ref().clone())
            .expect("DFSchema from schema"),
    );
    for op in ["=", "!=", "<", "<=", ">", ">=", "And", "Or", "+", "-", "*", "/"] {
        let expr = JsonExpr::BinOp {
            op: op.to_string(),
            left: Box::new(JsonExpr::Col { name: "amount".to_string(), table: None }),
            right: Box::new(JsonExpr::LitNum { value: serde_json::Number::from(1) }),
        };
        assert!(parser.parse_expr(&expr, &df_schema).is_ok(), "operator '{op}' should parse");
    }
}

#[test]
fn test_expr_is_null_is_not_null() {
    let parser = make_parser();
    let schema = orders_schema();
    let df_schema = Arc::new(
        datafusion::common::DFSchema::try_from(schema.as_ref().clone()).unwrap(),
    );
    let is_null = JsonExpr::IsNull {
        expr: Box::new(JsonExpr::Col { name: "category".to_string(), table: None }),
    };
    let is_not_null = JsonExpr::IsNotNull {
        expr: Box::new(JsonExpr::Col { name: "category".to_string(), table: None }),
    };
    assert!(parser.parse_expr(&is_null, &df_schema).is_ok());
    assert!(parser.parse_expr(&is_not_null, &df_schema).is_ok());
}

#[test]
fn test_chained_filter_limit() {
    let parser = make_parser();
    let node = JsonPlanNode::Limit {
        skip: 0,
        fetch: Some(5),
        input: Box::new(JsonPlanNode::Sort {
            exprs: vec![JsonSortExpr {
                expr: JsonExpr::Col { name: "amount".to_string(), table: None },
                asc: false,
                nulls_first: false,
            }],
            input: Box::new(JsonPlanNode::Filter {
                predicate: JsonExpr::BinOp {
                    op: ">".to_string(),
                    left: Box::new(JsonExpr::Col { name: "amount".to_string(), table: None }),
                    right: Box::new(JsonExpr::LitNum { value: serde_json::Number::from(50) }),
                },
                input: Box::new(JsonPlanNode::TableScan {
                    table: "orders".to_string(),
                    projection: None,
                }),
            }),
        }),
    };
    let plan = parser.parse(&node).expect("chained plan should parse");
    assert!(matches!(plan, LogicalPlan::Limit(_)));
}
