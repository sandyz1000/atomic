use std::collections::HashMap;

use atomic_nlq::workflow::{WorkflowPlan, WorkflowStep};

#[test]
fn test_workflow_plan_deserialize_single_step() {
    let json = r#"{
        "steps": [
            {
                "id": "count_orders",
                "tool": "sql_query",
                "args": {"query": "SELECT COUNT(*) FROM orders"},
                "depends_on": []
            }
        ]
    }"#;
    let plan: WorkflowPlan = serde_json::from_str(json).unwrap();
    assert_eq!(plan.steps.len(), 1);
    assert_eq!(plan.steps[0].id, "count_orders");
    assert_eq!(plan.steps[0].tool, "sql_query");
    assert!(plan.steps[0].depends_on.is_empty());
}

#[test]
fn test_workflow_plan_deserialize_parallel_steps() {
    let json = r#"{
        "steps": [
            {"id": "A", "tool": "sql_query", "args": {"query": "SELECT 1"}, "depends_on": []},
            {"id": "B", "tool": "sql_query", "args": {"query": "SELECT 2"}, "depends_on": []},
            {"id": "C", "tool": "sql_query", "args": {"query": "SELECT 3"}, "depends_on": ["A", "B"]}
        ]
    }"#;
    let plan: WorkflowPlan = serde_json::from_str(json).unwrap();
    assert_eq!(plan.steps.len(), 3);

    let step_c = plan.steps.iter().find(|s| s.id == "C").unwrap();
    assert_eq!(step_c.depends_on, vec!["A", "B"]);
}

#[test]
fn test_workflow_plan_serialize_roundtrip() {
    let plan = WorkflowPlan {
        steps: vec![
            WorkflowStep {
                id: "step1".to_string(),
                tool: "sql_query".to_string(),
                args: {
                    let mut m = HashMap::new();
                    m.insert("query".to_string(), serde_json::json!("SELECT 1"));
                    m
                },
                depends_on: vec![],
            },
        ],
    };
    let json = serde_json::to_string(&plan).unwrap();
    let parsed: WorkflowPlan = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.steps[0].id, "step1");
    assert_eq!(
        parsed.steps[0].args.get("query").unwrap(),
        &serde_json::json!("SELECT 1")
    );
}

#[test]
fn test_workflow_plan_empty_steps() {
    let json = r#"{"steps": []}"#;
    let plan: WorkflowPlan = serde_json::from_str(json).unwrap();
    assert!(plan.steps.is_empty());
}

#[test]
fn test_workflow_plan_invalid_json_fails() {
    let result = serde_json::from_str::<WorkflowPlan>("not json");
    assert!(result.is_err());
}
