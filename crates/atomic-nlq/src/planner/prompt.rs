use std::collections::HashMap;

use datafusion::arrow::datatypes::SchemaRef;
use serde_json::{Value, json};

use crate::registry::ToolRegistry;
use crate::workflow::AgentContext;

/// Build the system prompt for the LLM workflow planner.
///
/// Instructs the model to output a `WorkflowPlan` JSON object — a dependency
/// graph of tool calls. No SQL, no prose, no IR.
pub fn build_plan_prompt(
    tables: &HashMap<String, SchemaRef>,
    tools: &ToolRegistry,
    ctx: &AgentContext,
) -> String {
    let schema_section = build_schema_section(tables);
    let tools_section = build_tools_section(tools);
    let history_section = build_history_section(ctx);

    format!(
        r#"You are a workflow planner for a distributed data processing system.
Given a natural language query, produce a JSON workflow plan that calls the available tools.

OUTPUT REQUIREMENTS:
- Output ONLY a single JSON object. No SQL, no markdown, no prose.
- The JSON must match: {{"steps": [<step>, ...]}}
- Each step: {{"id": "<unique_id>", "tool": "<tool_name>", "args": {{...}}, "depends_on": [<step_id>, ...]}}
- Steps with empty "depends_on" run in parallel. Steps that list upstream ids wait for those results.
- Use short, descriptive step ids like "get_orders", "filter_high_value", "compute_ltv".

{schema_section}

{tools_section}

{history_section}

EXAMPLES:

Query: "count all orders"
{{"steps":[{{"id":"count_orders","tool":"sql_query","args":{{"query":"SELECT COUNT(*) AS total FROM orders"}},"depends_on":[]}}]}}

Query: "get churned customers from last month and compute their average LTV"
{{"steps":[
  {{"id":"get_churned","tool":"sql_query","args":{{"query":"SELECT customer_id FROM events WHERE status='churned' AND month='last'"}},"depends_on":[]}},
  {{"id":"compute_ltv","tool":"sql_query","args":{{"query":"SELECT AVG(ltv) FROM customers WHERE customer_id IN (SELECT customer_id FROM churned)"}},"depends_on":["get_churned"]}}
]}}"#,
        schema_section = schema_section,
        tools_section = tools_section,
        history_section = history_section,
    )
}

fn build_schema_section(tables: &HashMap<String, SchemaRef>) -> String {
    if tables.is_empty() {
        return "TABLES: No tables registered.".to_string();
    }
    let table_descs: Vec<Value> = tables
        .iter()
        .map(|(name, schema)| {
            let columns: Vec<Value> = schema
                .fields()
                .iter()
                .map(|f| {
                    json!({
                        "name": f.name(),
                        "type": format!("{:?}", f.data_type()),
                        "nullable": f.is_nullable()
                    })
                })
                .collect();
            json!({ "table": name, "columns": columns })
        })
        .collect();
    let schema_str = serde_json::to_string_pretty(&Value::Array(table_descs)).unwrap_or_default();
    format!("TABLES:\n{schema_str}")
}

fn build_tools_section(tools: &ToolRegistry) -> String {
    let all = tools.all_tools();
    if all.is_empty() {
        return "TOOLS: No tools registered.".to_string();
    }
    let lines: Vec<String> = all
        .iter()
        .map(|t| {
            let schema_str =
                serde_json::to_string(&t.input_schema).unwrap_or_else(|_| "{}".to_string());
            format!(
                "- {}: {} | args schema: {}",
                t.name, t.description, schema_str
            )
        })
        .collect();
    format!("TOOLS:\n{}", lines.join("\n"))
}

fn build_history_section(ctx: &AgentContext) -> String {
    if ctx.previous_results.is_empty() {
        return String::new();
    }
    let lines: Vec<String> = ctx
        .previous_results
        .iter()
        .map(|r| {
            let kind = match &r.output {
                crate::workflow::StepOutput::DataFrame(b) => {
                    format!("DataFrame ({} buffers)", b.len())
                }
                crate::workflow::StepOutput::Text(t) => {
                    format!("Text: {}", &t[..t.len().min(150)])
                }
                crate::workflow::StepOutput::Empty => "Empty".to_string(),
            };
            format!("  step '{}': {kind}", r.step_id)
        })
        .collect();
    format!(
        "PREVIOUS ROUND RESULTS (round {}):\n{}",
        ctx.rounds,
        lines.join("\n")
    )
}
