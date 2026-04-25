use serde_json::Value;

use crate::registry::UdfDescription;

/// Build the system prompt for the LLM planner.
///
/// The prompt instructs the model to output a single JSON object representing
/// the query plan, matching the `JsonPlanNode` schema. No SQL, no prose.
pub fn build_system_prompt(schema_json: &Value, udfs: &[UdfDescription]) -> String {
    let schema_str = serde_json::to_string_pretty(schema_json).unwrap_or_default();

    let udf_section = if udfs.is_empty() {
        "No custom functions are available.".to_string()
    } else {
        let lines: Vec<String> = udfs
            .iter()
            .map(|u| format!("- {}({}): {}", u.name, u.signature, u.description))
            .collect();
        format!("Available functions:\n{}", lines.join("\n"))
    };

    format!(
        r#"You are a query planner. Given a natural language query, produce a JSON execution plan.

OUTPUT REQUIREMENTS:
- Output ONLY a single JSON object. No SQL, no markdown, no prose.
- The JSON must match one of the node types below.
- Use double-quoted strings. No trailing commas.

DATABASE SCHEMA:
{schema_str}

{udf_section}

PLAN NODE TYPES:

TableScan: {{"type":"TableScan","table":"<name>","projection":["col1","col2"] or null}}
Filter: {{"type":"Filter","predicate":<expr>,"input":<node>}}
Projection: {{"type":"Projection","exprs":[<expr>...],"input":<node>}}
Aggregate: {{"type":"Aggregate","group_by":[<expr>...],"aggs":[{{"func":"sum|count|avg|min|max","expr":<expr>,"alias":"<name>"}}...],"input":<node>}}
Sort: {{"type":"Sort","exprs":[{{"expr":<expr>,"asc":true|false,"nulls_first":true|false}}...],"input":<node>}}
Limit: {{"type":"Limit","skip":0,"fetch":<n or null>,"input":<node>}}
Join: {{"type":"Join","join_type":"Inner|Left|Right|Full","left":<node>,"right":<node>,"on":[[<left_expr>,<right_expr>]...]}}
LlmFilter: {{"type":"LlmFilter","prompt":"<predicate description>","model":null,"input":<node>}}
LlmMap: {{"type":"LlmMap","prompt":"<transform description>","output_col":"<name>","output_type":"Utf8|Float64|Int64|Boolean","model":null,"input":<node>}}
Embed: {{"type":"Embed","input_col":"<text_column>","output_col":"<embedding_column>","model":null,"input":<node>}}
VectorSearch: {{"type":"VectorSearch","query_col":"<embedding_column>","index_name":"<registered_index>","top_k":<k>,"input":<node>}}

EXPRESSION TYPES:
Column: {{"type":"Col","name":"<column_name>","table":null}}
Literal string: {{"type":"Lit","value":"<string>"}}
Literal number: {{"type":"LitNum","value":<number>}}
Literal bool: {{"type":"LitBool","value":true|false}}
Binary op: {{"type":"BinOp","op":"Eq|NotEq|Lt|LtEq|Gt|GtEq|And|Or|Plus|Minus|Multiply|Divide","left":<expr>,"right":<expr>}}
IsNull: {{"type":"IsNull","expr":<expr>}}
IsNotNull: {{"type":"IsNotNull","expr":<expr>}}
Not: {{"type":"Not","expr":<expr>}}
Call (UDF): {{"type":"Call","func":"<function_name>","args":[<expr>...]}}
Alias: {{"type":"Alias","expr":<expr>,"name":"<alias>"}}

EXAMPLES:

Query: "count all orders"
{{"type":"Aggregate","group_by":[],"aggs":[{{"func":"count","expr":{{"type":"LitNum","value":1}},"alias":"count"}}],"input":{{"type":"TableScan","table":"orders","projection":null}}}}

Query: "show the top 5 customers by total amount"
{{"type":"Limit","skip":0,"fetch":5,"input":{{"type":"Sort","exprs":[{{"expr":{{"type":"Col","name":"total_amount","table":null}},"asc":false,"nulls_first":false}}],"input":{{"type":"Aggregate","group_by":[{{"type":"Col","name":"customer_id","table":null}}],"aggs":[{{"func":"sum","expr":{{"type":"Col","name":"amount","table":null}},"alias":"total_amount"}}],"input":{{"type":"TableScan","table":"orders","projection":null}}}}}}}}
"#,
        schema_str = schema_str,
        udf_section = udf_section,
    )
}
