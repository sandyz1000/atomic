use datafusion::arrow::datatypes::SchemaRef;
use serde_json::{Value, json};

/// Encode table schemas into a JSON description for the LLM system prompt.
pub fn encode_schema(tables: &[(String, SchemaRef)]) -> Value {
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
    Value::Array(table_descs)
}
