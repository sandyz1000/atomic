use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

/// Build an Arrow [`SchemaRef`] from a list of `(name, data_type, nullable)` tuples.
pub fn schema_from_fields(fields: Vec<(&str, DataType, bool)>) -> SchemaRef {
    let arrow_fields: Vec<Field> = fields
        .into_iter()
        .map(|(name, dt, nullable)| Field::new(name, dt, nullable))
        .collect();
    Arc::new(Schema::new(arrow_fields))
}

/// Apply a column projection to a schema, returning a new schema with only the
/// selected field indices.
pub fn project_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    match projection {
        None => schema.clone(),
        Some(indices) => {
            let projected: Vec<Field> = indices
                .iter()
                .map(|&i| schema.field(i).clone())
                .collect();
            Arc::new(Schema::new(projected))
        }
    }
}
