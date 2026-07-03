use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use futures::StreamExt;

use crate::config::NlqConfig;
use crate::llm::LlmClient;
use crate::nodes::llm_filter::batch_to_json_rows;

/// LLM-based row transformer. Adds a new column to each row.
#[derive(Debug, Clone)]
pub struct LlmMapNode {
    pub prompt: String,
    pub model: String,
    pub output_col: String,
    pub output_type: DataType,
    pub batch_size: usize,
    pub input: LogicalPlan,
    output_schema: DFSchemaRef,
}

impl PartialEq for LlmMapNode {
    fn eq(&self, other: &Self) -> bool {
        self.prompt == other.prompt
            && self.model == other.model
            && self.output_col == other.output_col
            && self.output_type == other.output_type
            && self.batch_size == other.batch_size
            && self.input == other.input
    }
}
impl Eq for LlmMapNode {}

impl Hash for LlmMapNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.prompt.hash(state);
        self.model.hash(state);
        self.output_col.hash(state);
        self.batch_size.hash(state);
        self.input.hash(state);
    }
}

impl PartialOrd for LlmMapNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (&self.prompt, &self.model, &self.output_col).partial_cmp(&(
            &other.prompt,
            &other.model,
            &other.output_col,
        ))
    }
}

impl LlmMapNode {
    pub fn new(
        prompt: String,
        model: String,
        output_col: String,
        output_type: DataType,
        input: LogicalPlan,
        batch_size: usize,
    ) -> Self {
        let output_schema = build_output_df_schema(&input, &output_col, &output_type);
        Self {
            prompt,
            model,
            output_col,
            output_type,
            batch_size,
            input,
            output_schema,
        }
    }
}

fn build_output_df_schema(
    input: &LogicalPlan,
    output_col: &str,
    output_type: &DataType,
) -> DFSchemaRef {
    // Arrow schema: input fields + new column
    let input_arrow_schema: &Schema = input.schema().as_arrow();
    let mut fields: Vec<Arc<Field>> = input_arrow_schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(output_col, output_type.clone(), true)));
    let arrow_schema = Schema::new(fields);
    Arc::new(DFSchema::try_from(arrow_schema).expect("failed to build LlmMapNode schema"))
}

impl UserDefinedLogicalNodeCore for LlmMapNode {
    fn name(&self) -> &str {
        "LlmMap"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LlmMap: model={}, output_col={}, prompt={:?}",
            self.model, self.output_col, self.prompt
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        let new_input = inputs.swap_remove(0);
        let output_schema = build_output_df_schema(&new_input, &self.output_col, &self.output_type);
        Ok(Self {
            input: new_input,
            output_schema,
            prompt: self.prompt.clone(),
            model: self.model.clone(),
            output_col: self.output_col.clone(),
            output_type: self.output_type.clone(),
            batch_size: self.batch_size,
        })
    }
}

pub struct LlmMapExec {
    prompt: String,
    model: String,
    output_col: String,
    output_type: DataType,
    batch_size: usize,
    client: Arc<dyn LlmClient>,
    config: Arc<NlqConfig>,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for LlmMapExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LlmMapExec")
            .field("output_col", &self.output_col)
            .finish_non_exhaustive()
    }
}

impl LlmMapExec {
    pub fn new(
        node: &LlmMapNode,
        input: Arc<dyn ExecutionPlan>,
        client: Arc<dyn LlmClient>,
        config: Arc<NlqConfig>,
    ) -> Self {
        let mut fields: Vec<Arc<Field>> = input.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(
            &node.output_col,
            node.output_type.clone(),
            true,
        )));
        let schema = Arc::new(Schema::new(fields));
        let n = input.output_partitioning().partition_count();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            prompt: node.prompt.clone(),
            model: node.model.clone(),
            output_col: node.output_col.clone(),
            output_type: node.output_type.clone(),
            batch_size: config.llm_batch_size,
            client,
            config,
            input,
            schema,
            properties,
        }
    }
}

impl DisplayAs for LlmMapExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LlmMapExec: output_col={}", self.output_col)
    }
}

impl ExecutionPlan for LlmMapExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "LlmMapExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children.swap_remove(0),
            prompt: self.prompt.clone(),
            model: self.model.clone(),
            output_col: self.output_col.clone(),
            output_type: self.output_type.clone(),
            batch_size: self.batch_size,
            client: self.client.clone(),
            config: self.config.clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let mut stream = self.input.execute(partition, context)?;
        let client = self.client.clone();
        let prompt = self.prompt.clone();
        let model = self.model.clone();
        let batch_size = self.batch_size;
        let max_chunk_bytes = self.config.max_chunk_bytes;
        let output_col = self.output_col.clone();
        let output_type = self.output_type.clone();
        let schema = self.schema.clone();
        let schema_for_adapter = schema.clone();

        let out = async_stream::stream! {
            while let Some(batch_res) = stream.next().await {
                let batch = match batch_res {
                    Ok(b) => b,
                    Err(e) => { yield Err(e); continue; }
                };

                // P1b: skip empty batches without an API call
                if batch.num_rows() == 0 {
                    match build_output_batch(&batch, &output_col, &output_type, vec![], schema.clone()) {
                        Ok(b) => { yield Ok(b); continue; }
                        Err(e) => { yield Err(e); return; }
                    }
                }

                let rows = batch.num_rows();
                let mut values: Vec<serde_json::Value> = Vec::with_capacity(rows);
                let mut row_offset = 0;

                while row_offset < rows {
                    let end = (row_offset + batch_size).min(rows);
                    let chunk = batch.slice(row_offset, end - row_offset);
                    match llm_map_chunk(&client, &prompt, &model, &output_type, max_chunk_bytes, &chunk).await {
                        Ok(mut vals) => {
                            // P1c: validate response length
                            if vals.len() != chunk.num_rows() {
                                yield Err(datafusion::error::DataFusionError::External(Box::new(
                                    crate::errors::NlqError::PlanParse(format!(
                                        "LlmMap: LLM returned {} values for {} rows",
                                        vals.len(), chunk.num_rows()
                                    ))
                                )));
                                return;
                            }
                            values.append(&mut vals);
                        }
                        Err(e) => {
                            yield Err(datafusion::error::DataFusionError::External(Box::new(e)));
                            return;
                        }
                    }
                    row_offset = end;
                }

                match build_output_batch(&batch, &output_col, &output_type, values, schema.clone()) {
                    Ok(b) => yield Ok(b),
                    Err(e) => yield Err(e),
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_adapter,
            out,
        )))
    }
}

async fn llm_map_chunk(
    client: &Arc<dyn LlmClient>,
    prompt: &str,
    model: &str,
    output_type: &DataType,
    max_chunk_bytes: usize,
    batch: &RecordBatch,
) -> crate::errors::Result<Vec<serde_json::Value>> {
    let rows = batch_to_json_rows(batch);
    let type_hint = match output_type {
        DataType::Utf8 => "strings",
        DataType::Int64 | DataType::Int32 => "integers",
        DataType::Float64 | DataType::Float32 => "floats",
        DataType::Boolean => "booleans",
        _ => "values",
    };

    let rows_str = serde_json::to_string(&rows).unwrap_or_default();

    if rows_str.len() > max_chunk_bytes {
        return Err(crate::errors::NlqError::Internal(format!(
            "LlmMap chunk serialized to {} bytes (limit {}); reduce llm_batch_size",
            rows_str.len(),
            max_chunk_bytes
        )));
    }

    let system = format!(
        "You are a row transformer. For each input row, produce one {type_hint} value. Output ONLY a JSON array."
    );
    let user = format!(
        "Transform: {prompt}\n\nRows:\n{rows_str}\n\nReturn a JSON array of {type_hint}, one per row."
    );

    log::debug!("LlmMap: sending {} rows to LLM", batch.num_rows());
    let text = client.chat_with_retry(model, &system, &user, 2048).await?;

    let vals: Vec<serde_json::Value> = serde_json::from_str(&text).map_err(|e| {
        crate::errors::NlqError::PlanParse(format!("LlmMap response parse error: {e}; got: {text}"))
    })?;
    Ok(vals)
}

fn build_output_batch(
    input: &RecordBatch,
    _output_col: &str,
    output_type: &DataType,
    values: Vec<serde_json::Value>,
    schema: SchemaRef,
) -> DFResult<RecordBatch> {
    let new_col: ArrayRef = match output_type {
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(values.len(), 0);
            for v in &values {
                match v.as_str() {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(values.len());
            for v in &values {
                match v.as_i64() {
                    Some(i) => b.append_value(i),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(values.len());
            for v in &values {
                match v.as_f64() {
                    Some(f) => b.append_value(f),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in &values {
                match v.as_bool() {
                    Some(b_val) => b.append_value(b_val),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        _ => {
            let mut b = StringBuilder::with_capacity(values.len(), 0);
            for v in &values {
                b.append_value(v.to_string());
            }
            Arc::new(b.finish())
        }
    };

    let mut columns: Vec<ArrayRef> = input.columns().to_vec();
    columns.push(new_col);

    Ok(RecordBatch::try_new(schema, columns)?)
}
