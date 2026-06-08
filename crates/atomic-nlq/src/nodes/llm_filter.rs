use std::any::Any;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchemaRef, Result as DFResult};
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
use serde_json::json;

use crate::config::NlqConfig;
use crate::openai::OpenAiClient;

/// LLM-based row filter. Schema is identical to the input's schema.
#[derive(Debug, Clone)]
pub struct LlmFilterNode {
    pub prompt: String,
    pub model: String,
    pub batch_size: usize,
    pub input: LogicalPlan,
}

impl PartialEq for LlmFilterNode {
    fn eq(&self, other: &Self) -> bool {
        self.prompt == other.prompt
            && self.model == other.model
            && self.batch_size == other.batch_size
            && self.input == other.input
    }
}
impl Eq for LlmFilterNode {}

impl Hash for LlmFilterNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.prompt.hash(state);
        self.model.hash(state);
        self.batch_size.hash(state);
        self.input.hash(state);
    }
}

impl PartialOrd for LlmFilterNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (&self.prompt, &self.model, self.batch_size)
            .partial_cmp(&(&other.prompt, &other.model, other.batch_size))
    }
}

impl LlmFilterNode {
    pub fn new(
        prompt: String,
        model: String,
        input: LogicalPlan,
        batch_size: usize,
    ) -> Self {
        Self { prompt, model, batch_size, input }
    }
}

impl UserDefinedLogicalNodeCore for LlmFilterNode {
    fn name(&self) -> &str {
        "LlmFilter"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LlmFilter: model={}, prompt={:?}", self.model, self.prompt)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        Ok(Self { input: inputs.swap_remove(0), ..self.clone() })
    }
}

pub struct LlmFilterExec {
    prompt: String,
    model: String,
    batch_size: usize,
    client: Arc<OpenAiClient>,
    config: Arc<NlqConfig>,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for LlmFilterExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LlmFilterExec")
            .field("model", &self.model)
            .field("batch_size", &self.batch_size)
            .finish_non_exhaustive()
    }
}

impl LlmFilterExec {
    pub fn new(
        node: &LlmFilterNode,
        input: Arc<dyn ExecutionPlan>,
        client: Arc<OpenAiClient>,
        config: Arc<NlqConfig>,
    ) -> Self {
        let schema = input.schema().clone();
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
            batch_size: config.llm_batch_size,
            client,
            config,
            input,
            schema,
            properties,
        }
    }
}

impl DisplayAs for LlmFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LlmFilterExec: model={}, batch_size={}", self.model, self.batch_size)
    }
}

impl ExecutionPlan for LlmFilterExec {
    fn name(&self) -> &str {
        "LlmFilterExec"
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
        let schema = self.schema.clone();

        let out = async_stream::stream! {
            while let Some(batch_res) = stream.next().await {
                let batch = match batch_res {
                    Ok(b) => b,
                    Err(e) => { yield Err(e); continue; }
                };

                if batch.num_rows() == 0 {
                    yield Ok(batch);
                    continue;
                }

                let rows = batch.num_rows();
                let mut keep = vec![false; rows];
                let mut row_offset = 0;

                while row_offset < rows {
                    let end = (row_offset + batch_size).min(rows);
                    let chunk = batch.slice(row_offset, end - row_offset);
                    match llm_filter_chunk(&client, &prompt, &model, max_chunk_bytes, &chunk).await {
                        Ok(mask) => {
                            if mask.len() != chunk.num_rows() {
                                yield Err(datafusion::error::DataFusionError::External(Box::new(
                                    crate::errors::NlqError::PlanParse(format!(
                                        "LlmFilter: LLM returned {} booleans for {} rows",
                                        mask.len(), chunk.num_rows()
                                    ))
                                )));
                                return;
                            }
                            for (i, v) in mask.iter().enumerate() {
                                keep[row_offset + i] = *v;
                            }
                        }
                        Err(e) => {
                            yield Err(datafusion::error::DataFusionError::External(Box::new(e)));
                            return;
                        }
                    }
                    row_offset = end;
                }

                let mask_array = BooleanArray::from(keep);
                match filter_record_batch(&batch, &mask_array) {
                    Ok(filtered) => yield Ok(filtered),
                    Err(e) => yield Err(datafusion::error::DataFusionError::ArrowError(
                        Box::new(e), None,
                    )),
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

async fn llm_filter_chunk(
    client: &Arc<OpenAiClient>,
    prompt: &str,
    model: &str,
    max_chunk_bytes: usize,
    batch: &RecordBatch,
) -> crate::errors::Result<Vec<bool>> {
    let rows = record_batch_to_json_rows(batch);
    let rows_str = serde_json::to_string(&rows).unwrap_or_default();

    if rows_str.len() > max_chunk_bytes {
        return Err(crate::errors::NlqError::Internal(format!(
            "LlmFilter chunk serialized to {} bytes (limit {}); reduce llm_batch_size",
            rows_str.len(),
            max_chunk_bytes
        )));
    }

    let system = "You are a row-level boolean classifier. For each row in the provided JSON array, \
        return a JSON array of booleans (true = keep row, false = discard). \
        Output ONLY the JSON array, no explanation.";
    let user = format!(
        "Predicate: {prompt}\n\nRows:\n{rows_str}\n\nReturn a JSON array of booleans, one per row."
    );

    log::debug!("LlmFilter: sending {} rows to LLM", batch.num_rows());
    let text = client.chat_with_retry(model, system, &user, 1024).await?;

    let bools: Vec<bool> = serde_json::from_str(&text).map_err(|e| {
        crate::errors::NlqError::PlanParse(format!(
            "LlmFilter response parse error: {e}; got: {text}"
        ))
    })?;

    Ok(bools)
}

pub fn record_batch_to_json_rows(batch: &RecordBatch) -> Vec<serde_json::Value> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let mut map = serde_json::Map::new();
        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let val = arrow_scalar_to_json(col.as_ref(), row_idx);
            map.insert(field.name().clone(), val);
        }
        rows.push(serde_json::Value::Object(map));
    }
    rows
}

fn arrow_scalar_to_json(
    arr: &dyn datafusion::arrow::array::Array,
    row: usize,
) -> serde_json::Value {
    use datafusion::arrow::array::*;
    if arr.is_null(row) {
        return serde_json::Value::Null;
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return json!(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return json!(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return json!(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return json!(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return json!(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return json!(a.value(row));
    }
    json!("<unsupported>")
}
