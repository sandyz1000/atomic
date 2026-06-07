use std::any::Any;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float32Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
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
use dashmap::DashMap;
use futures::StreamExt;

use crate::vector::provider::VectorIndexProvider;

// ── Logical node ──────────────────────────────────────────────────────────────

/// ANN similarity search against a registered vector index.
/// Adds `__vs_id: UInt64` and `__vs_score: Float32` columns.
#[derive(Debug, Clone)]
pub struct VectorSearchNode {
    pub query_col: String,
    pub index_name: String,
    pub top_k: usize,
    pub input: LogicalPlan,
    output_schema: DFSchemaRef,
}

impl PartialEq for VectorSearchNode {
    fn eq(&self, other: &Self) -> bool {
        self.query_col == other.query_col
            && self.index_name == other.index_name
            && self.top_k == other.top_k
            && self.input == other.input
    }
}
impl Eq for VectorSearchNode {}

impl Hash for VectorSearchNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.query_col.hash(state);
        self.index_name.hash(state);
        self.top_k.hash(state);
    }
}

impl PartialOrd for VectorSearchNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (&self.query_col, &self.index_name, self.top_k)
            .partial_cmp(&(&other.query_col, &other.index_name, other.top_k))
    }
}

impl VectorSearchNode {
    pub fn new(query_col: String, index_name: String, top_k: usize, input: LogicalPlan) -> Self {
        let output_schema = build_output_df_schema(&input);
        Self { query_col, index_name, top_k, input, output_schema }
    }
}

fn build_output_df_schema(input: &LogicalPlan) -> DFSchemaRef {
    let mut fields: Vec<FieldRef> = input.schema().as_arrow().fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new("__vs_id", DataType::UInt64, false)));
    fields.push(Arc::new(Field::new("__vs_score", DataType::Float32, false)));
    let arrow_schema = Schema::new(fields);
    Arc::new(DFSchema::try_from(arrow_schema).expect("vector_search output schema"))
}

impl UserDefinedLogicalNodeCore for VectorSearchNode {
    fn name(&self) -> &str {
        "VectorSearch"
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
            "VectorSearch: index={}, query_col={}, top_k={}",
            self.index_name, self.query_col, self.top_k
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, mut inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        let new_input = inputs.swap_remove(0);
        let output_schema = build_output_df_schema(&new_input);
        Ok(Self {
            input: new_input,
            output_schema,
            query_col: self.query_col.clone(),
            index_name: self.index_name.clone(),
            top_k: self.top_k,
        })
    }
}

// ── Physical exec ─────────────────────────────────────────────────────────────

pub struct VectorSearchExec {
    query_col: String,
    index_name: String,
    top_k: usize,
    indexes: Arc<DashMap<String, Arc<dyn VectorIndexProvider>>>,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for VectorSearchExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorSearchExec")
            .field("index_name", &self.index_name)
            .field("top_k", &self.top_k)
            .finish_non_exhaustive()
    }
}

impl VectorSearchExec {
    pub fn new(
        node: &VectorSearchNode,
        input: Arc<dyn ExecutionPlan>,
        indexes: Arc<DashMap<String, Arc<dyn VectorIndexProvider>>>,
    ) -> Self {
        let mut fields: Vec<FieldRef> = input.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new("__vs_id", DataType::UInt64, false)));
        fields.push(Arc::new(Field::new("__vs_score", DataType::Float32, false)));
        let schema = Arc::new(Schema::new(fields));
        let n = input.output_partitioning().partition_count();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            query_col: node.query_col.clone(),
            index_name: node.index_name.clone(),
            top_k: node.top_k,
            indexes,
            input,
            schema,
            properties,
        }
    }
}

impl DisplayAs for VectorSearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VectorSearchExec: index={}, top_k={}", self.index_name, self.top_k)
    }
}

impl ExecutionPlan for VectorSearchExec {
    fn name(&self) -> &str {
        "VectorSearchExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
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
            query_col: self.query_col.clone(),
            index_name: self.index_name.clone(),
            top_k: self.top_k,
            indexes: self.indexes.clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let index = self.indexes.get(&self.index_name).map(|e| e.value().clone());
        let index = match index {
            Some(i) => i,
            None => {
                return Err(datafusion::error::DataFusionError::External(Box::new(
                    crate::errors::NlqError::VectorIndex(format!(
                        "vector index '{}' is not registered",
                        self.index_name
                    )),
                )));
            }
        };

        let mut stream = self.input.execute(partition, context)?;
        let query_col = self.query_col.clone();
        let top_k = self.top_k;
        let schema = self.schema.clone();
        let schema_for_adapter = schema.clone();

        let out = async_stream::stream! {
            while let Some(batch_res) = stream.next().await {
                let batch = match batch_res {
                    Ok(b) => b,
                    Err(e) => { yield Err(e); continue; }
                };

                if batch.num_rows() == 0 {
                    match build_vs_batch(&batch, vec![], vec![], schema.clone()) {
                        Ok(b) => { yield Ok(b); continue; }
                        Err(e) => { yield Err(e); return; }
                    }
                }

                let col_idx = match batch.schema().index_of(&query_col) {
                    Ok(i) => i,
                    Err(e) => {
                        yield Err(datafusion::error::DataFusionError::External(Box::new(
                            crate::errors::NlqError::Schema(format!(
                                "VectorSearch: query column '{query_col}' not found: {e}"
                            ))
                        )));
                        return;
                    }
                };

                let mut all_ids: Vec<u64> = Vec::new();
                let mut all_scores: Vec<f32> = Vec::new();
                let mut row_indices: Vec<usize> = Vec::new();

                let col = batch.column(col_idx);
                let list_col = col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::FixedSizeListArray>();
                let list_col = match list_col {
                    Some(c) => c,
                    None => {
                        yield Err(datafusion::error::DataFusionError::External(Box::new(
                            crate::errors::NlqError::Schema(format!(
                                "VectorSearch: column '{query_col}' must be FixedSizeList<Float32>"
                            ))
                        )));
                        return;
                    }
                };

                for row_idx in 0..batch.num_rows() {
                    let slice = list_col.value(row_idx);
                    let float_col = slice.as_any().downcast_ref::<Float32Array>().unwrap();
                    let query_vec: Vec<f32> = (0..float_col.len()).map(|i| float_col.value(i)).collect();

                    log::debug!("VectorSearch: searching index '{}' for row {}", index.name(), row_idx);
                    match index.search(&query_vec, top_k).await {
                        Ok(results) => {
                            for (id, score) in results {
                                all_ids.push(id);
                                all_scores.push(score);
                                row_indices.push(row_idx);
                            }
                        }
                        Err(e) => {
                            yield Err(datafusion::error::DataFusionError::External(Box::new(e)));
                            return;
                        }
                    }
                }

                match expand_batch(&batch, &row_indices, all_ids, all_scores, schema.clone()) {
                    Ok(b) => yield Ok(b),
                    Err(e) => { yield Err(e); return; }
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema_for_adapter, out)))
    }
}

fn build_vs_batch(
    input: &RecordBatch,
    ids: Vec<u64>,
    scores: Vec<f32>,
    schema: SchemaRef,
) -> DFResult<RecordBatch> {
    let n = ids.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(input.num_columns() + 2);
    for col in input.columns() {
        columns.push(col.slice(0, 0));
    }
    columns.push(Arc::new(UInt64Array::from(ids)) as ArrayRef);
    columns.push(Arc::new(Float32Array::from(scores)) as ArrayRef);
    RecordBatch::try_new(schema, columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Expand input rows: for each result (row_idx, id, score), replicate the input row.
fn expand_batch(
    input: &RecordBatch,
    row_indices: &[usize],
    ids: Vec<u64>,
    scores: Vec<f32>,
    schema: SchemaRef,
) -> DFResult<RecordBatch> {
    use datafusion::arrow::compute::take;
    use datafusion::arrow::array::UInt32Array;

    let take_indices = UInt32Array::from(row_indices.iter().map(|&i| i as u32).collect::<Vec<_>>());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(input.num_columns() + 2);
    for col in input.columns() {
        let taken = take(col.as_ref(), &take_indices, None)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
        columns.push(taken);
    }
    columns.push(Arc::new(UInt64Array::from(ids)) as ArrayRef);
    columns.push(Arc::new(Float32Array::from(scores)) as ArrayRef);

    RecordBatch::try_new(schema, columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}
