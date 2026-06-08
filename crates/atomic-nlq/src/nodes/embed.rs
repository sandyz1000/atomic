use std::any::Any;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, FixedSizeListArray, Float32Array, StringArray};
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
use futures::StreamExt;

use crate::config::NlqConfig;
use crate::openai::OpenAiClient;

fn model_dim(model: &str, config_override: Option<usize>) -> i32 {
    if let Some(d) = config_override {
        return d as i32;
    }
    match model {
        m if m.starts_with("voyage-3-large") => 1024,
        m if m.starts_with("voyage-3") => 1024,
        m if m.starts_with("voyage-code") => 1536,
        m if m.starts_with("voyage-finance") => 1024,
        m if m.starts_with("text-embedding-3-large") => 3072,
        m if m.starts_with("text-embedding-3-small") => 1536,
        _ => 1024, // safe fallback
    }
}


/// Adds a `FixedSizeList<Float32>` embedding column to each row.
#[derive(Debug, Clone)]
pub struct EmbedNode {
    pub input_col: String,
    pub output_col: String,
    pub model: String,
    pub dim: i32,
    pub input: LogicalPlan,
    output_schema: DFSchemaRef,
}

impl PartialEq for EmbedNode {
    fn eq(&self, other: &Self) -> bool {
        self.input_col == other.input_col
            && self.output_col == other.output_col
            && self.model == other.model
            && self.dim == other.dim
            && self.input == other.input
    }
}
impl Eq for EmbedNode {}

impl Hash for EmbedNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input_col.hash(state);
        self.output_col.hash(state);
        self.model.hash(state);
        self.dim.hash(state);
    }
}

impl PartialOrd for EmbedNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (&self.input_col, &self.output_col, &self.model, self.dim)
            .partial_cmp(&(&other.input_col, &other.output_col, &other.model, other.dim))
    }
}

impl EmbedNode {
    pub fn new(
        input_col: String,
        output_col: String,
        model: String,
        input: LogicalPlan,
        embed_model_dim: Option<usize>,
    ) -> Self {
        let dim = model_dim(&model, embed_model_dim);
        let output_schema = build_output_df_schema(&input, &output_col, dim);
        Self { input_col, output_col, model, dim, input, output_schema }
    }
}

fn build_output_df_schema(input: &LogicalPlan, output_col: &str, dim: i32) -> DFSchemaRef {
    let mut fields: Vec<FieldRef> = input.schema().as_arrow().fields().iter().cloned().collect();
    let item_field = Arc::new(Field::new("item", DataType::Float32, true));
    fields.push(Arc::new(Field::new(
        output_col,
        DataType::FixedSizeList(item_field, dim),
        false,
    )));
    let arrow_schema = Schema::new(fields);
    Arc::new(DFSchema::try_from(arrow_schema).expect("embed output schema"))
}

impl UserDefinedLogicalNodeCore for EmbedNode {
    fn name(&self) -> &str {
        "Embed"
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
            "Embed: input_col={}, output_col={}, model={}, dim={}",
            self.input_col, self.output_col, self.model, self.dim
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, mut inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        let new_input = inputs.swap_remove(0);
        let output_schema = build_output_df_schema(&new_input, &self.output_col, self.dim);
        Ok(Self {
            input: new_input,
            output_schema,
            input_col: self.input_col.clone(),
            output_col: self.output_col.clone(),
            model: self.model.clone(),
            dim: self.dim,
        })
    }
}


pub struct EmbedExec {
    input_col: String,
    output_col: String,
    model: String,
    dim: i32,
    client: Arc<OpenAiClient>,
    config: Arc<NlqConfig>,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for EmbedExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EmbedExec")
            .field("output_col", &self.output_col)
            .field("model", &self.model)
            .finish_non_exhaustive()
    }
}

impl EmbedExec {
    pub fn new(
        node: &EmbedNode,
        input: Arc<dyn ExecutionPlan>,
        client: Arc<OpenAiClient>,
        config: Arc<NlqConfig>,
    ) -> Self {
        let mut fields: Vec<FieldRef> = input.schema().fields().iter().cloned().collect();
        let item_field = Arc::new(Field::new("item", DataType::Float32, true));
        fields.push(Arc::new(Field::new(
            &node.output_col,
            DataType::FixedSizeList(item_field, node.dim),
            false,
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
            input_col: node.input_col.clone(),
            output_col: node.output_col.clone(),
            model: node.model.clone(),
            dim: node.dim,
            client,
            config,
            input,
            schema,
            properties,
        }
    }
}

impl DisplayAs for EmbedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EmbedExec: output_col={}, model={}", self.output_col, self.model)
    }
}

impl ExecutionPlan for EmbedExec {
    fn name(&self) -> &str {
        "EmbedExec"
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
            input_col: self.input_col.clone(),
            output_col: self.output_col.clone(),
            model: self.model.clone(),
            dim: self.dim,
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
        let input_col = self.input_col.clone();
        let output_col = self.output_col.clone();
        let model = self.model.clone();
        let dim = self.dim;
        let batch_size = self.config.llm_batch_size;
        let schema = self.schema.clone();
        let schema_for_adapter = schema.clone();

        let out = async_stream::stream! {
            while let Some(batch_res) = stream.next().await {
                let batch = match batch_res {
                    Ok(b) => b,
                    Err(e) => { yield Err(e); continue; }
                };

                if batch.num_rows() == 0 {
                    match append_empty_embed_col(&batch, &output_col, dim, schema.clone()) {
                        Ok(b) => { yield Ok(b); continue; }
                        Err(e) => { yield Err(e); return; }
                    }
                }

                let col_idx = match batch.schema().index_of(&input_col) {
                    Ok(i) => i,
                    Err(e) => {
                        yield Err(datafusion::error::DataFusionError::External(Box::new(
                            crate::errors::NlqError::Schema(format!(
                                "Embed: input column '{input_col}' not found: {e}"
                            ))
                        )));
                        return;
                    }
                };

                let col = batch.column(col_idx);
                let str_col = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion::error::DataFusionError::External(Box::new(
                        crate::errors::NlqError::Schema(format!(
                            "Embed: column '{input_col}' must be Utf8"
                        ))
                    ))
                });
                let str_col = match str_col {
                    Ok(c) => c,
                    Err(e) => { yield Err(e); return; }
                };

                let texts: Vec<String> = (0..str_col.len())
                    .map(|i| if str_col.is_null(i) { String::new() } else { str_col.value(i).to_string() })
                    .collect();

                let mut all_embeddings: Vec<Vec<f32>> = Vec::with_capacity(texts.len());
                let mut offset = 0;
                while offset < texts.len() {
                    let end = (offset + batch_size).min(texts.len());
                    let chunk = &texts[offset..end];
                    log::debug!("EmbedExec: embedding {} texts", chunk.len());
                    match client.embed(&model, chunk.to_vec()).await {
                        Ok(mut vecs) => {
                            for v in &vecs {
                                if v.len() != dim as usize {
                                    yield Err(datafusion::error::DataFusionError::External(Box::new(
                                        crate::errors::NlqError::VectorIndex(format!(
                                            "Embed: model returned dim={} but expected dim={dim}",
                                            v.len()
                                        ))
                                    )));
                                    return;
                                }
                            }
                            all_embeddings.append(&mut vecs);
                        }
                        Err(e) => {
                            yield Err(datafusion::error::DataFusionError::External(Box::new(e)));
                            return;
                        }
                    }
                    offset = end;
                }

                match build_embed_batch(&batch, &output_col, dim, all_embeddings, schema.clone()) {
                    Ok(b) => yield Ok(b),
                    Err(e) => { yield Err(e); return; }
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema_for_adapter, out)))
    }
}

fn append_empty_embed_col(
    batch: &RecordBatch,
    output_col: &str,
    dim: i32,
    schema: SchemaRef,
) -> DFResult<RecordBatch> {
    build_embed_batch(batch, output_col, dim, vec![], schema)
}

fn build_embed_batch(
    input: &RecordBatch,
    output_col: &str,
    dim: i32,
    embeddings: Vec<Vec<f32>>,
    schema: SchemaRef,
) -> DFResult<RecordBatch> {
    let n = embeddings.len();
    let flat: Vec<f32> = embeddings.into_iter().flatten().collect();

    let values = Arc::new(Float32Array::from(flat));
    let item_field = Arc::new(Field::new("item", DataType::Float32, true));
    let embed_col: ArrayRef =
        Arc::new(FixedSizeListArray::try_new(item_field, dim, values, None)?);

    let mut columns: Vec<ArrayRef> = input.columns().to_vec();
    columns.push(embed_col);

    Ok(RecordBatch::try_new(schema, columns)?)
}
