use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use futures::stream;

/// A leaf [`ExecutionPlan`] that serves pre-loaded Arrow [`RecordBatch`]es to
/// DataFusion's query engine.
///
/// Each element of `data` corresponds to one DataFusion partition.  Inside each
/// partition there may be zero or more `RecordBatch`es that are yielded in order.
#[derive(Debug)]
pub struct AtomicScanExec {
    /// One inner `Vec<RecordBatch>` per partition.
    data: Vec<Vec<RecordBatch>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl AtomicScanExec {
    pub fn new(data: Vec<Vec<RecordBatch>>, schema: SchemaRef) -> Self {
        let n_partitions = data.len().max(1);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(n_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            data,
            schema,
            properties,
        }
    }
}

impl DisplayAs for AtomicScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AtomicScanExec: partitions={}, schema={}",
            self.data.len(),
            self.schema
        )
    }
}

impl ExecutionPlan for AtomicScanExec {
    fn name(&self) -> &str {
        "AtomicScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "AtomicScanExec has no children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let batches = self
            .data
            .get(partition)
            .cloned()
            .unwrap_or_default();
        let schema = self.schema.clone();
        let stream = stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
