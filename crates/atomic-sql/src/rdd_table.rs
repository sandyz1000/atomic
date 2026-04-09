use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use atomic_compute::context::Context;
use atomic_data::rdd::Rdd;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::stream;

use crate::schema::project_schema;

// ─────────────────────────────────────────────────────────────────────────────
// RddTableProvider
// ─────────────────────────────────────────────────────────────────────────────

/// A DataFusion [`TableProvider`] backed by an atomic-compute `Rdd<RecordBatch>`.
///
/// When DataFusion executes a query against this table, each partition is
/// materialized in parallel via atomic-compute's scheduler (local threads or
/// remote workers). DataFusion then applies its own operators (filter, project,
/// aggregate, join) on top of the returned Arrow batches.
///
/// This is the bridge between DataFusion's SQL layer and atomic's RDD execution
/// model — analogous to how Spark SQL uses Spark Core RDDs for data access.
pub struct RddTableProvider {
    schema: SchemaRef,
    rdd: Arc<dyn Rdd<Item = RecordBatch>>,
    sc: Arc<Context>,
}

impl RddTableProvider {
    pub fn new(
        schema: SchemaRef,
        rdd: Arc<dyn Rdd<Item = RecordBatch>>,
        sc: Arc<Context>,
    ) -> Self {
        Self { schema, rdd, sc }
    }
}

impl fmt::Debug for RddTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RddTableProvider")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for RddTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection);
        let num_partitions = self.rdd.number_of_splits();
        Ok(Arc::new(RddScanExec::new(
            projected_schema,
            self.rdd.clone(),
            self.sc.clone(),
            projection.cloned(),
            num_partitions,
        )))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RddScanExec
// ─────────────────────────────────────────────────────────────────────────────

/// A leaf [`ExecutionPlan`] that materializes one RDD partition via atomic-compute.
///
/// `execute(partition_idx, …)` calls `Context::run_job_with_partitions` to fetch
/// that partition's `RecordBatch`es through atomic's scheduler, then streams the
/// result back to DataFusion.
pub struct RddScanExec {
    schema: SchemaRef,
    rdd: Arc<dyn Rdd<Item = RecordBatch>>,
    sc: Arc<Context>,
    /// Column indices to project after materialization (None = all columns).
    projection: Option<Vec<usize>>,
    num_partitions: usize,
    properties: Arc<PlanProperties>,
}

impl RddScanExec {
    pub fn new(
        schema: SchemaRef,
        rdd: Arc<dyn Rdd<Item = RecordBatch>>,
        sc: Arc<Context>,
        projection: Option<Vec<usize>>,
        num_partitions: usize,
    ) -> Self {
        let n = num_partitions.max(1);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self { schema, rdd, sc, projection, num_partitions, properties }
    }
}

impl fmt::Debug for RddScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RddScanExec")
            .field("schema", &self.schema)
            .field("num_partitions", &self.num_partitions)
            .field("projection", &self.projection)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for RddScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RddScanExec: partitions={}, schema={}",
            self.num_partitions, self.schema
        )
    }
}

impl ExecutionPlan for RddScanExec {
    fn name(&self) -> &str {
        "RddScanExec"
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
                "RddScanExec has no children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let rdd = self.rdd.clone();
        let sc = self.sc.clone();
        let projection = self.projection.clone();
        let schema = self.schema.clone();

        // Materialize this partition via atomic-compute's scheduler.
        // run_job_with_partitions returns Vec<Vec<RecordBatch>> (one inner Vec per
        // requested partition); we request exactly one partition so flatten it.
        let mut batches: Vec<RecordBatch> = sc
            .run_job_with_partitions(
                rdd,
                |iter| iter.collect::<Vec<RecordBatch>>(),
                [partition],
            )
            .map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "atomic-compute RDD execution failed: {}",
                    e
                ))
            })?
            .into_iter()
            .flatten()
            .collect();

        // Apply column projection if requested.
        if let Some(ref indices) = projection {
            batches = batches
                .into_iter()
                .map(|b| b.project(indices))
                .collect::<datafusion::arrow::error::Result<Vec<_>>>()?;
        }

        let stream = stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
