use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use crate::exec_plan::AtomicScanExec;
use crate::errors::{AtomicSqlError, Result};
use crate::schema::project_schema;

/// A DataFusion [`TableProvider`] backed by in-memory Arrow [`RecordBatch`]es.
///
/// Data is organised as a list of partitions; each partition is a list of
/// `RecordBatch`es.  This maps naturally to atomic's RDD partition model —
/// callers can load an `TypedRdd<RecordBatch>`, call `collect()` to obtain
/// `Vec<RecordBatch>` (or `Vec<Vec<RecordBatch>>` for partitioned data), and
/// then register the result here.
#[derive(Debug, Clone)]
pub struct AtomicTableProvider {
    schema: SchemaRef,
    /// Outer: partitions.  Inner: batches within that partition.
    data: Vec<Vec<RecordBatch>>,
}

impl AtomicTableProvider {
    /// Create from a flat list of batches (treated as a single partition).
    pub fn from_batches(batches: Vec<RecordBatch>) -> Result<Self> {
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| AtomicSqlError::Schema("Cannot infer schema from empty batch list".into()))?;
        Ok(Self {
            schema,
            data: vec![batches],
        })
    }

    /// Create from pre-partitioned data.  Every inner `Vec` is one partition.
    /// All batches across all partitions must share the same schema.
    pub fn from_partitions(partitions: Vec<Vec<RecordBatch>>) -> Result<Self> {
        let schema = partitions
            .iter()
            .flat_map(|p| p.first())
            .map(|b| b.schema())
            .next()
            .ok_or_else(|| AtomicSqlError::Schema("Cannot infer schema from empty partitions".into()))?;
        Ok(Self {
            schema,
            data: partitions,
        })
    }

    /// Create from a known schema with no data (zero-row table).
    pub fn empty(schema: SchemaRef) -> Self {
        Self {
            schema,
            data: vec![],
        }
    }
}

#[async_trait]
impl TableProvider for AtomicTableProvider {
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

        // Apply column projection to every batch in every partition.
        let projected_data: Vec<Vec<RecordBatch>> = if let Some(indices) = projection {
            self.data
                .iter()
                .map(|partition| {
                    partition
                        .iter()
                        .map(|batch| batch.project(indices))
                        .collect::<datafusion::arrow::error::Result<Vec<_>>>()
                })
                .collect::<datafusion::arrow::error::Result<Vec<_>>>()?
        } else {
            self.data.clone()
        };

        Ok(Arc::new(AtomicScanExec::new(projected_data, projected_schema)))
    }
}
