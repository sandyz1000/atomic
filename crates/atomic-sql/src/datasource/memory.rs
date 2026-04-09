use datafusion::arrow::record_batch::RecordBatch;

use crate::errors::Result;
use crate::table::AtomicTableProvider;

/// Create an [`AtomicTableProvider`] from a flat list of [`RecordBatch`]es
/// (treated as a single partition).
pub fn from_batches(batches: Vec<RecordBatch>) -> Result<AtomicTableProvider> {
    AtomicTableProvider::from_batches(batches)
}

/// Create an [`AtomicTableProvider`] from pre-partitioned data.
pub fn from_partitions(partitions: Vec<Vec<RecordBatch>>) -> Result<AtomicTableProvider> {
    AtomicTableProvider::from_partitions(partitions)
}
