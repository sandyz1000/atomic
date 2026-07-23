//! Streaming engine that drives the [`MapGroupsWithState`](crate::map_groups_state) core over
//! Arrow micro-batches.
//!
//! Rows are represented as `Vec<GroupVal>` (one scalar per column) so the user's update function
//! works in plain Rust without an Arrow row API. Each batch is grouped by the key columns, the
//! core is invoked per group, processing-time timeouts fire between batches, and the output rows
//! are assembled back into a `RecordBatch` matching the caller-provided output schema.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use parking_lot::Mutex;

use crate::errors::{StructuredError, StructuredResult};
use crate::map_groups_state::{GroupState, GroupStateTimeout, MapGroupsWithState};
use crate::query::BatchEngine;
use crate::source::StreamSource;
use crate::state::GroupVal;

/// A row as scalar column values — the `R`/`Out` type the update function sees and returns.
pub type Row = Vec<GroupVal>;

/// The update-function shape for the streaming DSL: `(key, rows, state) -> output rows`.
pub type UpdateFn<S> = dyn Fn(&[GroupVal], Vec<Row>, &mut GroupState<S>) -> Vec<Row> + Send + Sync;

/// Drives the arbitrary-per-group state core over Arrow batches from `source`.
pub(crate) struct MapGroupsStateEngine<S: Send + 'static> {
    source: Arc<dyn StreamSource>,
    key_cols: Vec<String>,
    output_schema: SchemaRef,
    op: Mutex<MapGroupsWithState<S, Row, Row, Box<UpdateFn<S>>>>,
}

impl<S: Send + 'static> MapGroupsStateEngine<S> {
    pub(crate) fn new(
        source: Arc<dyn StreamSource>,
        key_cols: Vec<String>,
        output_schema: SchemaRef,
        timeout: GroupStateTimeout,
        update: Box<UpdateFn<S>>,
    ) -> Self {
        MapGroupsStateEngine {
            source,
            key_cols,
            output_schema,
            op: Mutex::new(MapGroupsWithState::new(update, timeout)),
        }
    }

    /// The `GroupVal` at `(row, col)` of a batch, casting via the column's type.
    fn cell(col: &ArrayRef, row: usize) -> GroupVal {
        if !col.is_valid(row) {
            return GroupVal::Null;
        }
        if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
            return GroupVal::Str(a.value(row).to_string());
        }
        if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            return GroupVal::Int(a.value(row));
        }
        GroupVal::Null
    }

    /// Build `(key, full_row)` scalars for every row in `batch`, keyed by the key columns.
    fn group_batch(
        &self,
        batch: &RecordBatch,
    ) -> StructuredResult<HashMap<Vec<GroupVal>, Vec<Row>>> {
        let key_idx: Vec<usize> = self
            .key_cols
            .iter()
            .map(|c| {
                batch
                    .schema()
                    .index_of(c)
                    .map_err(|_| StructuredError::Sql(format!("map_groups: no column '{c}'")))
            })
            .collect::<StructuredResult<_>>()?;
        let cols = batch.columns();
        let mut grouped: HashMap<Vec<GroupVal>, Vec<Row>> = HashMap::new();
        for r in 0..batch.num_rows() {
            let row: Row = cols.iter().map(|c| Self::cell(c, r)).collect();
            let key: Vec<GroupVal> = key_idx.iter().map(|&i| row[i].clone()).collect();
            grouped.entry(key).or_default().push(row);
        }
        Ok(grouped)
    }

    /// Assemble output rows into a `RecordBatch` matching `output_schema`. Every row must have one
    /// scalar per output field.
    fn rows_to_batch(&self, rows: Vec<Row>) -> StructuredResult<RecordBatch> {
        let fields = self.output_schema.fields();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());
        for (ci, field) in fields.iter().enumerate() {
            match field.data_type() {
                DataType::Int64 => {
                    let vals: Vec<Option<i64>> = rows
                        .iter()
                        .map(|r| match r.get(ci) {
                            Some(GroupVal::Int(v)) => Some(*v),
                            _ => None,
                        })
                        .collect();
                    columns.push(Arc::new(Int64Array::from(vals)));
                }
                DataType::Utf8 => {
                    let vals: Vec<Option<String>> = rows
                        .iter()
                        .map(|r| match r.get(ci) {
                            Some(GroupVal::Str(s)) => Some(s.clone()),
                            Some(GroupVal::Int(v)) => Some(v.to_string()),
                            _ => None,
                        })
                        .collect();
                    columns.push(Arc::new(StringArray::from(vals)));
                }
                other => {
                    return Err(StructuredError::Unsupported(format!(
                        "map_groups output column '{}' has unsupported type {other:?}",
                        field.name()
                    )));
                }
            }
        }
        RecordBatch::try_new(self.output_schema.clone(), columns)
            .map_err(|e| StructuredError::Sql(e.to_string()))
    }
}

impl<S: Send + 'static> BatchEngine for MapGroupsStateEngine<S> {
    fn process(&self, epoch: u64) -> StructuredResult<Vec<RecordBatch>> {
        let batches = self.source.next_batch(epoch);
        let mut op = self.op.lock();

        let mut out_rows: Vec<Row> = Vec::new();
        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let grouped = self.group_batch(batch)?;
            out_rows.extend(op.process_batch(grouped));
        }
        // Processing-time timeouts fire against this batch's epoch (ms since start).
        out_rows.extend(op.evict_timed_out(epoch));

        if out_rows.is_empty() {
            return Ok(vec![]);
        }
        Ok(vec![self.rows_to_batch(out_rows)?])
    }

    fn post_commit(&self, epoch: u64) {
        self.source.post_batch_commit(epoch);
    }
}
