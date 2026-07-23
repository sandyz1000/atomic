//! Deduplication within a watermark: drop repeated rows keyed by a column set, keeping the
//! first occurrence, and evict per-key state once the event-time watermark passes it.
//!
//! Mirrors Spark's `dropDuplicatesWithinWatermark`. State stays bounded because a key whose
//! last-seen event time falls below the watermark is forgotten; a later duplicate past the
//! watermark is treated as new (the same bound-vs-exactness trade-off Spark makes).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::UInt32Array;
use datafusion::arrow::compute::{cast, take as arrow_take};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use parking_lot::Mutex;

use crate::errors::{StructuredError, StructuredResult};
use crate::query::BatchEngine;
use crate::source::StreamSource;
use crate::watermark::WatermarkTracker;

/// Per-key state: the last event time the key was seen at, used for watermark eviction.
type SeenKeys = HashMap<String, u64>;

/// Engine that emits only the first row per distinct key, bounded by the watermark.
pub(crate) struct DedupEngine {
    source: Arc<dyn StreamSource>,
    key_cols: Vec<String>,
    time_col: String,
    watermark: Mutex<WatermarkTracker>,
    seen: Mutex<SeenKeys>,
}

impl DedupEngine {
    pub(crate) fn new(
        source: Arc<dyn StreamSource>,
        key_cols: Vec<String>,
        time_col: String,
        watermark_delay_ms: u64,
    ) -> Self {
        DedupEngine {
            source,
            key_cols,
            time_col,
            watermark: Mutex::new(WatermarkTracker::new(watermark_delay_ms)),
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// Build one composite string key per row from the key columns (cast to Utf8, tab-joined).
    fn row_keys(&self, batch: &RecordBatch) -> StructuredResult<Vec<String>> {
        use datafusion::arrow::array::{Array, StringArray};
        let n = batch.num_rows();
        let mut keys = vec![String::new(); n];
        for (ci, col_name) in self.key_cols.iter().enumerate() {
            let arr = batch
                .column_by_name(col_name)
                .ok_or_else(|| StructuredError::Sql(format!("dedup: no column '{col_name}'")))?;
            let utf8 =
                cast(arr, &DataType::Utf8).map_err(|e| StructuredError::Sql(e.to_string()))?;
            let strs = utf8
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| StructuredError::Sql("dedup: cast to Utf8 failed".into()))?;
            for (ri, key) in keys.iter_mut().enumerate() {
                if ci > 0 {
                    key.push('\t');
                }
                if strs.is_valid(ri) {
                    key.push_str(strs.value(ri));
                }
            }
        }
        Ok(keys)
    }

    /// Event times (epoch ms) per row from the time column, cast to `Int64`.
    fn row_times(&self, batch: &RecordBatch) -> StructuredResult<Vec<u64>> {
        use datafusion::arrow::array::Int64Array;
        let arr = batch.column_by_name(&self.time_col).ok_or_else(|| {
            StructuredError::Sql(format!("dedup: no time column '{}'", self.time_col))
        })?;
        let i64s = cast(arr, &DataType::Int64).map_err(|e| StructuredError::Sql(e.to_string()))?;
        let vals = i64s
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| StructuredError::Sql("dedup: time column not Int64".into()))?;
        Ok((0..vals.len())
            .map(|i| vals.value(i).max(0) as u64)
            .collect())
    }
}

impl BatchEngine for DedupEngine {
    fn process(&self, epoch: u64) -> StructuredResult<Vec<RecordBatch>> {
        let batches = self.source.next_batch(epoch);
        let mut out = Vec::new();
        let mut seen = self.seen.lock();
        let mut wm = self.watermark.lock();
        let mut batch_max_time = 0u64;

        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let keys = self.row_keys(batch)?;
            let times = self.row_times(batch)?;
            let mut keep: Vec<u32> = Vec::new();
            for (i, key) in keys.into_iter().enumerate() {
                let t = times[i];
                batch_max_time = batch_max_time.max(t);
                // First occurrence of a still-live key is kept; repeats are dropped.
                if let std::collections::hash_map::Entry::Vacant(e) = seen.entry(key) {
                    e.insert(t);
                    keep.push(i as u32);
                }
            }
            if keep.is_empty() {
                continue;
            }
            let idx = UInt32Array::from(keep);
            let cols = batch
                .columns()
                .iter()
                .map(|c| arrow_take(c, &idx, None))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| StructuredError::Sql(e.to_string()))?;
            out.push(
                RecordBatch::try_new(batch.schema(), cols)
                    .map_err(|e| StructuredError::Sql(e.to_string()))?,
            );
        }

        // Advance the watermark and forget keys that fell below it.
        if batch_max_time > 0 {
            wm.observe_max(batch_max_time);
            if let Some(w) = wm.current() {
                seen.retain(|_, &mut last| last > w);
            }
        }
        Ok(out)
    }

    fn post_commit(&self, epoch: u64) {
        self.source.post_batch_commit(epoch);
    }
}
