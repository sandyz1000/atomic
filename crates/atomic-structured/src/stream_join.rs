//! Stream-stream join engine (C3).
//!
//! Inner and outer equi-joins of two streaming sources, bounded by a
//! `join_time_bound`: rows older than `watermark − time_bound` are evicted and,
//! for outer joins, emitted as unmatched before they leave the buffer.
//!
//! # Design
//!
//! Each side buffers its rows in a `JoinStateStore` keyed by the join column.
//! On every batch both sources are drained; new left rows probe the right buffer
//! for matching keys (and vice versa). Outer-unmatched rows are emitted once the
//! watermark passes `row_time + time_bound`.
//!
//! This is driver-local (no distributed state), consistent with the rest of the
//! structured streaming engine.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use parking_lot::Mutex;

use crate::errors::{StructuredError, StructuredResult};
use crate::query::BatchEngine;
use crate::source::StreamSource;
use crate::state::GroupVal;
use crate::watermark::WatermarkTracker;
use crate::windowed::{read_group, read_i64};

// ── Join type ─────────────────────────────────────────────────────────────────

/// Which rows are emitted from an equi-join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
}

// ── JoinStateStore ────────────────────────────────────────────────────────────

/// One row buffered in a join side, with the event-time and a condensed
/// representation of all column values.
#[derive(Clone, Debug)]
pub struct JoinRow {
    pub time_ms: i64,
    pub cols: Vec<GroupVal>,
}

/// Per-side key-indexed buffer for stream-stream joins.
pub struct JoinStateStore {
    /// join_key_vals → buffered rows with that key
    map: HashMap<Vec<GroupVal>, Vec<JoinRow>>,
}

impl JoinStateStore {
    pub fn new() -> Self {
        JoinStateStore {
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: Vec<GroupVal>, row: JoinRow) {
        self.map.entry(key).or_default().push(row);
    }

    pub fn get(&self, key: &[GroupVal]) -> &[JoinRow] {
        self.map.get(key).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Remove rows whose `time_ms < evict_before_ms`.
    pub fn evict_before(&mut self, evict_before_ms: i64) {
        for rows in self.map.values_mut() {
            rows.retain(|r| r.time_ms >= evict_before_ms);
        }
        self.map.retain(|_, rows| !rows.is_empty());
    }

    /// Drain and return all rows whose `time_ms + time_bound < watermark_ms`
    /// (rows that will never get a match — used for outer unmatched emission).
    pub fn drain_unmatched(&mut self, watermark_ms: i64, time_bound_ms: i64) -> Vec<JoinRow> {
        let threshold = watermark_ms - time_bound_ms;
        let mut unmatched = Vec::new();
        for rows in self.map.values_mut() {
            let mut i = 0;
            while i < rows.len() {
                if rows[i].time_ms < threshold {
                    unmatched.push(rows.swap_remove(i));
                } else {
                    i += 1;
                }
            }
        }
        self.map.retain(|_, rows| !rows.is_empty());
        unmatched
    }
}

// ── Join spec ─────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct StreamJoinSpec {
    pub left_key_col: String,
    pub right_key_col: String,
    pub join_type: JoinType,
    pub time_bound_ms: u64,
    pub left_time_col: String,
    pub right_time_col: String,
    pub left_schema: Arc<Schema>,
    pub right_schema: Arc<Schema>,
    pub watermark_delay_ms: u64,
}

// ── StreamJoinEngine ──────────────────────────────────────────────────────────

pub(crate) struct StreamJoinEngine {
    left: Arc<dyn StreamSource>,
    right: Arc<dyn StreamSource>,
    spec: StreamJoinSpec,
    left_buf: Mutex<JoinStateStore>,
    right_buf: Mutex<JoinStateStore>,
    watermark: Mutex<WatermarkTracker>,
    /// Index of left key column in left schema.
    left_key_idx: usize,
    /// Index of right key column in right schema.
    right_key_idx: usize,
    /// Index of time column in left schema.
    left_time_idx: usize,
    /// Index of time column in right schema.
    right_time_idx: usize,
    #[allow(dead_code)]
    checkpoint_dir: Option<PathBuf>,
}

impl StreamJoinEngine {
    pub(crate) fn new(
        left: Arc<dyn StreamSource>,
        right: Arc<dyn StreamSource>,
        spec: StreamJoinSpec,
        checkpoint_dir: Option<PathBuf>,
    ) -> StructuredResult<Self> {
        let left_key_idx = spec
            .left_schema
            .index_of(&spec.left_key_col)
            .map_err(|e| StructuredError::Sql(format!("left key col: {e}")))?;
        let right_key_idx = spec
            .right_schema
            .index_of(&spec.right_key_col)
            .map_err(|e| StructuredError::Sql(format!("right key col: {e}")))?;
        let left_time_idx = spec
            .left_schema
            .index_of(&spec.left_time_col)
            .map_err(|e| StructuredError::Sql(format!("left time col: {e}")))?;
        let right_time_idx = spec
            .right_schema
            .index_of(&spec.right_time_col)
            .map_err(|e| StructuredError::Sql(format!("right time col: {e}")))?;
        let watermark = WatermarkTracker::new(spec.watermark_delay_ms);
        Ok(StreamJoinEngine {
            left,
            right,
            spec,
            left_buf: Mutex::new(JoinStateStore::new()),
            right_buf: Mutex::new(JoinStateStore::new()),
            watermark: Mutex::new(watermark),
            left_key_idx,
            right_key_idx,
            left_time_idx,
            right_time_idx,
            checkpoint_dir,
        })
    }

    /// Extract all rows from `batches` as `Vec<JoinRow>`, plus the key for each.
    fn extract_rows(
        &self,
        batches: &[RecordBatch],
        key_idx: usize,
        time_idx: usize,
    ) -> Vec<(Vec<GroupVal>, JoinRow)> {
        let mut out = Vec::new();
        for batch in batches {
            let key_arr = batch.column(key_idx);
            let time_arr = batch.column(time_idx);
            let ncols = batch.num_columns();
            for row in 0..batch.num_rows() {
                let key = vec![read_group(key_arr.as_ref(), row)];
                let time_ms = read_i64(time_arr.as_ref(), row).unwrap_or(0);
                let mut cols = Vec::with_capacity(ncols);
                for ci in 0..ncols {
                    cols.push(read_group(batch.column(ci).as_ref(), row));
                }
                out.push((key, JoinRow { time_ms, cols }));
            }
        }
        out
    }

    /// Build Arrow RecordBatch rows from matched (left_row, right_row) pairs.
    fn build_joined_batch(
        &self,
        pairs: &[(JoinRow, Option<JoinRow>)],
    ) -> StructuredResult<Vec<RecordBatch>> {
        if pairs.is_empty() {
            return Ok(vec![]);
        }
        let ls = &self.spec.left_schema;
        let rs = &self.spec.right_schema;
        let ncols = ls.fields().len() + rs.fields().len();
        let nrows = pairs.len();

        let mut col_data: Vec<Vec<GroupVal>> = vec![Vec::with_capacity(nrows); ncols];
        for (l, r_opt) in pairs {
            for (ci, v) in l.cols.iter().enumerate() {
                col_data[ci].push(v.clone());
            }
            let r_offset = ls.fields().len();
            for ci in 0..rs.fields().len() {
                let v = r_opt
                    .as_ref()
                    .and_then(|r| r.cols.get(ci))
                    .cloned()
                    .unwrap_or(GroupVal::Null);
                col_data[r_offset + ci].push(v);
            }
        }

        // All join output fields are nullable: either side may be absent in an outer join.
        let mut fields: Vec<Field> = ls
            .fields()
            .iter()
            .map(|f| Field::new(f.name(), f.data_type().clone(), true))
            .collect();
        fields.extend(rs.fields().iter().map(|f| {
            let name = if ls.fields().iter().any(|lf| lf.name() == f.name()) {
                format!("{}_right", f.name())
            } else {
                f.name().to_string()
            };
            Field::new(&name, f.data_type().clone(), true)
        }));
        let schema = Arc::new(Schema::new(fields.clone()));

        let columns: Vec<Arc<dyn Array>> = fields
            .iter()
            .enumerate()
            .map(|(ci, field)| -> Arc<dyn Array> {
                match field.data_type() {
                    DataType::Utf8 => Arc::new(StringArray::from(
                        col_data[ci]
                            .iter()
                            .map(|v| match v {
                                GroupVal::Str(s) => Some(s.clone()),
                                _ => None,
                            })
                            .collect::<Vec<_>>(),
                    )),
                    _ => Arc::new(Int64Array::from(
                        col_data[ci]
                            .iter()
                            .map(|v| match v {
                                GroupVal::Int(i) => Some(*i),
                                _ => None,
                            })
                            .collect::<Vec<_>>(),
                    )),
                }
            })
            .collect();

        Ok(vec![RecordBatch::try_new(schema, columns).map_err(
            |e| StructuredError::Sql(format!("join batch: {e}")),
        )?])
    }
}

impl BatchEngine for StreamJoinEngine {
    fn post_commit(&self, epoch: u64) {
        self.left.post_batch_commit(epoch);
        self.right.post_batch_commit(epoch);
    }

    fn process(&self, epoch: u64) -> StructuredResult<Vec<RecordBatch>> {
        let left_batches = self.left.next_batch(epoch);
        let right_batches = self.right.next_batch(epoch);

        // Advance watermark from both sides.
        let wm = {
            let mut tracker = self.watermark.lock();
            for b in left_batches.iter().chain(right_batches.iter()) {
                let tc = if b.schema().index_of(&self.spec.left_time_col).is_ok() {
                    self.spec.left_time_col.clone()
                } else {
                    self.spec.right_time_col.clone()
                };
                if let Some(m) = batch_max_time(&[b.clone()], &tc) {
                    tracker.observe_max(m);
                }
            }
            tracker.current()
        };

        let time_bound = self.spec.time_bound_ms as i64;
        let evict_before = wm.map(|w| w as i64 - time_bound).unwrap_or(i64::MIN);

        let new_left = self.extract_rows(&left_batches, self.left_key_idx, self.left_time_idx);
        let new_right = self.extract_rows(&right_batches, self.right_key_idx, self.right_time_idx);

        let mut matched_pairs: Vec<(JoinRow, Option<JoinRow>)> = Vec::new();

        // 1. New left probes buffered right (rows from previous batches).
        {
            let right_buf = self.right_buf.lock();
            for (key, lrow) in &new_left {
                for rrow in right_buf.get(key) {
                    if (lrow.time_ms - rrow.time_ms).abs() <= time_bound {
                        matched_pairs.push((lrow.clone(), Some(rrow.clone())));
                    }
                }
            }
        }
        // 2. New right probes buffered left (rows from previous batches).
        {
            let left_buf = self.left_buf.lock();
            for (key, rrow) in &new_right {
                for lrow in left_buf.get(key) {
                    if (lrow.time_ms - rrow.time_ms).abs() <= time_bound {
                        matched_pairs.push((lrow.clone(), Some(rrow.clone())));
                    }
                }
            }
        }
        // 3. Same-batch cross-probe: new left × new right (rows co-arriving this epoch).
        {
            // Build a per-key index into new_right for O(N) lookup.
            let mut right_index: std::collections::HashMap<&[GroupVal], Vec<&JoinRow>> =
                std::collections::HashMap::new();
            for (key, rrow) in &new_right {
                right_index.entry(key.as_slice()).or_default().push(rrow);
            }
            for (key, lrow) in &new_left {
                if let Some(rvec) = right_index.get(key.as_slice()) {
                    for rrow in rvec {
                        if (lrow.time_ms - rrow.time_ms).abs() <= time_bound {
                            matched_pairs.push((lrow.clone(), Some((*rrow).clone())));
                        }
                    }
                }
            }
        }

        // Outer unmatched: drain rows past their match window BEFORE eviction so they
        // can be emitted.  `drain_unmatched` and `evict_before` use the same threshold,
        // so draining first means evict_before finds nothing to add for those rows.
        if self.spec.join_type == JoinType::LeftOuter {
            if let Some(wm_ms) = wm {
                let unmatched = self
                    .left_buf
                    .lock()
                    .drain_unmatched(wm_ms as i64, time_bound);
                for row in unmatched {
                    matched_pairs.push((row, None));
                }
            }
        }
        if self.spec.join_type == JoinType::RightOuter {
            if let Some(wm_ms) = wm {
                let unmatched = self
                    .right_buf
                    .lock()
                    .drain_unmatched(wm_ms as i64, time_bound);
                for row in unmatched {
                    matched_pairs.push((
                        JoinRow {
                            time_ms: row.time_ms,
                            cols: vec![GroupVal::Null; self.spec.left_schema.fields().len()],
                        },
                        Some(row),
                    ));
                }
            }
        }

        // Buffer the new rows (after probing to avoid self-matches from same epoch).
        {
            let mut left_buf = self.left_buf.lock();
            for (key, row) in new_left {
                left_buf.insert(key, row);
            }
            left_buf.evict_before(evict_before);
        }
        {
            let mut right_buf = self.right_buf.lock();
            for (key, row) in new_right {
                right_buf.insert(key, row);
            }
            right_buf.evict_before(evict_before);
        }

        self.build_joined_batch(&matched_pairs)
    }
}

fn batch_max_time(batches: &[RecordBatch], col: &str) -> Option<u64> {
    let mut max: Option<u64> = None;
    for b in batches {
        let Ok(idx) = b.schema().index_of(col) else {
            continue;
        };
        let arr = b.column(idx);
        for row in 0..b.num_rows() {
            if let Some(v) = read_i64(arr.as_ref(), row).filter(|v| *v >= 0) {
                let v = v as u64;
                max = Some(max.map_or(v, |m| m.max(v)));
            }
        }
    }
    max
}
