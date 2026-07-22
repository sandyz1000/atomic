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

use atomic_compute::context::Context;
use atomic_data::distributed::{
    EngineStep, StateMergePayload, Step, StepKind, TaskRuntime, decode_payload,
};

use crate::distributed_state::shard_of;
use crate::errors::{StructuredError, StructuredResult};
use crate::query::BatchEngine;
use crate::source::StreamSource;
use crate::state::GroupVal;
use crate::watermark::WatermarkTracker;
use crate::windowed::{read_group, read_i64};

// ── Join type ─────────────────────────────────────────────────────────────────

/// Which rows are emitted from an equi-join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
}

// ── JoinStateStore ────────────────────────────────────────────────────────────

/// One row buffered in a join side, with the event-time and a condensed
/// representation of all column values.
#[derive(Clone, Debug, bincode::Encode, bincode::Decode)]
pub struct JoinRow {
    pub time_ms: i64,
    pub cols: Vec<GroupVal>,
}

/// Per-side key-indexed buffer for stream-stream joins.
#[derive(bincode::Encode, bincode::Decode)]
pub struct JoinStateStore {
    /// join_key_vals → buffered rows with that key
    map: HashMap<Vec<GroupVal>, Vec<JoinRow>>,
}

impl Default for JoinStateStore {
    fn default() -> Self {
        Self::new()
    }
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
        let schema = merged_schema(&self.spec.left_schema, &self.spec.right_schema);
        let col_data = collect_join_columns(pairs, &self.spec.left_schema, &self.spec.right_schema);
        let columns = scalar_columns_to_arrays(&col_data, &schema);
        Ok(vec![RecordBatch::try_new(schema, columns).map_err(
            |e| StructuredError::Sql(format!("join batch: {e}")),
        )?])
    }

    /// Driver-side per-batch work shared by the local and distributed join engines:
    /// pull both sides, advance the watermark, and extract keyed rows. The caller
    /// runs the probe against a buffer pair (local for [`StreamJoinEngine`], sharded
    /// for `DistributedJoinEngine`) via [`probe_and_buffer`].
    fn compute_rows(&self, epoch: u64) -> JoinRows {
        let left_batches = self.left.next_batch(epoch);
        let right_batches = self.right.next_batch(epoch);
        let wm = {
            let mut tracker = self.watermark.lock();
            for b in left_batches.iter().chain(right_batches.iter()) {
                let tc = if b.schema().index_of(&self.spec.left_time_col).is_ok() {
                    self.spec.left_time_col.clone()
                } else {
                    self.spec.right_time_col.clone()
                };
                if let Some(m) = batch_max_time(std::slice::from_ref(b), &tc) {
                    tracker.observe_max(m);
                }
            }
            tracker.current()
        };
        let new_left = self.extract_rows(&left_batches, self.left_key_idx, self.left_time_idx);
        let new_right = self.extract_rows(&right_batches, self.right_key_idx, self.right_time_idx);
        JoinRows {
            new_left,
            new_right,
            wm,
        }
    }
}

/// Build the merged output schema for a join: left fields (all nullable) followed by
/// right fields (all nullable, name-mangled with `_right` on collision).
fn merged_schema(left: &Schema, right: &Schema) -> Arc<Schema> {
    let mut fields: Vec<Field> = left
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), f.data_type().clone(), true))
        .collect();
    fields.extend(right.fields().iter().map(|f| {
        let name = if left.fields().iter().any(|lf| lf.name() == f.name()) {
            format!("{}_right", f.name())
        } else {
            f.name().to_string()
        };
        Field::new(&name, f.data_type().clone(), true)
    }));
    Arc::new(Schema::new(fields))
}

/// Collect per-column `GroupVal` data from matched pairs, null-padding absent right
/// values (outer joins).
fn collect_join_columns(
    pairs: &[(JoinRow, Option<JoinRow>)],
    left_schema: &Schema,
    right_schema: &Schema,
) -> Vec<Vec<GroupVal>> {
    let ncols = left_schema.fields().len() + right_schema.fields().len();
    let nrows = pairs.len();
    let mut col_data: Vec<Vec<GroupVal>> = vec![Vec::with_capacity(nrows); ncols];
    for (l, r_opt) in pairs {
        for (ci, v) in l.cols.iter().enumerate() {
            col_data[ci].push(v.clone());
        }
        let r_offset = left_schema.fields().len();
        for ci in 0..right_schema.fields().len() {
            let v = r_opt
                .as_ref()
                .and_then(|r| r.cols.get(ci))
                .cloned()
                .unwrap_or(GroupVal::Null);
            col_data[r_offset + ci].push(v);
        }
    }
    col_data
}

/// Convert per-column `GroupVal` vecs to typed Arrow arrays matching `schema`.
fn scalar_columns_to_arrays(col_data: &[Vec<GroupVal>], schema: &Schema) -> Vec<Arc<dyn Array>> {
    schema
        .fields()
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
        .collect()
}

/// Result of [`StreamJoinEngine::compute_rows`]: this batch's keyed rows per side
/// and the post-batch watermark.
struct JoinRows {
    new_left: Vec<(Vec<GroupVal>, JoinRow)>,
    new_right: Vec<(Vec<GroupVal>, JoinRow)>,
    wm: Option<u64>,
}

/// Probe `new_left`/`new_right` against the buffer pair, append matches (and outer
/// unmatched), then buffer the new rows and evict expired ones. Shared by the local
/// engine (one buffer pair) and each distributed shard (its own buffer pair), so the
/// join semantics are identical regardless of sharding. `left_ncols` is the left
/// schema width, used to null-pad right-outer unmatched rows.
#[allow(clippy::too_many_arguments)]
fn probe_and_buffer(
    left_buf: &mut JoinStateStore,
    right_buf: &mut JoinStateStore,
    new_left: Vec<(Vec<GroupVal>, JoinRow)>,
    new_right: Vec<(Vec<GroupVal>, JoinRow)>,
    time_bound: i64,
    join_type: JoinType,
    wm: Option<u64>,
    left_ncols: usize,
) -> Vec<(JoinRow, Option<JoinRow>)> {
    let evict_before = wm.map(|w| w as i64 - time_bound).unwrap_or(i64::MIN);
    let mut matched_pairs: Vec<(JoinRow, Option<JoinRow>)> = Vec::new();

    // New left probes buffered right; new right probes buffered left.
    for (key, lrow) in &new_left {
        for rrow in right_buf.get(key) {
            if (lrow.time_ms - rrow.time_ms).abs() <= time_bound {
                matched_pairs.push((lrow.clone(), Some(rrow.clone())));
            }
        }
    }
    for (key, rrow) in &new_right {
        for lrow in left_buf.get(key) {
            if (lrow.time_ms - rrow.time_ms).abs() <= time_bound {
                matched_pairs.push((lrow.clone(), Some(rrow.clone())));
            }
        }
    }
    // Same-batch cross-probe: rows co-arriving this epoch.
    {
        let mut right_index: HashMap<&[GroupVal], Vec<&JoinRow>> = HashMap::new();
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

    // Outer unmatched: drain rows past their match window before eviction.
    if join_type == JoinType::LeftOuter
        && let Some(wm_ms) = wm
    {
        for row in left_buf.drain_unmatched(wm_ms as i64, time_bound) {
            matched_pairs.push((row, None));
        }
    }
    if join_type == JoinType::RightOuter
        && let Some(wm_ms) = wm
    {
        for row in right_buf.drain_unmatched(wm_ms as i64, time_bound) {
            matched_pairs.push((
                JoinRow {
                    time_ms: row.time_ms,
                    cols: vec![GroupVal::Null; left_ncols],
                },
                Some(row),
            ));
        }
    }

    // Buffer the new rows (after probing, to avoid self-matches), then evict.
    for (key, row) in new_left {
        left_buf.insert(key, row);
    }
    left_buf.evict_before(evict_before);
    for (key, row) in new_right {
        right_buf.insert(key, row);
    }
    right_buf.evict_before(evict_before);

    matched_pairs
}

impl BatchEngine for StreamJoinEngine {
    fn post_commit(&self, epoch: u64) {
        self.left.post_batch_commit(epoch);
        self.right.post_batch_commit(epoch);
    }

    fn process(&self, epoch: u64) -> StructuredResult<Vec<RecordBatch>> {
        let r = self.compute_rows(epoch);
        let mut left_buf = self.left_buf.lock();
        let mut right_buf = self.right_buf.lock();
        let matched_pairs = probe_and_buffer(
            &mut left_buf,
            &mut right_buf,
            r.new_left,
            r.new_right,
            self.spec.time_bound_ms as i64,
            self.spec.join_type,
            r.wm,
            self.spec.left_schema.fields().len(),
        );
        drop(left_buf);
        drop(right_buf);
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

// ── Distributed stream-stream join state (Part 3 / D4) ───────────────────────────

/// Registered name of the stream-join state-merge function.
pub(crate) const JOIN_MERGE_FN: &str = "atomic_structured::stream_join_v1";

/// A shard's two-sided buffer state (both join inputs for the keys in the shard).
#[derive(bincode::Encode, bincode::Decode)]
struct JoinShardState {
    left: JoinStateStore,
    right: JoinStateStore,
}

/// Per-shard merge config for a stream-stream join.
#[derive(bincode::Encode, bincode::Decode)]
struct JoinMergeParams {
    watermark_ms: Option<u64>,
    time_bound_ms: u64,
    join_type: JoinType,
    left_ncols: u32,
}

type KeyedRows = Vec<(Vec<GroupVal>, JoinRow)>;

/// Registered join state-merge: probe this batch's rows against the shard's buffer
/// pair, then buffer + evict — identical semantics to the local engine because both
/// call [`probe_and_buffer`]. An equi-join's two matching rows share the key, so they
/// route to the same shard and all matches are found within it.
fn join_state_merge(
    prev: Option<&[u8]>,
    partials: &[u8],
    params: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), String> {
    let cfg = bincode::config::standard();
    let mut state = match prev {
        Some(b) => decode_payload::<JoinShardState>(b).map_err(|e| e.to_string())?,
        None => JoinShardState {
            left: JoinStateStore::new(),
            right: JoinStateStore::new(),
        },
    };
    let (new_left, new_right): (KeyedRows, KeyedRows) =
        decode_payload(partials).map_err(|e| e.to_string())?;
    let params: JoinMergeParams = decode_payload(params).map_err(|e| e.to_string())?;
    let matched = probe_and_buffer(
        &mut state.left,
        &mut state.right,
        new_left,
        new_right,
        params.time_bound_ms as i64,
        params.join_type,
        params.watermark_ms,
        params.left_ncols as usize,
    );
    let new_state = bincode::encode_to_vec(&state, cfg).map_err(|e| e.to_string())?;
    let matched_bytes = bincode::encode_to_vec(&matched, cfg).map_err(|e| e.to_string())?;
    Ok((new_state, matched_bytes))
}

atomic_compute::register_state_merge!(JOIN_MERGE_FN, join_state_merge);

/// Stream-stream join whose two-sided buffer state is sharded across the cluster.
///
/// Wraps a [`StreamJoinEngine`] for driver-side row extraction and output assembly,
/// but routes each batch's rows (by stable join-key hash, so both sides of a match
/// land together) into worker-resident buffer shards via `MergeState` tasks.
pub(crate) struct DistributedJoinEngine {
    inner: StreamJoinEngine,
    sc: Arc<Context>,
    num_shards: u32,
    state_id_base: u64,
    checkpoint_dir: Option<String>,
}

impl DistributedJoinEngine {
    pub(crate) fn new(
        inner: StreamJoinEngine,
        sc: Arc<Context>,
        num_shards: u32,
        query_id: u64,
        checkpoint_dir: Option<String>,
    ) -> Self {
        DistributedJoinEngine {
            inner,
            sc,
            num_shards: num_shards.max(1),
            state_id_base: query_id << 16,
            checkpoint_dir,
        }
    }
}

impl BatchEngine for DistributedJoinEngine {
    fn post_commit(&self, epoch: u64) {
        self.inner.post_commit(epoch);
    }

    fn process(&self, epoch: u64) -> StructuredResult<Vec<RecordBatch>> {
        let n = self.num_shards as usize;
        let time_bound_ms = self.inner.spec.time_bound_ms;
        let join_type = self.inner.spec.join_type;
        let left_ncols = self.inner.spec.left_schema.fields().len() as u32;
        let r = self.inner.compute_rows(epoch);

        // Route both sides by join key; the same key lands in the same shard.
        let mut left_by_shard: Vec<KeyedRows> = vec![Vec::new(); n];
        for (key, row) in r.new_left {
            left_by_shard[shard_of(&key, self.num_shards) as usize].push((key, row));
        }
        let mut right_by_shard: Vec<KeyedRows> = vec![Vec::new(); n];
        for (key, row) in r.new_right {
            right_by_shard[shard_of(&key, self.num_shards) as usize].push((key, row));
        }

        let cfg = bincode::config::standard();
        let params = JoinMergeParams {
            watermark_ms: r.wm,
            time_bound_ms,
            join_type,
            left_ncols,
        };
        let params_bytes = bincode::encode_to_vec(&params, cfg)
            .map_err(|e| StructuredError::Sql(e.to_string()))?;

        let mut source_partitions: Vec<Vec<u8>> = Vec::with_capacity(n);
        for (shard, (lefts, rights)) in left_by_shard.into_iter().zip(right_by_shard).enumerate() {
            let partials = bincode::encode_to_vec(&(lefts, rights), cfg)
                .map_err(|e| StructuredError::Sql(e.to_string()))?;
            let payload = StateMergePayload {
                state_id: self.state_id_base + shard as u64,
                params: params_bytes.clone(),
                partials,
                checkpoint_dir: self.checkpoint_dir.clone(),
            };
            source_partitions.push(
                bincode::encode_to_vec(&payload, cfg)
                    .map_err(|e| StructuredError::Sql(e.to_string()))?,
            );
        }

        let steps = vec![Step {
            task_name: String::new(),
            kind: StepKind::Engine(EngineStep::MergeState {
                merge_fn: JOIN_MERGE_FN.to_string(),
            }),
            runtime: TaskRuntime::Native,
            payload: vec![],
        }];

        let results = self
            .sc
            .dispatch_pipeline(source_partitions, steps)
            .map_err(|e| StructuredError::Sql(format!("distributed join merge: {e}")))?;

        let mut matched: Vec<(JoinRow, Option<JoinRow>)> = Vec::new();
        for bytes in results {
            let pairs: Vec<(JoinRow, Option<JoinRow>)> = decode_payload(&bytes)
                .map_err(|e| StructuredError::Sql(format!("join matched decode: {e}")))?;
            matched.extend(pairs);
        }

        self.inner.build_joined_batch(&matched)
    }
}
