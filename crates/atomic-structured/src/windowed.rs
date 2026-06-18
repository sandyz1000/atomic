//! Windowed stateful aggregation engine (4b).
//!
//! Tumbling event-time windows on an epoch-millisecond column, computed each batch
//! as a partial aggregate via DataFusion SQL, merged into a keyed [`StateStore`],
//! and emitted per [`OutputMode`] under a [`WatermarkTracker`]. State + watermark
//! are checkpointed to `{dir}/state.bin` after each batch.

use std::path::PathBuf;
use std::sync::Arc;

use atomic_sql::context::AtomicSqlContext;
use datafusion::arrow::array::{
    Array, Float32Array, Float64Array, Int32Array, Int64Array, StringArray, UInt64Array,
};
use datafusion::arrow::compute::take as arrow_take;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use parking_lot::Mutex;

use atomic_data::distributed::decode_payload;

use crate::OutputMode;
use crate::errors::{StructuredError, StructuredResult};
use crate::query::BatchEngine;
use crate::source::StreamSource;
use crate::state::{Agg, AggKind, AggState, GroupVal, StateKey, StateStore};
use crate::watermark::WatermarkTracker;

/// A windowed-aggregation query specification (built by the frame builder).
#[derive(Clone)]
pub(crate) struct WindowedSpec {
    pub time_col: String,
    pub window_size_ms: u64,
    /// `None` = tumbling (slide == size). `Some(s)` where `s < window_size_ms` = sliding.
    pub slide_ms: Option<u64>,
    pub watermark_delay_ms: Option<u64>,
    pub group_cols: Vec<String>,
    pub aggs: Vec<Agg>,
    pub mode: OutputMode,
}

/// Persisted snapshot: the state store plus the watermark.
#[derive(bincode::Encode, bincode::Decode)]
struct Snapshot {
    store: Vec<u8>,
    watermark_ms: Option<u64>,
}

pub(crate) struct WindowedEngine {
    sql_ctx: AtomicSqlContext,
    runtime: tokio::runtime::Runtime,
    source: Arc<dyn StreamSource>,
    spec: WindowedSpec,
    state: Mutex<StateStore>,
    watermark: Mutex<WatermarkTracker>,
    checkpoint_dir: Option<PathBuf>,
}

impl WindowedEngine {
    pub(crate) fn new(
        sql_ctx: AtomicSqlContext,
        runtime: tokio::runtime::Runtime,
        source: Arc<dyn StreamSource>,
        spec: WindowedSpec,
        checkpoint_dir: Option<PathBuf>,
    ) -> StructuredResult<Self> {
        let watermark = WatermarkTracker::new(spec.watermark_delay_ms.unwrap_or(0));
        let mut engine = WindowedEngine {
            sql_ctx,
            runtime,
            source,
            spec,
            state: Mutex::new(StateStore::new()),
            watermark: Mutex::new(watermark),
            checkpoint_dir,
        };
        engine.maybe_restore()?;
        Ok(engine)
    }

    /// Reload state + watermark from the checkpoint dir if one exists.
    fn maybe_restore(&mut self) -> StructuredResult<()> {
        let Some(dir) = &self.checkpoint_dir else {
            return Ok(());
        };
        let path = dir.join("state.bin");
        if !path.exists() {
            return Ok(());
        }
        let bytes = std::fs::read(&path).map_err(|e| StructuredError::Checkpoint(e.to_string()))?;
        let snap: Snapshot =
            decode_payload(&bytes).map_err(|e| StructuredError::Checkpoint(e.to_string()))?;
        *self.state.get_mut() = StateStore::decode(&snap.store)?;
        self.watermark.get_mut().restore(snap.watermark_ms);
        Ok(())
    }

    fn persist(&self) -> StructuredResult<()> {
        let Some(dir) = &self.checkpoint_dir else {
            return Ok(());
        };
        std::fs::create_dir_all(dir).map_err(|e| StructuredError::Checkpoint(e.to_string()))?;
        let snap = Snapshot {
            store: self.state.lock().encode()?,
            watermark_ms: self.watermark.lock().current(),
        };
        let bytes = bincode::encode_to_vec(&snap, bincode::config::standard())
            .map_err(|e| StructuredError::Checkpoint(e.to_string()))?;
        let tmp = dir.join("state.bin.tmp");
        std::fs::write(&tmp, &bytes).map_err(|e| StructuredError::Checkpoint(e.to_string()))?;
        std::fs::rename(&tmp, dir.join("state.bin"))
            .map_err(|e| StructuredError::Checkpoint(e.to_string()))?;
        Ok(())
    }

    /// The per-batch partial-aggregate SQL built from the spec.
    ///
    /// For tumbling windows: computes `window_start` in SQL via integer division.
    /// For sliding windows: the `window_start` column was pre-expanded by
    /// `expand_for_sliding` and is referenced directly.
    fn partial_sql(&self) -> String {
        let s = &self.spec;
        let is_sliding = s.slide_ms.is_some_and(|sl| sl < s.window_size_ms);
        let win = if is_sliding {
            "window_start".to_string()
        } else {
            format!(
                "({} / {}) * {}",
                s.time_col, s.window_size_ms, s.window_size_ms
            )
        };
        let mut sel = vec![format!("{win} AS window_start")];
        sel.extend(s.group_cols.iter().cloned());
        for (i, agg) in s.aggs.iter().enumerate() {
            sel.extend(agg.select_exprs(i));
        }
        let mut grp = vec![win];
        grp.extend(s.group_cols.iter().cloned());
        format!(
            "SELECT {} FROM input GROUP BY {}",
            sel.join(", "),
            grp.join(", ")
        )
    }

    /// For sliding windows: expand each row into one row per covering window,
    /// prepending a `window_start` column.  Tumbling callers skip this step.
    fn expand_for_sliding(&self, batches: Vec<RecordBatch>) -> StructuredResult<Vec<RecordBatch>> {
        let size = self.spec.window_size_ms as i64;
        let slide = self.spec.slide_ms.unwrap_or(self.spec.window_size_ms) as i64;

        let mut expanded: Vec<RecordBatch> = Vec::new();
        for batch in &batches {
            let schema = batch.schema();
            let time_idx = schema.index_of(&self.spec.time_col).map_err(|e| {
                StructuredError::Sql(format!(
                    "time column '{}' not found: {e}",
                    self.spec.time_col
                ))
            })?;
            let time_arr = batch.column(time_idx);

            // Build (row_index, window_start) pairs.
            let mut row_windows: Vec<(u64, i64)> = Vec::new();
            for row in 0..batch.num_rows() {
                if let Some(t) = read_i64(time_arr.as_ref(), row) {
                    for ws in covering_windows(t, size, slide) {
                        row_windows.push((row as u64, ws));
                    }
                }
            }
            if row_windows.is_empty() {
                continue;
            }

            let n = row_windows.len();
            let ws_col: Arc<dyn Array> = Arc::new(Int64Array::from(
                row_windows.iter().map(|(_, ws)| *ws).collect::<Vec<_>>(),
            ));
            let indices =
                UInt64Array::from(row_windows.iter().map(|(ri, _)| *ri).collect::<Vec<_>>());

            let mut cols: Vec<Arc<dyn Array>> = Vec::with_capacity(1 + batch.num_columns());
            cols.push(ws_col);
            for col_arr in batch.columns() {
                let taken = arrow_take(col_arr.as_ref(), &indices, None)
                    .map_err(|e| StructuredError::Sql(format!("sliding expand: {e}")))?;
                cols.push(taken);
            }

            let mut fields = vec![Field::new("window_start", DataType::Int64, false)];
            fields.extend(schema.fields().iter().map(|f| f.as_ref().clone()));
            let new_schema = Arc::new(Schema::new(fields));
            let _ = n; // length captured in arrays
            expanded.push(
                RecordBatch::try_new(new_schema, cols)
                    .map_err(|e| StructuredError::Sql(format!("sliding batch: {e}")))?,
            );
        }
        Ok(expanded)
    }
}

// ── Arrow read helpers ────────────────────────────────────────────────────────

pub(crate) fn read_i64(arr: &dyn Array, row: usize) -> Option<i64> {
    if arr.is_null(row) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        Some(a.value(row))
    } else {
        arr.as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(row) as i64)
    }
}

fn read_f64(arr: &dyn Array, row: usize) -> Option<f64> {
    if arr.is_null(row) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        Some(a.value(row))
    } else if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        Some(a.value(row) as f64)
    } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        Some(a.value(row) as f64)
    } else {
        arr.as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(row) as f64)
    }
}

pub(crate) fn read_group(arr: &dyn Array, row: usize) -> GroupVal {
    if arr.is_null(row) {
        return GroupVal::Null;
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        GroupVal::Str(a.value(row).to_string())
    } else if let Some(v) = read_i64(arr, row) {
        GroupVal::Int(v)
    } else {
        GroupVal::Null
    }
}

/// All window starts (multiples of `slide`) that cover event time `t_ms` in a window of `size_ms`.
///
/// A window with start `ws` covers `t` iff `ws <= t < ws + size`.
/// Equivalently: `t - size < ws <= t`, ws is a multiple of slide.
fn covering_windows(t_ms: i64, size_ms: i64, slide_ms: i64) -> Vec<i64> {
    // k_min = floor((t - W) / S) + 1, k_max = floor(t / S)
    let k_min = (t_ms - size_ms).div_euclid(slide_ms) + 1;
    let k_max = t_ms.div_euclid(slide_ms);
    (k_min..=k_max)
        .filter(|&k| k >= 0)
        .map(|k| k * slide_ms)
        .collect()
}

/// Maximum value of the epoch-ms `col` across `batches`, if any.
fn batch_max_ms(batches: &[RecordBatch], col: &str) -> Option<u64> {
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

impl WindowedEngine {
    /// Read one partial-result row into `(StateKey, Vec<AggState>)`.
    fn read_partial_row(
        &self,
        batch: &RecordBatch,
        row: usize,
    ) -> StructuredResult<(StateKey, Vec<AggState>)> {
        let window_start_ms = read_i64(batch.column(0).as_ref(), row)
            .ok_or_else(|| StructuredError::Sql("null window_start".into()))?
            as u64;

        let ngroups = self.spec.group_cols.len();
        let mut group = Vec::with_capacity(ngroups);
        for g in 0..ngroups {
            group.push(read_group(batch.column(1 + g).as_ref(), row));
        }

        // Aggregate columns begin right after window_start + group columns.
        let mut col = 1 + ngroups;
        let mut states = Vec::with_capacity(self.spec.aggs.len());
        for agg in &self.spec.aggs {
            let st = match agg.kind {
                AggKind::Count => {
                    AggState::Count(read_i64(batch.column(col).as_ref(), row).unwrap_or(0))
                }
                AggKind::Sum => {
                    AggState::Sum(read_f64(batch.column(col).as_ref(), row).unwrap_or(0.0))
                }
                AggKind::Min => {
                    AggState::Min(read_f64(batch.column(col).as_ref(), row).unwrap_or(f64::MAX))
                }
                AggKind::Max => {
                    AggState::Max(read_f64(batch.column(col).as_ref(), row).unwrap_or(f64::MIN))
                }
                AggKind::Avg => AggState::Avg {
                    sum: read_f64(batch.column(col).as_ref(), row).unwrap_or(0.0),
                    count: read_i64(batch.column(col + 1).as_ref(), row).unwrap_or(0),
                },
            };
            col += agg.partial_width();
            states.push(st);
        }
        Ok((
            StateKey {
                window_start_ms,
                group,
            },
            states,
        ))
    }

    /// Build the emission `RecordBatch` from a set of `(key, state)` cells.
    pub(crate) fn emit_batch(
        &self,
        cells: &[(StateKey, Vec<AggState>)],
    ) -> StructuredResult<Vec<RecordBatch>> {
        if cells.is_empty() {
            return Ok(vec![]);
        }
        let s = &self.spec;
        let ngroups = s.group_cols.len();

        // Schema: window_start (Int64) + group cols + agg cols.
        let mut fields = vec![Field::new("window_start", DataType::Int64, false)];
        for (gi, name) in s.group_cols.iter().enumerate() {
            // Group col type inferred from the first cell's value.
            let dt = match cells[0].0.group.get(gi) {
                Some(GroupVal::Str(_)) => DataType::Utf8,
                _ => DataType::Int64,
            };
            fields.push(Field::new(name, dt, true));
        }
        for agg in &s.aggs {
            let dt = if agg.kind == AggKind::Count {
                DataType::Int64
            } else {
                DataType::Float64
            };
            fields.push(Field::new(&agg.output_col, dt, false));
        }
        let schema = Arc::new(Schema::new(fields));

        // Columns.
        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
        columns.push(Arc::new(Int64Array::from(
            cells
                .iter()
                .map(|(k, _)| k.window_start_ms as i64)
                .collect::<Vec<_>>(),
        )));
        for gi in 0..ngroups {
            let is_str = matches!(cells[0].0.group.get(gi), Some(GroupVal::Str(_)));
            if is_str {
                let vals: Vec<Option<String>> = cells
                    .iter()
                    .map(|(k, _)| match k.group.get(gi) {
                        Some(GroupVal::Str(v)) => Some(v.clone()),
                        _ => None,
                    })
                    .collect();
                columns.push(Arc::new(StringArray::from(vals)));
            } else {
                let vals: Vec<Option<i64>> = cells
                    .iter()
                    .map(|(k, _)| match k.group.get(gi) {
                        Some(GroupVal::Int(v)) => Some(*v),
                        _ => None,
                    })
                    .collect();
                columns.push(Arc::new(Int64Array::from(vals)));
            }
        }
        for (ai, agg) in s.aggs.iter().enumerate() {
            if agg.kind == AggKind::Count {
                let vals: Vec<i64> = cells
                    .iter()
                    .map(|(_, st)| match &st[ai] {
                        AggState::Count(c) => *c,
                        other => other.output_value() as i64,
                    })
                    .collect();
                columns.push(Arc::new(Int64Array::from(vals)));
            } else {
                let vals: Vec<f64> = cells.iter().map(|(_, st)| st[ai].output_value()).collect();
                columns.push(Arc::new(Float64Array::from(vals)));
            }
        }

        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| StructuredError::Sql(e.to_string()))?;
        Ok(vec![batch])
    }

    /// The windowed spec (window size, mode, aggs) — read by `DistributedStateEngine`
    /// to build merge params and the output schema.
    pub(crate) fn spec(&self) -> &WindowedSpec {
        &self.spec
    }

    /// Driver-side per-batch work shared by the local and distributed engines: read
    /// the source batch, compute per-window partials via SQL, drop windows already
    /// finalized (late), and advance the watermark. The caller applies these
    /// partials to a state store (local `Mutex<StateStore>` for [`WindowedEngine`];
    /// sharded worker stores for `DistributedStateEngine`) and emits.
    pub(crate) fn compute_partials(&self, epoch: u64) -> StructuredResult<BatchPartials> {
        let batches = self.source.next_batch(epoch);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let wm_before = self.watermark.lock().current();
        if total_rows == 0 {
            return Ok(BatchPartials {
                cells: vec![],
                wm_after: wm_before,
                had_data: false,
            });
        }

        let batch_max = batch_max_ms(&batches, &self.spec.time_col);
        let is_sliding = self
            .spec
            .slide_ms
            .is_some_and(|sl| sl < self.spec.window_size_ms);
        let sql_batches = if is_sliding {
            self.expand_for_sliding(batches)?
        } else {
            batches
        };

        let _ = self.sql_ctx.deregister_table("input");
        self.sql_ctx
            .register_batches("input", sql_batches)
            .map_err(|e| StructuredError::Sql(e.to_string()))?;
        let sql = self.partial_sql();
        let partials = self.runtime.block_on(async {
            let df = self
                .sql_ctx
                .sql(&sql)
                .await
                .map_err(|e| StructuredError::Sql(e.to_string()))?;
            df.collect()
                .await
                .map_err(|e| StructuredError::Sql(e.to_string()))
        })?;

        // Drop windows already finalized before this batch (late data).
        let size = self.spec.window_size_ms;
        let mut cells: Vec<(StateKey, Vec<AggState>)> = Vec::new();
        for b in &partials {
            for row in 0..b.num_rows() {
                let (key, partial) = self.read_partial_row(b, row)?;
                let window_end = key.window_start_ms + size;
                if matches!(wm_before, Some(w) if window_end <= w) {
                    continue;
                }
                cells.push((key, partial));
            }
        }

        if let Some(m) = batch_max {
            self.watermark.lock().observe_max(m);
        }
        let wm_after = self.watermark.lock().current();
        Ok(BatchPartials {
            cells,
            wm_after,
            had_data: true,
        })
    }
}

/// Result of [`WindowedEngine::compute_partials`]: this batch's late-filtered
/// partials plus the post-batch watermark.
pub(crate) struct BatchPartials {
    /// `(window-key, per-aggregate partial)` cells to merge into the state store.
    pub cells: Vec<(StateKey, Vec<AggState>)>,
    /// Watermark after observing this batch's max event time.
    pub wm_after: Option<u64>,
    /// Whether the source produced any rows this batch.
    pub had_data: bool,
}

impl BatchEngine for WindowedEngine {
    fn post_commit(&self, epoch: u64) {
        self.source.post_batch_commit(epoch);
    }

    fn process(&self, epoch: u64) -> StructuredResult<Vec<RecordBatch>> {
        let size = self.spec.window_size_ms;
        let bp = self.compute_partials(epoch)?;

        if !bp.had_data {
            // No new data: only Complete mode re-emits the whole store.
            if self.spec.mode == OutputMode::Complete {
                let cells: Vec<(StateKey, Vec<AggState>)> = self
                    .state
                    .lock()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                return self.emit_batch(&cells);
            }
            return Ok(vec![]);
        }

        // Merge this batch's partials into the local store; track touched keys.
        let mut touched: Vec<StateKey> = Vec::with_capacity(bp.cells.len());
        {
            let mut store = self.state.lock();
            for (key, partial) in &bp.cells {
                store.merge(key.clone(), partial.clone());
                touched.push(key.clone());
            }
        }

        // Emit per output mode.
        let emitted = match self.spec.mode {
            OutputMode::Update => {
                let store = self.state.lock();
                touched
                    .iter()
                    .filter_map(|k| store.get(k).map(|v| (k.clone(), v.clone())))
                    .collect::<Vec<_>>()
            }
            OutputMode::Append => match bp.wm_after {
                Some(w) => self.state.lock().evict_final(w, size),
                None => vec![],
            },
            OutputMode::Complete => self
                .state
                .lock()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>(),
        };

        let out = self.emit_batch(&emitted)?;
        self.persist()?;
        Ok(out)
    }
}
