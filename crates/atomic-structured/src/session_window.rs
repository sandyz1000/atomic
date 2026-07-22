//! Session window engine (C2).
//!
//! Sessions have **dynamic** bounds: events within `gap` of each other coalesce into
//! one session; a gap wider than `gap_ms` starts a new one.
//!
//! # Design
//!
//! `SessionStore` holds per-group lists of open sessions. For each incoming event
//! at time `t`:
//!   1. Find all sessions whose `[session_start − gap, session_end + gap]` contains `t`.
//!   2. If zero: open a new `{start: t, end: t, agg_state}`.
//!   3. If one: extend it (update start/end, merge agg_state).
//!   4. If two or more (the event bridges them): merge all into one session.
//!
//! A session is finalized and emitted (Append) or re-emitted (Update) when
//! `session_end + gap ≤ watermark`.
//!
//! Complete output mode is rejected for sessions — the key space is unbounded.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use parking_lot::Mutex;

use atomic_compute::context::Context;
use atomic_data::distributed::{EngineStep, StateMergePayload, Step, StepKind, TaskRuntime};

use crate::OutputMode;
use crate::distributed_state::{MODE_APPEND, mode_code, shard_of};
use crate::errors::{StructuredError, StructuredResult};
use crate::query::BatchEngine;
use crate::source::StreamSource;
use crate::state::{Agg, AggKind, AggState, GroupVal};
use crate::watermark::WatermarkTracker;
use crate::windowed::{read_group, read_i64};

/// One extracted event: its grouping-key values, event time, and per-aggregate partials.
type SessionEvent = (Vec<GroupVal>, i64, Vec<AggState>);

// ── Session state ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, bincode::Encode, bincode::Decode)]
struct Session {
    start_ms: i64,
    end_ms: i64,
    agg: Vec<AggState>,
}

impl Session {
    fn new(t: i64, partial: Vec<AggState>) -> Self {
        Session {
            start_ms: t,
            end_ms: t,
            agg: partial,
        }
    }

    fn extend(&mut self, t: i64, partial: &[AggState]) {
        self.start_ms = self.start_ms.min(t);
        self.end_ms = self.end_ms.max(t);
        for (e, p) in self.agg.iter_mut().zip(partial.iter()) {
            e.merge(p);
        }
    }

    fn merge_with(&mut self, other: Session) {
        self.start_ms = self.start_ms.min(other.start_ms);
        self.end_ms = self.end_ms.max(other.end_ms);
        for (e, p) in self.agg.iter_mut().zip(other.agg.iter()) {
            e.merge(p);
        }
    }

    fn is_final(&self, gap_ms: i64, watermark_ms: i64) -> bool {
        self.end_ms + gap_ms <= watermark_ms
    }
}

#[derive(bincode::Encode, bincode::Decode)]
struct SessionStore {
    map: HashMap<Vec<GroupVal>, Vec<Session>>,
}

impl SessionStore {
    fn new() -> Self {
        SessionStore {
            map: HashMap::new(),
        }
    }

    fn absorb(&mut self, group: Vec<GroupVal>, t: i64, partial: Vec<AggState>, gap_ms: i64) {
        let sessions = self.map.entry(group).or_default();

        let overlapping: Vec<usize> = sessions
            .iter()
            .enumerate()
            .filter(|(_, s)| s.start_ms - gap_ms <= t && t <= s.end_ms + gap_ms)
            .map(|(i, _)| i)
            .collect();

        if overlapping.is_empty() {
            sessions.push(Session::new(t, partial));
            return;
        }

        let first_idx = overlapping[0];
        sessions[first_idx].extend(t, &partial);

        // Remove extra overlapping sessions from largest index to smallest so that
        // first_idx (the smallest) is never shifted by a swap_remove.
        let to_merge: Vec<Session> = overlapping[1..]
            .iter()
            .rev()
            .copied()
            .map(|i| sessions.swap_remove(i))
            .collect();
        for extra in to_merge {
            sessions[first_idx].merge_with(extra);
        }
    }

    fn drain_final(&mut self, gap_ms: i64, watermark_ms: i64) -> Vec<(Vec<GroupVal>, Session)> {
        let mut out = Vec::new();
        for (group, sessions) in &mut self.map {
            let mut i = 0;
            while i < sessions.len() {
                if sessions[i].is_final(gap_ms, watermark_ms) {
                    out.push((group.clone(), sessions.swap_remove(i)));
                } else {
                    i += 1;
                }
            }
        }
        self.map.retain(|_, v| !v.is_empty());
        out
    }

    fn all_sessions(&self) -> Vec<(Vec<GroupVal>, Session)> {
        self.map
            .iter()
            .flat_map(|(g, ss)| ss.iter().map(move |s| (g.clone(), s.clone())))
            .collect()
    }
}

// ── Spec ──────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct SessionSpec {
    pub time_col: String,
    pub gap_ms: u64,
    pub watermark_delay_ms: Option<u64>,
    pub group_cols: Vec<String>,
    pub aggs: Vec<Agg>,
    pub mode: OutputMode,
}

// ── Engine ────────────────────────────────────────────────────────────────────

pub(crate) struct SessionEngine {
    source: Arc<dyn StreamSource>,
    spec: SessionSpec,
    store: Mutex<SessionStore>,
    watermark: Mutex<WatermarkTracker>,
    #[allow(dead_code)]
    checkpoint_dir: Option<PathBuf>,
}

impl SessionEngine {
    pub(crate) fn new(
        source: Arc<dyn StreamSource>,
        spec: SessionSpec,
        checkpoint_dir: Option<PathBuf>,
    ) -> StructuredResult<Self> {
        let watermark = WatermarkTracker::new(spec.watermark_delay_ms.unwrap_or(0));
        Ok(SessionEngine {
            source,
            spec,
            store: Mutex::new(SessionStore::new()),
            watermark: Mutex::new(watermark),
            checkpoint_dir,
        })
    }

    fn extract_events(&self, batch: &RecordBatch) -> StructuredResult<Vec<SessionEvent>> {
        let schema = batch.schema();
        let time_idx = schema
            .index_of(&self.spec.time_col)
            .map_err(|e| StructuredError::Sql(format!("time col '{}': {e}", self.spec.time_col)))?;
        let time_arr = batch.column(time_idx);

        let group_idxs: Vec<usize> = self
            .spec
            .group_cols
            .iter()
            .map(|gc| {
                schema
                    .index_of(gc)
                    .map_err(|e| StructuredError::Sql(format!("group col '{gc}': {e}")))
            })
            .collect::<StructuredResult<_>>()?;

        let agg_idxs: Vec<Option<usize>> = self
            .spec
            .aggs
            .iter()
            .map(|agg| {
                agg.input_col
                    .as_ref()
                    .map(|ic| {
                        schema
                            .index_of(ic)
                            .map_err(|e| StructuredError::Sql(format!("agg col '{ic}': {e}")))
                    })
                    .transpose()
            })
            .collect::<StructuredResult<_>>()?;

        let mut out = Vec::new();
        for row in 0..batch.num_rows() {
            let Some(t) = read_i64(time_arr.as_ref(), row) else {
                continue;
            };
            let group: Vec<GroupVal> = group_idxs
                .iter()
                .map(|&ci| read_group(batch.column(ci).as_ref(), row))
                .collect();

            let partial: Vec<AggState> = self
                .spec
                .aggs
                .iter()
                .zip(agg_idxs.iter())
                .map(|(agg, ci_opt)| match agg.kind {
                    AggKind::Count => AggState::Count(1),
                    AggKind::Sum => {
                        let v = ci_opt
                            .and_then(|ci| read_i64(batch.column(ci).as_ref(), row))
                            .unwrap_or(0) as f64;
                        AggState::Sum(v)
                    }
                    AggKind::Min => {
                        let v = ci_opt
                            .and_then(|ci| read_i64(batch.column(ci).as_ref(), row))
                            .unwrap_or(i64::MAX) as f64;
                        AggState::Min(v)
                    }
                    AggKind::Max => {
                        let v = ci_opt
                            .and_then(|ci| read_i64(batch.column(ci).as_ref(), row))
                            .unwrap_or(i64::MIN) as f64;
                        AggState::Max(v)
                    }
                    AggKind::Avg => {
                        let v = ci_opt
                            .and_then(|ci| read_i64(batch.column(ci).as_ref(), row))
                            .unwrap_or(0) as f64;
                        AggState::Avg { sum: v, count: 1 }
                    }
                })
                .collect();
            out.push((group, t, partial));
        }
        Ok(out)
    }

    fn emit_batch(&self, cells: &[(Vec<GroupVal>, Session)]) -> StructuredResult<Vec<RecordBatch>> {
        if cells.is_empty() {
            return Ok(vec![]);
        }
        let ngroups = self.spec.group_cols.len();

        let mut fields = vec![
            Field::new("session_start", DataType::Int64, false),
            Field::new("session_end", DataType::Int64, false),
        ];
        for (gi, name) in self.spec.group_cols.iter().enumerate() {
            let dt = match cells[0].0.get(gi) {
                Some(GroupVal::Str(_)) => DataType::Utf8,
                _ => DataType::Int64,
            };
            fields.push(Field::new(name, dt, true));
        }
        for agg in &self.spec.aggs {
            let dt = if agg.kind == AggKind::Count {
                DataType::Int64
            } else {
                DataType::Float64
            };
            fields.push(Field::new(&agg.output_col, dt, false));
        }
        let schema = Arc::new(Schema::new(fields));

        let mut columns: Vec<Arc<dyn Array>> = Vec::new();
        columns.push(Arc::new(Int64Array::from(
            cells.iter().map(|(_, s)| s.start_ms).collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            cells.iter().map(|(_, s)| s.end_ms).collect::<Vec<_>>(),
        )));

        for gi in 0..ngroups {
            let is_str = matches!(cells[0].0.get(gi), Some(GroupVal::Str(_)));
            if is_str {
                let vals: Vec<Option<String>> = cells
                    .iter()
                    .map(|(g, _)| match g.get(gi) {
                        Some(GroupVal::Str(v)) => Some(v.clone()),
                        _ => None,
                    })
                    .collect();
                columns.push(Arc::new(StringArray::from(vals)));
            } else {
                let vals: Vec<Option<i64>> = cells
                    .iter()
                    .map(|(g, _)| match g.get(gi) {
                        Some(GroupVal::Int(v)) => Some(*v),
                        _ => None,
                    })
                    .collect();
                columns.push(Arc::new(Int64Array::from(vals)));
            }
        }

        for (ai, agg) in self.spec.aggs.iter().enumerate() {
            if agg.kind == AggKind::Count {
                let vals: Vec<i64> = cells
                    .iter()
                    .map(|(_, s)| match &s.agg[ai] {
                        AggState::Count(c) => *c,
                        other => other.output_value() as i64,
                    })
                    .collect();
                columns.push(Arc::new(Int64Array::from(vals)));
            } else {
                let vals: Vec<f64> = cells
                    .iter()
                    .map(|(_, s)| s.agg[ai].output_value())
                    .collect();
                columns.push(Arc::new(Float64Array::from(vals)));
            }
        }

        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| StructuredError::Sql(format!("session emit: {e}")))?;
        Ok(vec![batch])
    }

    /// Driver-side per-batch work shared by the local and distributed session
    /// engines: read the batch, extract `(group, time, partial)` events, drop
    /// events whose session is already closed before this batch (late), and advance
    /// the watermark. The caller absorbs the events into a store (local for
    /// [`SessionEngine`], sharded for `DistributedSessionEngine`) and emits.
    fn compute_events(&self, epoch: u64) -> StructuredResult<SessionBatch> {
        let batches = self.source.next_batch(epoch);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let wm_before = self.watermark.lock().current();
        if total_rows == 0 {
            return Ok(SessionBatch {
                events: vec![],
                wm_after: wm_before,
                had_data: false,
            });
        }

        let batch_max = {
            let mut m: Option<u64> = None;
            for b in &batches {
                let Ok(idx) = b.schema().index_of(&self.spec.time_col) else {
                    continue;
                };
                let arr = b.column(idx);
                for row in 0..b.num_rows() {
                    if let Some(v) = read_i64(arr.as_ref(), row).filter(|v| *v >= 0) {
                        m = Some(m.map_or(v as u64, |x| x.max(v as u64)));
                    }
                }
            }
            m
        };
        if let Some(m) = batch_max {
            self.watermark.lock().observe_max(m);
        }
        let wm_after = self.watermark.lock().current();
        let gap = self.spec.gap_ms as i64;

        let mut events: Vec<SessionEvent> = Vec::new();
        for b in &batches {
            for (group, t, partial) in self.extract_events(b)? {
                if matches!(wm_before, Some(wm) if t + gap <= wm as i64) {
                    continue;
                }
                events.push((group, t, partial));
            }
        }
        Ok(SessionBatch {
            events,
            wm_after,
            had_data: true,
        })
    }
}

/// Result of [`SessionEngine::compute_events`]: this batch's late-filtered events
/// plus the post-batch watermark.
struct SessionBatch {
    events: Vec<SessionEvent>,
    wm_after: Option<u64>,
    had_data: bool,
}

impl BatchEngine for SessionEngine {
    fn post_commit(&self, epoch: u64) {
        self.source.post_batch_commit(epoch);
    }

    fn process(&self, epoch: u64) -> StructuredResult<Vec<RecordBatch>> {
        let gap = self.spec.gap_ms as i64;
        let sb = self.compute_events(epoch)?;

        if !sb.had_data {
            if self.spec.mode == OutputMode::Update {
                return self.emit_batch(&self.store.lock().all_sessions());
            }
            return Ok(vec![]);
        }

        {
            let mut store = self.store.lock();
            for (group, t, partial) in sb.events {
                store.absorb(group, t, partial, gap);
            }
        }

        let emitted: Vec<(Vec<GroupVal>, Session)> = match self.spec.mode {
            OutputMode::Append => match sb.wm_after {
                Some(wm) => self.store.lock().drain_final(gap, wm as i64),
                None => vec![],
            },
            OutputMode::Update => self.store.lock().all_sessions(),
            OutputMode::Complete => {
                return Err(StructuredError::Unsupported(
                    "Complete output mode is not supported for session windows".into(),
                ));
            }
        };

        self.emit_batch(&emitted)
    }
}

// ── Distributed session state (Part 3 / D4) ─────────────────────────────────────

/// Registered name of the session state-merge function.
pub(crate) const SESSION_MERGE_FN: &str = "atomic_structured::session_v1";

/// Per-shard merge/emit config for session windows.
#[derive(bincode::Encode, bincode::Decode)]
struct SessionMergeParams {
    watermark_ms: Option<u64>,
    gap_ms: u64,
    mode: u8,
}

/// Registered session state-merge: absorb this batch's events into the shard's
/// `SessionStore` (coalescing / merging sessions within `gap`), then emit per mode.
fn session_state_merge(
    prev: Option<&[u8]>,
    events: &[u8],
    params: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), String> {
    let cfg = bincode::config::standard();
    let mut store = match prev {
        Some(b) => bincode::decode_from_slice::<SessionStore, _>(b, cfg)
            .map(|(s, _)| s)
            .map_err(|e| e.to_string())?,
        None => SessionStore::new(),
    };
    let (events, _): (Vec<SessionEvent>, _) =
        bincode::decode_from_slice(events, cfg).map_err(|e| e.to_string())?;
    let (params, _): (SessionMergeParams, _) =
        bincode::decode_from_slice(params, cfg).map_err(|e| e.to_string())?;
    let gap = params.gap_ms as i64;
    for (group, t, partial) in events {
        store.absorb(group, t, partial, gap);
    }
    let emitted: Vec<(Vec<GroupVal>, Session)> = match params.mode {
        MODE_APPEND => match params.watermark_ms {
            Some(w) => store.drain_final(gap, w as i64),
            None => vec![],
        },
        // Update — Complete is rejected for sessions before dispatch.
        _ => store.all_sessions(),
    };
    let new_state = bincode::encode_to_vec(&store, cfg).map_err(|e| e.to_string())?;
    let emitted_bytes = bincode::encode_to_vec(&emitted, cfg).map_err(|e| e.to_string())?;
    Ok((new_state, emitted_bytes))
}

atomic_compute::register_state_merge!(SESSION_MERGE_FN, session_state_merge);

/// Session-window aggregation whose per-group state is sharded across the cluster.
///
/// Wraps a [`SessionEngine`] for the driver-side event extraction and output, but
/// merges each batch's events into worker-resident `SessionStore` shards (routed by
/// a stable group-key hash) via `MergeState` tasks instead of one driver-local
/// store.
pub(crate) struct DistributedSessionEngine {
    inner: SessionEngine,
    sc: Arc<Context>,
    num_shards: u32,
    state_id_base: u64,
    checkpoint_dir: Option<String>,
}

impl DistributedSessionEngine {
    pub(crate) fn new(
        inner: SessionEngine,
        sc: Arc<Context>,
        num_shards: u32,
        query_id: u64,
        checkpoint_dir: Option<String>,
    ) -> Self {
        DistributedSessionEngine {
            inner,
            sc,
            num_shards: num_shards.max(1),
            state_id_base: query_id << 16,
            checkpoint_dir,
        }
    }
}

impl BatchEngine for DistributedSessionEngine {
    fn post_commit(&self, epoch: u64) {
        self.inner.post_commit(epoch);
    }

    fn process(&self, epoch: u64) -> StructuredResult<Vec<RecordBatch>> {
        let gap_ms = self.inner.spec.gap_ms;
        let mode = self.inner.spec.mode;
        if mode == OutputMode::Complete {
            return Err(StructuredError::Unsupported(
                "Complete output mode is not supported for session windows".into(),
            ));
        }
        let sb = self.inner.compute_events(epoch)?;

        // Route events to shards by stable group-key hash; dispatch all shards each
        // batch so Append eviction covers shards with no new events this batch.
        let mut by_shard: Vec<Vec<SessionEvent>> = vec![Vec::new(); self.num_shards as usize];
        for (group, t, partial) in sb.events {
            let shard = shard_of(&group, self.num_shards) as usize;
            by_shard[shard].push((group, t, partial));
        }

        let cfg = bincode::config::standard();
        let params = SessionMergeParams {
            watermark_ms: sb.wm_after,
            gap_ms,
            mode: mode_code(mode),
        };
        let params_bytes = bincode::encode_to_vec(&params, cfg)
            .map_err(|e| StructuredError::Sql(e.to_string()))?;

        let mut source_partitions: Vec<Vec<u8>> = Vec::with_capacity(self.num_shards as usize);
        for (shard, events) in by_shard.into_iter().enumerate() {
            let partials = bincode::encode_to_vec(&events, cfg)
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
                merge_fn: SESSION_MERGE_FN.to_string(),
            }),
            runtime: TaskRuntime::Native,
            payload: vec![],
        }];

        let results = self
            .sc
            .dispatch_pipeline(source_partitions, steps)
            .map_err(|e| StructuredError::Sql(format!("distributed session merge: {e}")))?;

        let mut emitted: Vec<(Vec<GroupVal>, Session)> = Vec::new();
        for bytes in results {
            let (cells, _): (Vec<(Vec<GroupVal>, Session)>, _) =
                bincode::decode_from_slice(&bytes, cfg)
                    .map_err(|e| StructuredError::Sql(format!("session emitted decode: {e}")))?;
            emitted.extend(cells);
        }

        self.inner.emit_batch(&emitted)
    }
}
