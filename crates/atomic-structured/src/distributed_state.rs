//! Distributed stateful windowed aggregation (Part 3 / D1).
//!
//! [`WindowedEngine`] keeps the whole keyed state in a driver-local
//! `Mutex<StateStore>`, which grows unbounded as windows accumulate.
//! [`DistributedStateEngine`] instead shards the state by `StateKey` across the
//! cluster: each micro-batch's partials are routed by a stable hash to one of
//! `num_shards` shards, and a [`TaskAction::MergeState`] task merges them into that
//! shard's persistent state on the owning worker (`WORKER_STATE_STORE`), returning
//! only the cells to emit. The driver computes the partials and assembles the
//! output, but never holds the full cross-batch state.
//!
//! The merge itself is a registered, content-agnostic
//! [`StateMergeFn`](atomic_compute::task_registry::StateMergeFn): the data/compute
//! layers carry no streaming types. The same `dispatch_pipeline` path runs the
//! merge in-process in local mode (shards share the process-global store) and on
//! workers in distributed mode.

use std::sync::Arc;

use atomic_compute::context::Context;
use atomic_data::distributed::{PipelineOp, StateMergePayload, TaskAction, TaskRuntime};
use datafusion::arrow::record_batch::RecordBatch;

use crate::OutputMode;
use crate::errors::{StructuredError, StructuredResult};
use crate::query::BatchEngine;
use crate::state::{AggState, StateKey, StateStore};
use crate::windowed::WindowedEngine;

/// Registered name of the windowed state-merge function.
pub(crate) const WINDOWED_MERGE_FN: &str = "atomic_structured::windowed_v1";

pub(crate) const MODE_APPEND: u8 = 0;
pub(crate) const MODE_UPDATE: u8 = 1;
pub(crate) const MODE_COMPLETE: u8 = 2;

pub(crate) fn mode_code(mode: OutputMode) -> u8 {
    match mode {
        OutputMode::Append => MODE_APPEND,
        OutputMode::Update => MODE_UPDATE,
        OutputMode::Complete => MODE_COMPLETE,
    }
}

/// Per-batch merge/emit configuration shipped to each shard (bincode-encoded into
/// `StateMergePayload.params`).
#[derive(bincode::Encode, bincode::Decode)]
struct WindowedMergeParams {
    /// Post-batch watermark; `None` until the first event time is observed.
    watermark_ms: Option<u64>,
    window_size_ms: u64,
    /// Output mode code (`MODE_*`).
    mode: u8,
}

/// Stable shard assignment for a key (FNV-1a over its bincode encoding, so it is
/// deterministic across driver and workers — the `Hash` derive is not). Used for
/// the windowed `StateKey` and the session/join group key alike.
pub(crate) fn shard_of<T: bincode::Encode>(key: &T, num_shards: u32) -> u32 {
    let bytes = bincode::encode_to_vec(key, bincode::config::standard()).unwrap_or_default();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in &bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h % num_shards as u64) as u32
}

/// The registered windowed state-merge: merge this batch's partials into the
/// shard's state, then emit per output mode. `prev` is the shard's current
/// serialized [`StateStore`] (`None` on first use).
fn windowed_state_merge(
    prev: Option<&[u8]>,
    partials: &[u8],
    params: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut store = match prev {
        Some(b) => StateStore::decode(b).map_err(|e| e.to_string())?,
        None => StateStore::new(),
    };
    let cfg = bincode::config::standard();
    let (cells, _): (Vec<(StateKey, Vec<AggState>)>, _) =
        bincode::decode_from_slice(partials, cfg).map_err(|e| e.to_string())?;
    let touched: Vec<StateKey> = cells.iter().map(|(k, _)| k.clone()).collect();
    for (k, v) in cells {
        store.merge(k, v);
    }

    let (params, _): (WindowedMergeParams, _) =
        bincode::decode_from_slice(params, cfg).map_err(|e| e.to_string())?;
    let emitted: Vec<(StateKey, Vec<AggState>)> = match params.mode {
        MODE_UPDATE => touched
            .iter()
            .filter_map(|k| store.get(k).map(|v| (k.clone(), v.clone())))
            .collect(),
        MODE_APPEND => match params.watermark_ms {
            Some(w) => store.evict_final(w, params.window_size_ms),
            None => vec![],
        },
        _ => store.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
    };

    let new_state = store.encode().map_err(|e| e.to_string())?;
    let emitted_bytes = bincode::encode_to_vec(&emitted, cfg).map_err(|e| e.to_string())?;
    Ok((new_state, emitted_bytes))
}

atomic_compute::register_state_merge!(WINDOWED_MERGE_FN, windowed_state_merge);

/// Windowed aggregation whose keyed state is sharded across the cluster.
///
/// Wraps a [`WindowedEngine`] for the driver-side per-batch work (partial SQL,
/// late-data filtering, watermark, output assembly) but replaces its local state
/// merge with a sharded, worker-resident merge dispatched per batch.
pub(crate) struct DistributedStateEngine {
    inner: WindowedEngine,
    sc: Arc<Context>,
    num_shards: u32,
    /// Base `state_id`; shard `i` uses `state_id_base + i` so multiple queries in
    /// one process do not collide.
    state_id_base: u64,
    /// When set, each shard's post-merge state is checkpointed under this directory
    /// and reloaded on a cold shard after a restart.
    checkpoint_dir: Option<String>,
}

impl DistributedStateEngine {
    pub(crate) fn new(
        inner: WindowedEngine,
        sc: Arc<Context>,
        num_shards: u32,
        query_id: u64,
        checkpoint_dir: Option<String>,
    ) -> Self {
        DistributedStateEngine {
            inner,
            sc,
            num_shards: num_shards.max(1),
            state_id_base: query_id << 16,
            checkpoint_dir,
        }
    }
}

impl BatchEngine for DistributedStateEngine {
    fn post_commit(&self, epoch: u64) {
        self.inner.post_commit(epoch);
    }

    fn process(&self, epoch: u64) -> StructuredResult<Vec<RecordBatch>> {
        let spec = self.inner.spec();
        let window_size_ms = spec.window_size_ms;
        let mode = spec.mode;
        let bp = self.inner.compute_partials(epoch)?;

        // With no new data, only Append (watermark-driven eviction) and Complete
        // (re-emit) need a round; Update emits nothing.
        if !bp.had_data && mode == OutputMode::Update {
            return Ok(vec![]);
        }

        // Route partials to shards by stable key hash.
        let mut by_shard: Vec<Vec<(StateKey, Vec<AggState>)>> =
            vec![Vec::new(); self.num_shards as usize];
        for (k, v) in bp.cells {
            let shard = shard_of(&k, self.num_shards) as usize;
            by_shard[shard].push((k, v));
        }

        let cfg = bincode::config::standard();
        let params = WindowedMergeParams {
            watermark_ms: bp.wm_after,
            window_size_ms,
            mode: mode_code(mode),
        };
        let params_bytes = bincode::encode_to_vec(&params, cfg)
            .map_err(|e| StructuredError::Sql(e.to_string()))?;

        // One MergeState task per shard. Empty shards still run so Append eviction
        // and Complete re-emission cover state with no new partials this batch.
        let mut source_partitions: Vec<Vec<u8>> = Vec::with_capacity(self.num_shards as usize);
        for (shard, cells) in by_shard.into_iter().enumerate() {
            let partials = bincode::encode_to_vec(&cells, cfg)
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

        let ops = vec![PipelineOp {
            op_id: String::new(),
            action: TaskAction::MergeState {
                merge_fn: WINDOWED_MERGE_FN.to_string(),
            },
            runtime: TaskRuntime::Native,
            payload: vec![],
        }];

        let results = self
            .sc
            .dispatch_pipeline(source_partitions, ops)
            .map_err(|e| StructuredError::Sql(format!("distributed state merge: {e}")))?;

        // Reassemble emitted cells from every shard into the output batch.
        let mut emitted: Vec<(StateKey, Vec<AggState>)> = Vec::new();
        for bytes in results {
            let (cells, _): (Vec<(StateKey, Vec<AggState>)>, _) =
                bincode::decode_from_slice(&bytes, cfg)
                    .map_err(|e| StructuredError::Sql(format!("emitted decode: {e}")))?;
            emitted.extend(cells);
        }

        self.inner.emit_batch(&emitted)
    }
}
