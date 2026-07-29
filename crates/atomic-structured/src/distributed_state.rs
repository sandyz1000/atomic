//! Distributed stateful windowed aggregation (Part 3 / D1).
//!
//! [`WindowedEngine`] keeps the whole keyed state in a driver-local
//! `Mutex<StateStore>`, which grows unbounded as windows accumulate.
//! [`DistributedStateEngine`] instead shards the state by `StateKey` across the
//! cluster: each micro-batch's partials are routed by a stable hash to one of
//! `num_shards` shards, and a [`EngineAction::MergeState`] task merges them into that
//! shard's persistent state on the owning worker (`WORKER_STATE_STORE`), returning
//! only the cells to emit. The driver computes the partials and assembles the
//! output, but never holds the full cross-batch state.
//!
//! The merge itself is a registered, content-agnostic
//! [`StateMergeFn`](atomic_compute::registry::StateMergeFn): the data/compute
//! layers carry no streaming types. The same `dispatch_pipeline` path runs the
//! merge in-process in local mode (shards share the process-global store) and on
//! workers in distributed mode.

use std::sync::Arc;

use atomic_compute::context::Context;
use atomic_data::distributed::{
    EngineAction, StateMergePayload, Step, StepKind, TaskRuntime, decode_payload,
};
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

/// Shared dispatch skeleton for every sharded `Distributed*Engine::process()`:
/// bincode-encode `params` once, wrap each shard's already-routed `bucket` into a
/// `StateMergePayload` (state id = `state_id_base + shard index`), dispatch one
/// `MergeState` task per shard via `merge_fn`, then decode and concatenate each
/// shard's emitted items in shard order.
///
/// Callers own the engine-specific parts: building `Params`, routing this batch's
/// items into per-shard `Bucket`s (a windowed engine buckets one list per shard, a
/// join buckets a `(left, right)` pair — that shape genuinely differs), and turning
/// the flat `Vec<Emitted>` back into output `RecordBatch`es.
pub(crate) fn dispatch_merge_state<Bucket, Params, Emitted>(
    sc: &Context,
    merge_fn: &str,
    state_id_base: u64,
    checkpoint_dir: &Option<String>,
    params: &Params,
    buckets: Vec<Bucket>,
) -> StructuredResult<Vec<Emitted>>
where
    Bucket: bincode::Encode,
    Params: bincode::Encode,
    Emitted: bincode::Decode<()>,
{
    let cfg = bincode::config::standard();
    let params_bytes =
        bincode::encode_to_vec(params, cfg).map_err(|e| StructuredError::Sql(e.to_string()))?;

    let mut source_partitions: Vec<Vec<u8>> = Vec::with_capacity(buckets.len());
    for (shard, bucket) in buckets.into_iter().enumerate() {
        let partials = bincode::encode_to_vec(&bucket, cfg)
            .map_err(|e| StructuredError::Sql(e.to_string()))?;
        let payload = StateMergePayload {
            state_id: state_id_base + shard as u64,
            params: params_bytes.clone(),
            partials,
            checkpoint_dir: checkpoint_dir.clone(),
        };
        source_partitions.push(
            bincode::encode_to_vec(&payload, cfg)
                .map_err(|e| StructuredError::Sql(e.to_string()))?,
        );
    }

    let steps = vec![Step {
        task_name: String::new(),
        kind: StepKind::Engine(EngineAction::MergeState {
            merge_fn: merge_fn.to_string(),
        }),
        runtime: TaskRuntime::Native,
        payload: vec![],
    }];

    let results = sc
        .dispatch_pipeline(source_partitions, steps)
        .map_err(|e| StructuredError::Sql(format!("distributed state merge ({merge_fn}): {e}")))?;

    let mut emitted: Vec<Emitted> = Vec::new();
    for bytes in results {
        let items: Vec<Emitted> = decode_payload(&bytes)
            .map_err(|e| StructuredError::Sql(format!("emitted decode ({merge_fn}): {e}")))?;
        emitted.extend(items);
    }
    Ok(emitted)
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
    let cells: Vec<(StateKey, Vec<AggState>)> =
        decode_payload(partials).map_err(|e| e.to_string())?;
    let touched: Vec<StateKey> = cells.iter().map(|(k, _)| k.clone()).collect();
    for (k, v) in cells {
        store.merge(k, v);
    }

    let params: WindowedMergeParams = decode_payload(params).map_err(|e| e.to_string())?;
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

        let params = WindowedMergeParams {
            watermark_ms: bp.wm_after,
            window_size_ms,
            mode: mode_code(mode),
        };

        // One MergeState task per shard. Empty shards still run so Append eviction
        // and Complete re-emission cover state with no new partials this batch.
        let emitted: Vec<(StateKey, Vec<AggState>)> = dispatch_merge_state(
            &self.sc,
            WINDOWED_MERGE_FN,
            self.state_id_base,
            &self.checkpoint_dir,
            &params,
            by_shard,
        )?;

        self.inner.emit_batch(&emitted)
    }
}
