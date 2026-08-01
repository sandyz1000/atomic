//! Distributed session-window state (Part 3 / D4).
//!
//! [`DistributedSessionEngine`] shards [`SessionEngine`]'s per-group `SessionStore`
//! across the cluster: each batch's events are routed by a stable group-key hash into
//! worker-resident store shards via [`EngineAction::MergeState`](atomic_data::distributed::EngineAction::MergeState)
//! tasks, dispatched through [`dispatch_merge_state`].

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;

use atomic_compute::context::Context;
use atomic_data::distributed::{WireEncode, decode_payload};

use crate::OutputMode;
use crate::distributed_state::{MODE_APPEND, dispatch_merge_state, mode_code, shard_of};
use crate::errors::{StructuredError, StructuredResult};
use crate::query::BatchEngine;
use crate::session_window::{Session, SessionEngine, SessionEvent, SessionStore};
use crate::state::GroupVal;

/// Registered name of the session state-merge function.
pub(crate) const SESSION_MERGE_FN: &str = "atomic_structured::session_v1";

/// Per-shard merge/emit config for session windows.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
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
    let mut store = match prev {
        Some(b) => decode_payload::<SessionStore>(b).map_err(|e| e.to_string())?,
        None => SessionStore::new(),
    };
    let events: Vec<SessionEvent> =
        decode_payload(events).map_err(|e| e.to_string())?;
    let params: SessionMergeParams =
        decode_payload(params).map_err(|e| e.to_string())?;
    let gap = params.gap_ms as i64;
    for (group, t, partial) in events {
        store.absorb(group, t, partial, gap);
    }
    let emitted: Vec<(Vec<GroupVal>, Session)> = match params.mode {
        MODE_APPEND => match params.watermark_ms {
            Some(w) => store.drain_final(gap, w as i64),
            None => vec![],
        },
        _ => store.all_sessions(),
    };
    let new_state = store.encode_wire().map_err(|e| e.to_string())?;
    let emitted_bytes = emitted.encode_wire().map_err(|e| e.to_string())?;
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

        let params = SessionMergeParams {
            watermark_ms: sb.wm_after,
            gap_ms,
            mode: mode_code(mode),
        };

        let emitted: Vec<(Vec<GroupVal>, Session)> = dispatch_merge_state(
            &self.sc,
            SESSION_MERGE_FN,
            self.state_id_base,
            &self.checkpoint_dir,
            &params,
            by_shard,
        )?;

        self.inner.emit_batch(&emitted)
    }
}
