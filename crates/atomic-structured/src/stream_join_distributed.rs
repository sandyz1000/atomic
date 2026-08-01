//! Distributed stream-stream join state (Part 3 / D4).
//!
//! [`DistributedJoinEngine`] shards [`StreamJoinEngine`]'s two-sided buffer across the
//! cluster instead of holding it driver-local: each batch's rows are routed by a
//! stable join-key hash (so both sides of a match land in the same shard) into
//! worker-resident buffer shards via [`EngineAction::MergeState`](atomic_data::distributed::EngineAction::MergeState)
//! tasks, dispatched through [`dispatch_merge_state`]. Join semantics are identical
//! to the local engine because both call [`probe_and_buffer`].

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;

use atomic_compute::context::Context;
use atomic_data::distributed::{WireEncode, decode_payload};

use crate::distributed_state::{dispatch_merge_state, shard_of};
use crate::errors::StructuredResult;
use crate::query::BatchEngine;
use crate::state::GroupVal;
use crate::stream_join::{JoinRow, JoinStateStore, JoinType, StreamJoinEngine, probe_and_buffer};

/// Registered name of the stream-join state-merge function.
pub(crate) const JOIN_MERGE_FN: &str = "atomic_structured::stream_join_v1";

/// A shard's two-sided buffer state (both join inputs for the keys in the shard).
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct JoinShardState {
    left: JoinStateStore,
    right: JoinStateStore,
}

/// Per-shard merge config for a stream-stream join.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
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
    let new_state = state.encode_wire().map_err(|e| e.to_string())?;
    let matched_bytes = matched.encode_wire().map_err(|e| e.to_string())?;
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

        let params = JoinMergeParams {
            watermark_ms: r.wm,
            time_bound_ms,
            join_type,
            left_ncols,
        };
        let buckets: Vec<(KeyedRows, KeyedRows)> =
            left_by_shard.into_iter().zip(right_by_shard).collect();

        let matched: Vec<(JoinRow, Option<JoinRow>)> = dispatch_merge_state(
            &self.sc,
            JOIN_MERGE_FN,
            self.state_id_base,
            &self.checkpoint_dir,
            &params,
            buckets,
        )?;

        self.inner.build_joined_batch(&matched)
    }
}
