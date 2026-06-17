use std::net::SocketAddrV4;

use atomic_data::distributed::{PipelineOp, TaskAction};

use super::DistributedScheduler;

/// Outcome of [`DistributedScheduler::plan_cache_dispatch`]: either recompute the
/// staged pipeline, or serve a cached RDD and run the ops after the cache boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheDispatch {
    /// Run the pipeline as given (computes + caches when it ends in a `Cache` op).
    Recompute,
    /// Read partitions of `rdd_id` from cache (pinned to `locs[partition]`) and run
    /// `post_ops` on top.
    Serve {
        rdd_id: usize,
        post_ops: Vec<PipelineOp>,
        locs: Vec<SocketAddrV4>,
    },
}

impl DistributedScheduler {
    /// Decide whether a staged pipeline can be served from cache. If its terminal
    /// `Cache { rdd_id }` op's partitions are all present in `cache_locs`, return a
    /// `Serve` plan (read from cache + run the ops after the cache boundary, each
    /// pinned to a holding worker); otherwise `Recompute` the full pipeline.
    pub fn plan_cache_dispatch(&self, ops: &[PipelineOp], num_partitions: usize) -> CacheDispatch {
        let Some(idx) = ops
            .iter()
            .rposition(|o| matches!(o.action, TaskAction::Cache { .. }))
        else {
            return CacheDispatch::Recompute;
        };
        let TaskAction::Cache { rdd_id } = ops[idx].action else {
            return CacheDispatch::Recompute;
        };
        let Some(entry) = self.cache_endpoints.get(&rdd_id) else {
            return CacheDispatch::Recompute;
        };
        // Every partition must have at least one holding worker.
        let locs: Vec<SocketAddrV4> = (0..num_partitions)
            .map(|p| entry.get(p).and_then(|v| v.first().copied()))
            .collect::<Option<Vec<_>>>()
            .map_or(Vec::new(), |v| v);
        if locs.len() != num_partitions {
            return CacheDispatch::Recompute;
        }
        CacheDispatch::Serve {
            rdd_id,
            post_ops: ops[idx + 1..].to_vec(),
            locs,
        }
    }

    /// Record that `endpoint` now holds the given `(rdd_id, partition)` cached
    /// partitions, so `plan_cache_dispatch` can pin later reads to it.
    pub fn register_cache_locs(&self, cached: &[(usize, usize)], endpoint: SocketAddrV4) {
        for &(rdd_id, part) in cached {
            let mut entry = self.cache_endpoints.entry(rdd_id).or_default();
            if entry.len() <= part {
                entry.resize(part + 1, Vec::new());
            }
            if !entry[part].contains(&endpoint) {
                entry[part].push(endpoint);
            }
        }
    }

    /// Forget every cache location held by `endpoint` — the worker is gone, so its
    /// cached partitions no longer exist. Affected partitions lose their holder, so
    /// `plan_cache_dispatch` falls back to `Recompute` for them.
    pub(crate) fn invalidate_cache_for_worker(&self, endpoint: SocketAddrV4) {
        for mut entry in self.cache_endpoints.iter_mut() {
            for locs in entry.value_mut().iter_mut() {
                locs.retain(|a| *a != endpoint);
            }
        }
    }

    /// Forget all cache locations for `rdd_id` (used by `unpersist`); the next job
    /// over it recomputes.
    pub fn invalidate_rdd_cache(&self, rdd_id: usize) {
        self.cache_endpoints.remove(&rdd_id);
    }

    /// Record that `endpoint` now holds the given state shards, so subsequent
    /// `MergeState` batches route each shard back to the worker that holds its state.
    pub fn register_state_locs(&self, state_ids: &[u64], endpoint: SocketAddrV4) {
        for &id in state_ids {
            self.state_locs.insert(id, endpoint);
        }
    }

    /// Forget every state shard held by `endpoint` — the worker is gone (or its
    /// in-memory state is lost). Affected shards fall back to `pin_state_shard` (modulo)
    /// and reload from their checkpoint on the next batch.
    pub(crate) fn invalidate_state_for_worker(&self, endpoint: SocketAddrV4) {
        self.state_locs.retain(|_, v| *v != endpoint);
    }
}
