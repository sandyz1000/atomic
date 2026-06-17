//! Worker-resident, content-agnostic state store for distributed stateful
//! streaming.
//!
//! Keyed by an opaque `state_id` (a query-shard identifier); the value is the
//! serialized state bytes for that shard, persisted across micro-batches. The
//! merge semantics live in the registered state-merge function
//! (`atomic_compute::task_registry::STATE_MERGE_REGISTRY`) — this store only holds
//! the bytes, so it carries no streaming-layer types and stays at the data layer.
//!
//! In local mode the process-global store is shared by the in-process worker
//! threads; in distributed mode each worker holds the shards routed to it.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Process-global per-shard serialized-state store.
#[derive(Default)]
pub struct WorkerStateStore {
    inner: Mutex<HashMap<u64, Vec<u8>>>,
}

impl WorkerStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Current serialized state for `state_id`, if any.
    pub fn get(&self, state_id: u64) -> Option<Vec<u8>> {
        self.inner.lock().unwrap().get(&state_id).cloned()
    }

    /// Replace the serialized state for `state_id`.
    pub fn put(&self, state_id: u64, bytes: Vec<u8>) {
        self.inner.lock().unwrap().insert(state_id, bytes);
    }

    /// Drop a shard's state.
    pub fn remove(&self, state_id: u64) {
        self.inner.lock().unwrap().remove(&state_id);
    }

    /// Snapshot every `(state_id, bytes)` pair — used to checkpoint all shards.
    pub fn snapshot(&self) -> Vec<(u64, Vec<u8>)> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// Drop every shard (used by tests and full query reset).
    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }

    /// Number of resident shards (tests/metrics).
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Global worker-side state store for distributed stateful streaming.
pub static WORKER_STATE_STORE: OnceLock<WorkerStateStore> = OnceLock::new();

/// Accessor that lazily initialises the worker state store on first use.
pub fn worker_state_store() -> &'static WorkerStateStore {
    WORKER_STATE_STORE.get_or_init(WorkerStateStore::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_roundtrip() {
        let s = WorkerStateStore::new();
        assert_eq!(s.get(1), None);
        s.put(1, vec![1, 2, 3]);
        assert_eq!(s.get(1), Some(vec![1, 2, 3]));
        s.put(1, vec![4]);
        assert_eq!(s.get(1), Some(vec![4]));
    }

    #[test]
    fn snapshot_lists_shards() {
        let s = WorkerStateStore::new();
        s.put(10, vec![0]);
        s.put(20, vec![1]);
        let mut snap = s.snapshot();
        snap.sort();
        assert_eq!(snap, vec![(10, vec![0]), (20, vec![1])]);
    }
}
