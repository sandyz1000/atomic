use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use once_cell::sync::Lazy;

use crate::distributed::WireDecode;

static NEXT_BROADCAST_ID: AtomicUsize = AtomicUsize::new(1);

pub fn next_broadcast_id() -> usize {
    NEXT_BROADCAST_ID.fetch_add(1, Ordering::SeqCst)
}

/// Process-global broadcast cache, keyed by broadcast id. Populated once per worker
/// process — the driver sends a broadcast's bytes only on the first task that reaches
/// a given worker, and every later task on that worker reads the value from here. This
/// is the worker side of the send-once-per-worker model: bytes cross the wire once,
/// then live for the process lifetime (until [`evict_broadcast`]).
static BROADCAST_CACHE: Lazy<DashMap<usize, Arc<Vec<u8>>>> = Lazy::new(DashMap::new);

/// Cache broadcast bytes the worker does not already hold. Idempotent.
pub fn cache_broadcast_values(values: &[(usize, Vec<u8>)]) {
    for (id, bytes) in values {
        BROADCAST_CACHE
            .entry(*id)
            .or_insert_with(|| Arc::new(bytes.clone()));
    }
}

/// Stable substring of [`BroadcastError`]'s message; the driver matches it to detect a
/// worker that lost its cache and re-send the bytes.
pub const BROADCAST_CACHE_MISS: &str = "broadcast cache miss";

/// A broadcast a task needs is absent from this worker's cache — typically the worker
/// restarted after the driver believed it had sent the bytes.
#[derive(Debug, thiserror::Error)]
#[error("{BROADCAST_CACHE_MISS}: id {0}")]
pub struct BroadcastError(pub usize);

/// Confirm every id a task needs is cached before it runs.
pub fn ensure_broadcasts_cached(ids: &[usize]) -> Result<(), BroadcastError> {
    for id in ids {
        if !BROADCAST_CACHE.contains_key(id) {
            return Err(BroadcastError(*id));
        }
    }
    Ok(())
}

pub fn cached_broadcast(id: usize) -> Option<Arc<Vec<u8>>> {
    BROADCAST_CACHE.get(&id).map(|e| Arc::clone(e.value()))
}

/// Drop a broadcast from the worker cache when it is unpersisted.
pub fn evict_broadcast(id: usize) {
    BROADCAST_CACHE.remove(&id);
}

/// A handle to a broadcast value, created on the driver by `Context::broadcast(value)`
/// and read on workers via [`value`](Self::value). It carries only the id; the bytes
/// live in each worker's process-global broadcast cache.
///
/// `T` must implement `WireEncode` (driver) and `WireDecode` (worker) — both blanket
/// implementations for types satisfying the rkyv bounds.
#[derive(Debug, Clone)]
pub struct BroadcastVar<T> {
    pub id: usize,
    _phantom: PhantomData<T>,
}

impl<T> BroadcastVar<T>
where
    T: WireDecode,
{
    pub fn new(id: usize) -> Self {
        BroadcastVar {
            id,
            _phantom: PhantomData,
        }
    }

    /// Read the value from this worker's cache. Panics if the id was never cached —
    /// [`ensure_broadcasts_cached`] guards every task first, so reaching this panic
    /// means the id was missing from the envelope's `broadcast_ids`.
    pub fn value(&self) -> T {
        let bytes = cached_broadcast(self.id)
            .unwrap_or_else(|| panic!("BroadcastVar {}: not cached on this worker", self.id));
        T::decode_wire(&bytes).expect("BroadcastVar: decode failed")
    }
}
