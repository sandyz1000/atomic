use std::sync::{Arc, RwLock};

use crate::shuffle::cache::ShuffleCache;
use crate::shuffle::MapOutputTracker;

/// Global shuffle cache — populated by `atomic_compute::env::init_shuffle()` before any
/// jobs are submitted. Both the ShuffleManager HTTP server and `do_shuffle_task_typed`
/// write/read through this reference.
pub static SHUFFLE_CACHE: RwLock<Option<Arc<dyn ShuffleCache>>> = RwLock::new(None);

/// URI of the local ShuffleManager HTTP server (e.g. `http://127.0.0.1:49200`).
/// Resettable so a new Context can start a fresh ShuffleManager after the previous
/// one is dropped.
pub static SHUFFLE_SERVER_URI: RwLock<Option<String>> = RwLock::new(None);

/// Shared MapOutputTracker instance used by both the scheduler (to register map outputs)
/// and ShuffledRdd (to look up server URIs when fetching shuffle data).
pub static MAP_OUTPUT_TRACKER: RwLock<Option<Arc<MapOutputTracker>>> = RwLock::new(None);

// ── Accessors ──────────────────────────────────────────────────────────────────

pub fn get_shuffle_cache() -> Option<Arc<dyn ShuffleCache>> {
    SHUFFLE_CACHE.read().unwrap().clone()
}

pub fn get_shuffle_server_uri() -> Option<String> {
    SHUFFLE_SERVER_URI.read().unwrap().clone()
}

pub fn get_map_output_tracker() -> Option<Arc<MapOutputTracker>> {
    MAP_OUTPUT_TRACKER.read().unwrap().clone()
}

// ── Setters ────────────────────────────────────────────────────────────────────

pub fn set_shuffle_cache(v: Arc<dyn ShuffleCache>) {
    *SHUFFLE_CACHE.write().unwrap() = Some(v);
}

pub fn set_shuffle_server_uri(uri: String) {
    *SHUFFLE_SERVER_URI.write().unwrap() = Some(uri);
}

pub fn set_map_output_tracker(v: Arc<MapOutputTracker>) {
    *MAP_OUTPUT_TRACKER.write().unwrap() = Some(v);
}

// ── Teardown ───────────────────────────────────────────────────────────────────

/// Clear all shuffle infrastructure state.
/// Called from `Context::drop()` so the next Context starts with a clean slate.
pub fn clear_shuffle_infrastructure() {
    *SHUFFLE_SERVER_URI.write().unwrap() = None;
    *SHUFFLE_CACHE.write().unwrap() = None;
    *MAP_OUTPUT_TRACKER.write().unwrap() = None;
}
