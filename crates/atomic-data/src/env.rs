use std::sync::{Arc, RwLock};

use crate::shuffle::MapOutputTracker;
use crate::shuffle::cache::ShuffleCache;

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

pub fn get_shuffle_cache() -> Option<Arc<dyn ShuffleCache>> {
    SHUFFLE_CACHE.read().unwrap().clone()
}

pub fn get_shuffle_server_uri() -> Option<String> {
    SHUFFLE_SERVER_URI.read().unwrap().clone()
}

pub fn get_map_output_tracker() -> Option<Arc<MapOutputTracker>> {
    MAP_OUTPUT_TRACKER.read().unwrap().clone()
}

pub fn set_shuffle_cache(v: Arc<dyn ShuffleCache>) {
    *SHUFFLE_CACHE.write().unwrap() = Some(v);
}

pub fn set_shuffle_server_uri(uri: String) {
    *SHUFFLE_SERVER_URI.write().unwrap() = Some(uri);
}

pub fn set_map_output_tracker(v: Arc<MapOutputTracker>) {
    *MAP_OUTPUT_TRACKER.write().unwrap() = Some(v);
}

/// Reduce-partition count at or above which map-side shuffle writes switch from the legacy
/// per-bucket layout to the consolidated (sort-shuffle) layout: one DATA blob + one offset
/// INDEX per map task instead of `R` entries. Set from `ShuffleConfig` during init; falls
/// back to the `ATOMIC_SORT_SHUFFLE_THRESHOLD` env var, otherwise 200 (the bypass-merge
/// threshold below which the per-bucket layout is kept).
pub static SORT_SHUFFLE_THRESHOLD: RwLock<Option<usize>> = RwLock::new(None);

pub fn set_sort_shuffle_threshold(n: usize) {
    *SORT_SHUFFLE_THRESHOLD.write().unwrap() = Some(n);
}

pub fn sort_shuffle_threshold() -> usize {
    if let Some(n) = *SORT_SHUFFLE_THRESHOLD.read().unwrap() {
        return n;
    }
    std::env::var("ATOMIC_SORT_SHUFFLE_THRESHOLD")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(200)
}

/// Base directory for `MemoryAndDisk` / `DiskOnly` partition spill files.
/// Set by `Context` during init (`rdd-cache/` under the job work_dir).
/// `None` means disk storage levels fall back to `MemoryOnly`.
pub static RDD_CACHE_SPILL_DIR: RwLock<Option<std::path::PathBuf>> = RwLock::new(None);

pub fn set_rdd_cache_spill_dir(dir: std::path::PathBuf) {
    *RDD_CACHE_SPILL_DIR.write().unwrap() = Some(dir);
}

pub fn get_rdd_cache_spill_dir() -> Option<std::path::PathBuf> {
    RDD_CACHE_SPILL_DIR.read().unwrap().clone()
}

/// Clear all shuffle infrastructure state.
/// Called from `Context::drop()` so the next Context starts with a clean slate.
pub fn clear_shuffle_infra() {
    *SHUFFLE_SERVER_URI.write().unwrap() = None;
    *SHUFFLE_CACHE.write().unwrap() = None;
    *MAP_OUTPUT_TRACKER.write().unwrap() = None;
    *SORT_SHUFFLE_THRESHOLD.write().unwrap() = None;
    clear_tls_connector();
}

crate::cfg_tls! {
    fn clear_tls_connector() {
        *SHUFFLE_TLS_CONNECTOR.write().unwrap() = None;
    }
}
crate::cfg_not_tls! {
    fn clear_tls_connector() {}
}

crate::cfg_tls! {
    /// Global TLS connector used by `ShuffleFetcher` when the shuffle server URI is `https://`.
    /// Set by `atomic_compute::env::init_shuffle()` when TLS is configured.
    pub static SHUFFLE_TLS_CONNECTOR: RwLock<Option<std::sync::Arc<tokio_rustls::TlsConnector>>> =
        RwLock::new(None);

    pub fn set_shuffle_tls_connector(c: std::sync::Arc<tokio_rustls::TlsConnector>) {
        *SHUFFLE_TLS_CONNECTOR.write().unwrap() = Some(c);
    }

    pub fn get_shuffle_tls_connector() -> Option<std::sync::Arc<tokio_rustls::TlsConnector>> {
        SHUFFLE_TLS_CONNECTOR.read().unwrap().clone()
    }
}
