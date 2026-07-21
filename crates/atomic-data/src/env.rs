//! Process-global runtime environment.
//!
//! Ambient state read from execution paths that carry no `Context` handle — the shuffle-map
//! task (behind a `dyn ShuffleExecutor`), the `ShuffleFetcher`, and worker-side auth. Grouped
//! into one [`Env`] behind a single lock instead of a scatter of loose statics. All access
//! goes through the free functions below; the struct and its lock stay private.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::shuffle::MapOutputTracker;
use crate::shuffle::cache::ShuffleCache;

/// Per-`Context` shuffle infrastructure, set at `init_shuffle` and reset on `Context::drop`
/// (see [`clear_shuffle_infra`]) so the next `Context` starts clean.
struct ShuffleEnv {
    /// Populated by `atomic_compute::env::init_shuffle()` before any jobs are submitted. Both
    /// the ShuffleManager HTTP server and `do_shuffle_task_typed` write/read through this.
    cache: Option<Arc<dyn ShuffleCache>>,
    /// URI of the local ShuffleManager HTTP server (e.g. `http://127.0.0.1:49200`).
    server_uri: Option<String>,
    /// Shared instance used by the scheduler (to register map outputs) and `ShuffledRdd`
    /// (to look up server URIs when fetching).
    tracker: Option<Arc<MapOutputTracker>>,
    /// Reduce-partition count at or above which map-side writes switch from the legacy
    /// per-bucket layout to the consolidated (sort-shuffle) layout. `None` → env-var / 200.
    sort_threshold: Option<usize>,
    /// Client-side TLS connector used by `ShuffleFetcher` when the server URI is `https://`.
    tls_connector: Option<Arc<tokio_rustls::TlsConnector>>,
}

impl ShuffleEnv {
    const fn empty() -> Self {
        Self {
            cache: None,
            server_uri: None,
            tracker: None,
            sort_threshold: None,
            tls_connector: None,
        }
    }
}

/// The process-global runtime environment.
struct Env {
    shuffle: ShuffleEnv,
    /// Cluster auth token. When set, worker TCP connections must open with a matching `Auth`
    /// frame and shuffle/registration HTTP requests must carry `Authorization: Bearer <token>`.
    /// Set on driver and workers from `Config::auth_token` (`ATOMIC_AUTH_TOKEN`); process-
    /// lifetime (not reset per `Context`). `None` disables authentication.
    auth_token: Option<String>,
    /// Base directory for `MemoryAndDisk` / `DiskOnly` partition spill files, set by `Context`
    /// during init. `None` means disk storage levels fall back to `MemoryOnly`.
    rdd_cache_spill_dir: Option<PathBuf>,
}

impl Env {
    const fn empty() -> Self {
        Self {
            shuffle: ShuffleEnv::empty(),
            auth_token: None,
            rdd_cache_spill_dir: None,
        }
    }
}

static ENV: RwLock<Env> = RwLock::new(Env::empty());

// --- Shuffle infrastructure ---

pub fn get_shuffle_cache() -> Option<Arc<dyn ShuffleCache>> {
    ENV.read().unwrap().shuffle.cache.clone()
}

pub fn set_shuffle_cache(v: Arc<dyn ShuffleCache>) {
    ENV.write().unwrap().shuffle.cache = Some(v);
}

pub fn get_shuffle_server_uri() -> Option<String> {
    ENV.read().unwrap().shuffle.server_uri.clone()
}

pub fn set_shuffle_server_uri(uri: String) {
    ENV.write().unwrap().shuffle.server_uri = Some(uri);
}

pub fn get_map_output_tracker() -> Option<Arc<MapOutputTracker>> {
    ENV.read().unwrap().shuffle.tracker.clone()
}

pub fn set_map_output_tracker(v: Arc<MapOutputTracker>) {
    ENV.write().unwrap().shuffle.tracker = Some(v);
}

pub fn set_sort_shuffle_threshold(n: usize) {
    ENV.write().unwrap().shuffle.sort_threshold = Some(n);
}

/// Programmatic threshold if set, else the `ATOMIC_SORT_SHUFFLE_THRESHOLD` env var, else 200
/// (the bypass-merge threshold below which the per-bucket layout is kept).
pub fn sort_shuffle_threshold() -> usize {
    if let Some(n) = ENV.read().unwrap().shuffle.sort_threshold {
        return n;
    }
    std::env::var("ATOMIC_SORT_SHUFFLE_THRESHOLD")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(200)
}

pub fn get_shuffle_tls_connector() -> Option<Arc<tokio_rustls::TlsConnector>> {
    ENV.read().unwrap().shuffle.tls_connector.clone()
}

pub fn set_shuffle_tls_connector(c: Arc<tokio_rustls::TlsConnector>) {
    ENV.write().unwrap().shuffle.tls_connector = Some(c);
}

/// Reset the shuffle infrastructure. Called from `Context::drop()` so the next `Context`
/// starts with a clean slate. Process-lifetime state (auth token, spill dir) is untouched.
pub fn clear_shuffle_infra() {
    ENV.write().unwrap().shuffle = ShuffleEnv::empty();
}

// --- Auth ---

pub fn set_auth_token(token: String) {
    ENV.write().unwrap().auth_token = Some(token);
}

pub fn get_auth_token() -> Option<String> {
    ENV.read().unwrap().auth_token.clone()
}

/// Constant-time check of `candidate` against the configured token.
/// Returns `true` when no token is configured (auth disabled).
pub fn auth_token_matches(candidate: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    match &ENV.read().unwrap().auth_token {
        Some(token) => token.as_bytes().ct_eq(candidate).into(),
        None => true,
    }
}

/// Validate an HTTP `Authorization` header value (`"Bearer <token>"`) against the
/// configured token. Returns `true` when no token is configured (auth disabled).
pub fn bearer_authorized(auth_header: Option<&str>) -> bool {
    if get_auth_token().is_none() {
        return true;
    }
    auth_header
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| auth_token_matches(token.as_bytes()))
}

// --- RDD cache spill ---

pub fn set_rdd_cache_spill_dir(dir: PathBuf) {
    ENV.write().unwrap().rdd_cache_spill_dir = Some(dir);
}

pub fn get_rdd_cache_spill_dir() -> Option<PathBuf> {
    ENV.read().unwrap().rdd_cache_spill_dir.clone()
}
