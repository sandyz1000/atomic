use once_cell::sync::OnceCell;
use std::sync::Arc;

use crate::shuffle::cache::ShuffleCache;
use crate::shuffle::MapOutputTracker;

/// Global shuffle cache — populated by `atomic_compute::env::init_shuffle()` before any
/// jobs are submitted. Both the ShuffleManager HTTP server and `do_shuffle_task_typed`
/// write/read through this reference.
pub static SHUFFLE_CACHE: OnceCell<Arc<dyn ShuffleCache>> = OnceCell::new();

/// URI of the local ShuffleManager HTTP server (e.g. `http://127.0.0.1:49200`).
/// Returned by `do_shuffle_task_typed` so the scheduler can register map output locations
/// with the MapOutputTracker.
pub static SHUFFLE_SERVER_URI: OnceCell<String> = OnceCell::new();

/// Shared MapOutputTracker instance used by both the scheduler (to register map outputs)
/// and ShuffledRdd (to look up server URIs when fetching shuffle data).
pub static MAP_OUTPUT_TRACKER: OnceCell<Arc<MapOutputTracker>> = OnceCell::new();
