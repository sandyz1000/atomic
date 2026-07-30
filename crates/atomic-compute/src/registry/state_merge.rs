//! Distributed stateful-streaming merge registry.
//!
//! `atomic-structured` is the only consumer: its windowed/session/join engines each
//! register one content-agnostic merge function under a stable name, so the
//! data/compute layers here carry no streaming types — this file is the one place
//! that cross-crate dependency is explicit.
//!
//! ```rust,ignore
//! atomic_compute::register_state_merge!("atomic_structured::windowed_v1", windowed_state_merge);
//! ```

use once_cell::sync::Lazy;
use std::collections::HashMap;

/// Same `String`-error rationale as [`TaskHandlerFn`](super::TaskHandlerFn): the merge
/// function itself is written by whichever downstream crate registers it (currently
/// `atomic-structured`), not hand-written inside `atomic-compute`, so this ABI can't assume
/// a shared internal error type is in scope at every call site. Converted to `ComputeError`
/// at the dispatch boundary (`NativeDispatcher::dispatch`).
pub type StateMergeFn =
    fn(prev: Option<&[u8]>, partials: &[u8], params: &[u8]) -> Result<(Vec<u8>, Vec<u8>), String>;

/// A registered state-merge function for distributed stateful streaming.
///
/// Content-agnostic, so the data/compute layers carry no streaming types: it
/// operates on opaque serialized state. The worker calls it for a
/// [`StepKind::MergeState`](atomic_data::distributed::StepKind::MergeState):
/// `prev` is the shard's current serialized state (`None` if this is the first
/// merge), `partials` is this batch's partial state for the shard, and `params` is
/// the merge/emit configuration. It returns `(new_state_bytes, emitted_bytes)`.
pub struct StateMergeEntry {
    /// Stable name used as the dispatch key (e.g. `"atomic_structured::windowed_v1"`).
    pub name: &'static str,
    /// The merge handler.
    pub handler: StateMergeFn,
}

impl StateMergeEntry {
    pub fn call(
        &self,
        prev: Option<&[u8]>,
        partials: &[u8],
        params: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), String> {
        (self.handler)(prev, partials, params)
    }
}

inventory::collect!(StateMergeEntry);

/// Global compile-time state-merge registry — built once from all
/// `register_state_merge!` calls linked into the binary. `NativeBackend` uses this
/// when it sees [`StepKind::MergeState`](atomic_data::distributed::StepKind::MergeState).
pub static STATE_MERGE_REGISTRY: Lazy<HashMap<&'static str, &'static StateMergeEntry>> =
    Lazy::new(|| {
        inventory::iter::<StateMergeEntry>
            .into_iter()
            .map(|entry| (entry.name, entry))
            .collect()
    });
