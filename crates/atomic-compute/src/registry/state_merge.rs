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

use super::declare_registry;

/// A registered state-merge function for distributed stateful streaming.
///
/// Content-agnostic, so the data/compute layers carry no streaming types: it
/// operates on opaque serialized state. The worker calls it for a
/// [`StepKind::MergeState`](atomic_data::distributed::StepKind::MergeState):
/// `prev` is the shard's current serialized state (`None` if this is the first
/// merge), `partials` is this batch's partial state for the shard, and `params` is
/// the merge/emit configuration. It returns `(new_state_bytes, emitted_bytes)`.
///
/// Same `String`-error rationale as [`ShuffleMapHandlerFn`](super::ShuffleMapHandlerFn):
/// this is a `fn`-pointer ABI shared across binaries, converted to `ComputeError` at
/// the dispatch boundary.
pub type StateMergeFn =
    fn(prev: Option<&[u8]>, partials: &[u8], params: &[u8]) -> Result<(Vec<u8>, Vec<u8>), String>;

declare_registry!(
    /// A compile-time state-merge handler, registered by `register_state_merge!`.
    StateMergeEntry {
        /// Stable name used as the dispatch key (e.g. `"atomic_structured::windowed_v1"`).
        name: &'static str,
        /// The merge handler.
        handler: StateMergeFn,
    },
    /// Global compile-time state-merge registry — built once from all
    /// `register_state_merge!` calls linked into the binary. `NativeBackend` uses this
    /// when it sees [`StepKind::MergeState`](atomic_data::distributed::StepKind::MergeState).
    STATE_MERGE_REGISTRY: &'static str => StateMergeFn,

    |entry: &StateMergeEntry| (entry.name, entry.handler)
);
