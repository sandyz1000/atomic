//! Shared data types for the Atomic distributed compute engine.
//!
//! This crate defines the core abstractions used by every layer of Atomic:
//!
//! - [`rdd`] — [`RddBase`](rdd::RddBase) and [`Rdd`](rdd::Rdd) traits; the fundamental compute primitive.
//! - [`distributed`] — wire types: [`TaskEnvelope`](distributed::TaskEnvelope),
//!   [`TaskResultEnvelope`](distributed::TaskResultEnvelope), [`WorkerCapabilities`](distributed::WorkerCapabilities).
//! - [`dependency`] — [`Dependency`](dependency::Dependency) (narrow vs. shuffle) and [`ShuffleDependency`](dependency::ShuffleDependency).
//! - [`partitioner`] — [`Partitioner`](partitioner::Partitioner), [`HashPartitioner`](partitioner::HashPartitioner), [`RangePartitioner`](partitioner::RangePartitioner).
//! - [`broadcast`] — [`BroadcastVar`](broadcast::BroadcastVar) for driver-to-worker read-only data.
//! - [`accumulator`] — distributed accumulators (sum, max, etc.).
//! - [`state_store`] — per-key state for stateful streaming.

pub mod accumulator;
pub mod aggregator;
pub mod broadcast;
pub mod cache;
pub mod data;
pub mod dependency;
pub mod distributed;
pub mod env;
pub mod error;
pub mod fn_traits;
pub mod hosts;
pub mod partial;
pub mod partitioner;
pub mod rdd;
pub mod shuffle;
pub mod split;
pub mod state_store;
pub mod task;
pub mod task_context;

pub use task::TaskResult;
pub use task_context::TaskContext;

// Feature-gate macros — one definition per feature flag, shared by every crate in the
// workspace that needs it. Centralized here (rather than duplicated per-crate) because
// atomic-data is the one crate everyone else already depends on. `#[cfg(...)]` resolves
// against whichever crate a macro expands *in*, not the crate that defines it, so a
// single definition here works correctly for callers in atomic-compute, atomic-streaming,
// atomic-structured, etc. — each crate just needs the exact matching feature name in its
// own Cargo.toml. The JS runtime feature is named `"js"` everywhere, including this
// crate's own Cargo.toml, so `cfg_js!` covers every call site.
//
// One shape only: `cfg_x! { item item ... }` for one or more `fn`/`mod`/`struct`/
// `impl`/... items. There is deliberately no statement-position variant — a function
// body that needs a feature-gated `if`/`let`/block extracts that logic into a small
// private helper fn (wrapped with this same item macro) instead, e.g.
// `atomic-compute/src/context/io.rs`'s `s3_sources`. That keeps every call
// site using the one macro shape, and sidesteps a real ambiguity: a bare `{ ... }` is
// parseable as either "block statement" or "item list", so a merged item/statement
// macro's arm-matching order becomes a footgun (confirmed by trying it — whichever arm
// is tried first silently wins even when it's wrong for the input's intent).
//
// Struct fields, struct-literal field initializers, enum variants, and match arms have
// no macro form at all in stable Rust (`rustc` itself: "macros cannot expand to struct
// fields" / "... to enum variants"; match-arm position is a hard parse error) — those
// four positions keep the raw `#[cfg(...)]` attribute. See the `atomic-rust-standards`
// skill for the full convention and the compiling counter-examples behind this comment.
#[macro_export]
macro_rules! cfg_tls {
    ($($item:item)*) => { $(#[cfg(feature = "tls")] $item)* };
}

#[macro_export]
macro_rules! cfg_not_tls {
    ($($item:item)*) => { $(#[cfg(not(feature = "tls"))] $item)* };
}

#[macro_export]
macro_rules! cfg_kafka {
    ($($item:item)*) => { $(#[cfg(feature = "kafka")] $item)* };
}

#[macro_export]
macro_rules! cfg_not_kafka {
    ($($item:item)*) => { $(#[cfg(not(feature = "kafka"))] $item)* };
}

#[macro_export]
macro_rules! cfg_python {
    ($($item:item)*) => { $(#[cfg(feature = "python")] $item)* };
}

#[macro_export]
macro_rules! cfg_not_python {
    ($($item:item)*) => { $(#[cfg(not(feature = "python"))] $item)* };
}

/// Gate on the `"js"` feature (embedded V8/deno_core runtime).
#[macro_export]
macro_rules! cfg_js {
    ($($item:item)*) => { $(#[cfg(feature = "js")] $item)* };
}

/// Negative counterpart of [`cfg_js`] — see its doc comment.
#[macro_export]
macro_rules! cfg_not_js {
    ($($item:item)*) => { $(#[cfg(not(feature = "js"))] $item)* };
}

#[macro_export]
macro_rules! cfg_s3 {
    ($($item:item)*) => { $(#[cfg(feature = "s3")] $item)* };
}

#[macro_export]
macro_rules! cfg_not_s3 {
    ($($item:item)*) => { $(#[cfg(not(feature = "s3"))] $item)* };
}

#[macro_export]
macro_rules! cfg_k8s {
    ($($item:item)*) => { $(#[cfg(feature = "k8s")] $item)* };
}

#[macro_export]
macro_rules! cfg_not_k8s {
    ($($item:item)*) => { $(#[cfg(not(feature = "k8s"))] $item)* };
}
