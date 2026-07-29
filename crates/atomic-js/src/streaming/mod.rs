//! Streaming bindings for Node.js — Phase 4.4
//!
//! Design: JS callbacks are stored as `FunctionRef<Args, Return>` — a
//! persistent napi reference (`napi_create_reference`) that survives scope
//! boundaries.  `FunctionRef::borrow_back(&env)` produces a fresh, scope-valid
//! `Function` handle each time we need to call the stored callback.  This
//! avoids the prior `mem::transmute`-to-`'static` approach whose scope-bound
//! handle became invalid after the registration method returned.
//!
//! Split by responsibility: [`dstream`] is the `DStream` builder API and its internal
//! transform-tree representation; [`engine`] interprets that tree to materialize one
//! batch; [`batch_queue`] is the trivial test-source queue; [`context`] is the
//! top-level orchestrator (`StreamingContext`) that owns output ops and the batch loop.

mod batch_queue;
mod context;
mod dstream;
mod engine;

pub use batch_queue::JsBatchQueue;
pub use context::JsStreamingContext;
pub use dstream::JsDStream;
