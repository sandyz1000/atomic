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
