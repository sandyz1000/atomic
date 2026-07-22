//! Shared data types for the Atomic distributed compute engine.
//!
//! This crate defines the core abstractions used by every layer of Atomic

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
macro_rules! cfg_k8s {
    ($($item:item)*) => { $(#[cfg(feature = "k8s")] $item)* };
}

#[macro_export]
macro_rules! cfg_not_k8s {
    ($($item:item)*) => { $(#[cfg(not(feature = "k8s"))] $item)* };
}
