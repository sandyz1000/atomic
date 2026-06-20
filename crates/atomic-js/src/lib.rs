#![deny(clippy::all)]

mod context;
mod distributed_vars;
mod graph;
mod rdd;
mod sql;
mod streaming;

pub use context::JsContext;
pub use distributed_vars::{Accumulator, BroadcastVar};
pub use graph::JsGraph;
pub use rdd::JsRdd;
pub use sql::{JsDataFrame, JsSqlContext};
pub use streaming::{JsBatchQueue, JsDStream, JsStreamingContext};

/// Registers the framework-native agent runner once when the native module loads,
/// so `JsRdd::agent_step` works without a separate explicit init call from JS.
#[napi_derive::module_init]
fn init() {
    atomic_nlq::agent_runner::register();
}
