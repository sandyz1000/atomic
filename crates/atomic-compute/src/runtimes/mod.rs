pub mod native;

crate::cfg_python! {
    pub mod py;
}

crate::cfg_js! {
    pub mod js;
}

pub use native::ComputeEngine;

use crate::error::ComputeResult;
use atomic_data::distributed::{Step, TaskEnvelope, TaskResultEnvelope};

/// Full-task execution backend.
///
/// A [`Backend`] takes a complete [`TaskEnvelope`] — which may contain multiple
/// pipeline steps — and returns a [`TaskResultEnvelope`] in all cases.
/// Errors are encoded as `FatalFailure` rather than propagated so the scheduler
/// can handle them uniformly.
///
/// The `Default` supertrait ensures every backend implementation can be
/// instantiated with no configuration, making it usable as a type parameter
/// (e.g. `fn run<B: Backend>(env: &Env) { let b = B::default(); ... }`).
pub trait Backend: Default + Send + Sync {
    fn execute(&self, worker_id: &str, task: &TaskEnvelope) -> ComputeResult<TaskResultEnvelope>;
}

/// Dispatches a single pipeline operation for one specific runtime.
///
/// Register concrete implementations in [`ComputeEngine::default`] keyed by
/// [`TaskRuntime`].  Adding a new runtime = one new `impl Dispatcher` + one
/// `HashMap` entry.  [`ComputeEngine::execute`] never needs to change.
pub(crate) trait Dispatcher: Send + Sync {
    /// Execute one pipeline op and return the transformed output bytes.
    ///
    /// `partition_id` is forwarded from the enclosing [`TaskEnvelope`]; most
    /// dispatchers ignore it, but the native shuffle-map path needs it to write
    /// the correct bucket.
    fn dispatch(&self, op: &Step, partition_id: usize, data: &[u8]) -> ComputeResult<Vec<u8>>;
}
