pub mod native_executor;

#[cfg(feature = "python")]
pub mod python_executor;

#[cfg(feature = "js")]
pub mod js_executor;

pub use native_executor::NativeBackend;

use atomic_data::distributed::{PipelineOp, TaskEnvelope, TaskResultEnvelope};
use crate::error::ComputeResult;

/// Full-task execution backend.
///
/// A [`Backend`] takes a complete [`TaskEnvelope`] — which may contain multiple
/// pipeline ops — and returns a [`TaskResultEnvelope`] in all cases.
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
/// Register concrete implementations in `NativeBackend::default()` keyed by
/// [`TaskRuntime`].  Adding a new runtime = one new `impl OpDispatcher` + one
/// `HashMap` entry.  `NativeBackend::execute()` never needs to change.
pub(crate) trait OpDispatcher: Send + Sync {
    /// Execute one pipeline op and return the transformed output bytes.
    ///
    /// `partition_id` is forwarded from the enclosing [`TaskEnvelope`]; most
    /// dispatchers ignore it, but the native shuffle-map path needs it to write
    /// the correct bucket.
    fn dispatch(
        &self,
        op: &PipelineOp,
        partition_id: usize,
        data: &[u8],
    ) -> std::result::Result<Vec<u8>, String>;
}
