pub mod native;
pub mod udf;

pub use native::NativeBackend;

use atomic_data::distributed::{TaskEnvelope, TaskResultEnvelope};
use crate::error::LibResult;

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
    fn execute(&self, worker_id: &str, task: &TaskEnvelope) -> LibResult<TaskResultEnvelope>;
}
