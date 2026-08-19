pub mod native;

crate::cfg_python! {
    pub mod py;
}

crate::cfg_js! {
    pub mod js;
}

pub use native::ComputeEngine;

use crate::error::ComputeResult;
use atomic_data::distributed::Step;

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
