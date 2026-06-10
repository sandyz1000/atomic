pub mod app;
pub mod runtimes;
pub mod builtin_tasks;
pub mod shuffle_map;
pub mod context;
pub mod env;
pub mod error;
pub mod executor;
pub mod hosts;
pub mod io;
pub mod rdd;
pub mod task_registry;
pub mod task_traits;
pub mod tls;

pub mod __macro_support {
    pub use crate::task_registry::{ShuffleKeyEntry, ShuffleMapEntry, TaskEntry};
    pub use crate::task_traits::{BinaryTask, UnaryTask};
    pub use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
    pub use inventory;
}

/// Register a shuffle-write handler for the `(K, V)` key-value type pair.
///
/// Place this once in the binary that calls `reduce_by_key` or `group_by_key`
/// on a `TypedRdd<(K, V)>`. Both the driver and worker binary must contain the
/// same call (they are the same binary in Atomic's model, so one call suffices).
///
/// The dispatch key is generated at compile time from the source-level token text
/// of `K` and `V` using `stringify!` (e.g. `"String::u32"`). This is stable across
/// compiler versions, unlike `std::any::type_name`.
///
/// # Example
///
/// ```rust,ignore
/// // In main.rs, before any shuffle operations:
/// atomic_compute::register_shuffle_map!(String, u32);
/// ```
#[macro_export]
macro_rules! register_shuffle_map {
    ($K:ty, $V:ty) => {
        $crate::__macro_support::inventory::submit!(
            $crate::__macro_support::ShuffleMapEntry {
                type_id: || concat!(stringify!($K), "::", stringify!($V)),
                handler: $crate::shuffle_map::shuffle_map_handler::<$K, $V>,
            }
        );
        $crate::__macro_support::inventory::submit!(
            $crate::__macro_support::ShuffleKeyEntry {
                type_id: || std::any::TypeId::of::<($K, $V)>(),
                key: concat!(stringify!($K), "::", stringify!($V)),
            }
        );
    };
}

pub use atomic_runtime_macros::task;
pub use atomic_runtime_macros::task_fn;

pub use env::{Config, WorkerConfig};
