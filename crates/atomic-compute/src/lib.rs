pub mod app;
pub mod builtin_tasks;
pub mod context;
pub mod udf_backend;
pub mod env;
pub mod error;
pub mod executor;
pub mod fs;
pub mod grpc;
pub mod hosts;
pub mod io;
pub mod native_backend;
pub mod rdd;
pub mod task_registry;
pub mod task_traits;

pub mod __macro_support {
    pub use crate::task_registry::TaskEntry;
    pub use crate::task_traits::{BinaryTask, UnaryTask};
    pub use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
    pub use inventory;
}

pub use atomic_runtime_macros::task;
pub use atomic_runtime_macros::task_fn;
