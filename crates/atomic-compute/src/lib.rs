pub mod app;
pub mod context;
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

pub mod __macro_support {
    pub use crate::task_registry::TaskEntry;
    pub use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
    pub use inventory;
}

pub use atomic_runtime_macros::task;
