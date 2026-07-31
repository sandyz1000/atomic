//! Execution runtime for the Atomic distributed compute engine.
//!
//! # Entry points
//!
//! Use [`app::AtomicApp`] to build a driver or worker from CLI flags:
//!
//! ```rust,ignore
//! let app = AtomicApp::build().await?;
//! let ctx = app.driver_context()?;
//! ```
//!
//! For programmatic setup, construct a [`context::Context`] directly:
//!
//! ```rust,ignore
//! use atomic_compute::{context::Context, env::Config};
//!
//! let ctx = Context::new_with_config(Config::local())?;
//! let rdd = ctx.parallelize_typed(vec![1i32, 2, 3], 2);
//! let result = rdd.map_task(MyTask).collect()?;
//! ```
//!
//! # Task registration
//!
//! Distributed work must be registered at compile time:
//!
//! ```rust,ignore
//! use atomic_compute::task;
//!
//! #[task]
//! fn double(x: i32) -> i32 { x * 2 }
//!
//! // In main.rs — registers the shuffle handler for (String, u32) pairs:
//! atomic_compute::register_shuffle_map!(String, u32);
//! ```
//!
//! # Modes
//!
//! - **Local** (`Config::local()`) — all partitions run on a thread pool in-process.
//! - **Distributed** (`Config::distributed(workers)`) — partitions are dispatched as
//!   [`TaskEnvelope`](atomic_data::distributed::TaskEnvelope)s over TCP to remote workers.
//!   Workers run the same binary with `--worker --port N`.

// `atomic-runtime-macros`' proc-macros generate hardcoded `::atomic_compute::...` paths (see
// `__macro_support` below). `builtin_tasks/*.rs` calls those macros from inside this crate
// itself, where the crate has no external name — this alias gives it one.
extern crate self as atomic_compute;

pub mod app;
pub mod builtin_tasks;
pub mod context;
pub mod env;
pub mod error;
pub mod executor;
pub mod hosts;
pub mod io;
pub mod rdd;
pub mod registry;
pub mod runtimes;
pub mod task_traits;
pub mod tls;

// Feature-gate macros (`cfg_x!`/`cfg_not_x!`, item positions only — see that crate's
// `lib.rs` and the `atomic-rust-standards` skill for the convention and why there's no
// statement-position variant) are defined once in `atomic_data` and shared
// workspace-wide. Used here via `crate::cfg_x!`, re-exported below.
pub use atomic_data::{
    cfg_js, cfg_k8s, cfg_kafka, cfg_not_js, cfg_not_k8s, cfg_not_python, cfg_python,
};

/// Re-export shim giving `atomic-runtime-macros`' generated code one stable, absolute
/// path (`::atomic_compute::__macro_support::...`) to reference. All 9 macros in
/// `atomic_runtime_macros` are `#[proc_macro]`/`#[proc_macro_attribute]` functions, so
/// they hardcode this path rather than relying on `$crate` (a `macro_rules!`-only
/// mechanism); this module is what makes that path resolve.
pub mod __macro_support {
    pub use crate::registry::{
        CombineKeyEntry, PartitionerEntry, ShuffleKeyEntry, StateMergeEntry, TaskEntry,
        combine_handler, combine_lift_handler, shuffle_map_handler, sort_shuffle_map_handler,
    };
    pub use crate::task_traits::{AggregateTask, BinaryTask, PartitionTask, UnaryTask};
    pub use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
    pub use atomic_data::partitioner::{NamedPartitioner, Partitioner, TypedPartitioner};
    pub use inventory;
}

pub use atomic_runtime_macros::{
    register_aggregate_task, register_binary_task, register_combine, register_combine_lift,
    register_partition_task, register_partitioner, register_shuffle_map, register_sort_shuffle_map,
    register_state_merge, task, task_fn,
};

pub use atomic_scheduler::{ResourceProfile, WorkerAllocator};
pub use env::{Config, WorkerConfig};
pub use registry::{AgentRunner, register_agent_runner};
