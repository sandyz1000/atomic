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

pub mod app;
pub mod builtin_tasks;
pub mod context;
pub mod env;
pub mod error;
pub mod executor;
pub mod hosts;
pub mod io;
pub mod rdd;
pub mod runtimes;
pub mod shuffle_map;
pub mod task_registry;
pub mod task_traits;
pub mod tls;

// Feature-gate macros (`cfg_x!`/`cfg_not_x!`, item positions only — see that crate's
// `lib.rs` and the `atomic-rust-standards` skill for the convention and why there's no
// statement-position variant) are defined once in `atomic_data` and shared
// workspace-wide. Used here via `crate::cfg_x!`, re-exported below.
pub use atomic_data::{
    cfg_js, cfg_k8s, cfg_kafka, cfg_not_js, cfg_not_k8s, cfg_not_python, cfg_not_s3, cfg_not_tls,
    cfg_python, cfg_s3, cfg_tls,
};

pub mod __macro_support {
    pub use crate::task_registry::{
        PartitionerEntry, ShuffleKeyEntry, ShuffleMapEntry, SortShuffleMapEntry, StateMergeEntry,
        TaskEntry,
    };
    pub use crate::task_traits::{BinaryTask, UnaryTask};
    pub use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
    pub use atomic_data::partitioner::{NamedPartitioner, Partitioner, TypedPartitioner};
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
        $crate::__macro_support::inventory::submit!($crate::__macro_support::ShuffleMapEntry {
            type_id: || concat!(stringify!($K), "::", stringify!($V)),
            handler: $crate::shuffle_map::shuffle_map_handler::<$K, $V>,
        });
        $crate::__macro_support::inventory::submit!($crate::__macro_support::ShuffleKeyEntry {
            type_id: || std::any::TypeId::of::<($K, $V)>(),
            key: concat!(stringify!($K), "::", stringify!($V)),
        });
    };
}

/// Register a **sorted** shuffle-write handler for `(K, V)` where `K: Ord`.
///
/// Use this (instead of / in addition to [`register_shuffle_map!`]) for key types that are
/// `Ord` and may be shuffled with a range partitioner (e.g. via `sort_by_key`). It registers
/// the base hash handler **and** a sorted handler: in distributed mode the worker then
/// partitions with the RDD's real partitioner and writes sorted runs, so the driver-side reduce
/// produces globally-ordered output. For non-`Ord` keys, use [`register_shuffle_map!`].
///
/// ```rust,ignore
/// atomic_compute::register_sort_shuffle_map!(i64, f64);
/// ```
#[macro_export]
macro_rules! register_sort_shuffle_map {
    ($K:ty, $V:ty) => {
        $crate::register_shuffle_map!($K, $V);
        $crate::__macro_support::inventory::submit!($crate::__macro_support::SortShuffleMapEntry {
            type_id: || concat!(stringify!($K), "::", stringify!($V)),
            handler: $crate::shuffle_map::sort_shuffle_map_handler::<$K, $V>,
        });
    };
}

/// Register a [`NamedPartitioner`](atomic_data::partitioner::NamedPartitioner) so
/// distributed `partition_by_named` can ship it to workers by name (no closure
/// serialization). Place this once in the binary, like `register_shuffle_map!`.
///
/// ```rust,ignore
/// atomic_compute::register_partitioner!(ModPartitioner);
/// ```
#[macro_export]
macro_rules! register_partitioner {
    ($P:ty) => {
        $crate::__macro_support::inventory::submit!($crate::__macro_support::PartitionerEntry {
            name: || <$P as $crate::__macro_support::NamedPartitioner>::NAME,
            factory: |n| $crate::__macro_support::Partitioner::from_named::<$P>(n),
        });
    };
}

/// Register a state-merge function for distributed stateful streaming under a
/// stable `name`. Place this once in the binary (driver and workers run the same
/// binary). The worker looks up `name` in `STATE_MERGE_REGISTRY` when it handles a
/// [`StepKind::MergeState`](atomic_data::distributed::StepKind::MergeState).
///
/// ```rust,ignore
/// atomic_compute::register_state_merge!("atomic_structured::windowed_v1", windowed_state_merge);
/// ```
#[macro_export]
macro_rules! register_state_merge {
    ($name:expr, $handler:path) => {
        $crate::__macro_support::inventory::submit!($crate::__macro_support::StateMergeEntry {
            name: $name,
            handler: $handler,
        });
    };
}

pub use atomic_runtime_macros::task;
pub use atomic_runtime_macros::task_fn;

pub use atomic_scheduler::{ResourceProfile, WorkerAllocator};
pub use env::{Config, WorkerConfig};
pub use task_registry::{AgentRunner, register_agent_runner};
