//! Built-in framework tasks registered via `inventory`.
//!
//! These are dispatched on workers exactly like user `#[task]` functions — the
//! driver sends a `task_name` string and the worker binary contains the handler via
//! `inventory::submit!`.
//!
//! Built-in tasks power `TypedRdd::mean()`, `variance()`/`stdev()`, the per-key aggregators
//! (`sum_values` / `max_values` / `min_values`), and can be used directly with `fold_task` /
//! `map_task`.
//!
//! # One trait per reduction shape
//!
//! The trait a builtin implements is dictated by the reduction's *shape*, and each trait has a
//! matching `register_*_task!` macro that generates its `inventory` dispatch handler:
//!
//! - **`max`, `min` implement [`BinaryTask<T>`](crate::task_traits::BinaryTask)**, registered
//!   with `register_binary_task!`. They are monoids with no identity element: `fn(T, T) -> T`,
//!   associative, accumulator type equal to the element type, seeded from the first element.
//!
//! - **`sum` also implements `BinaryTask<T>`** but registers by hand, because it folds from a
//!   payload-supplied zero (the fold identity) rather than seeding from the first element.
//!
//! - **`mean`, `variance`, `stats`, `count_approx_distinct` implement
//!   [`AggregateTask<A, T>`](crate::task_traits::AggregateTask)**, registered with
//!   `register_aggregate_task!`. Their accumulator type `A` differs from the element type `T`
//!   (`mean`: `(f64, u64)`; `variance`: `(u64, f64, f64)`), the asymmetric `seqOp: fn(A, T) -> A`
//!   shape that `BinaryTask` cannot express.
//!
//! - **`distinct`, `sort`, `topk`, `take_ordered` implement
//!   [`PartitionTask<T>`](crate::task_traits::PartitionTask)**, registered with
//!   `register_partition_task!`. They have no element-level combine — the handler transforms the
//!   whole partition `Vec<T>` (dedup, sort, local top-k) and the cross-partition merge is done
//!   driver-side.
//!
//! # Worker registration
//!
//! Each task registers itself in the compile-time dispatch table.  Because these
//! are in the `atomic-compute` crate (which is linked into every driver and worker
//! binary), the handlers are always present — no user action required.

pub mod distinct;
pub mod hll;
pub mod max;
pub mod mean;
pub mod min;
pub mod sort;
pub mod stats;
pub mod sum;
pub mod take_ordered;
pub mod topk;
pub mod variance;

/// Widening conversion to `f64` for the numeric primitives, so generic aggregations
/// (`mean`, `variance`, `histogram`) can accept any of them without a per-type macro at the
/// call site.
pub trait NumericValue: Copy + Send + Sync + 'static {
    fn to_f64(self) -> f64;
}

macro_rules! impl_numeric_value {
    ($($ty:ty),*) => {
        $(impl NumericValue for $ty {
            fn to_f64(self) -> f64 {
                self as f64
            }
        })*
    };
}

impl_numeric_value!(i32, i64, u32, u64, f32, f64);

/// Map a concrete element type to the `task_name` of its registered builtin, or `None` when
/// the type is not one of the primitives (plus `String`) the builtins cover. Used by
/// `max`/`min`/`top`/`take_ordered` to route to worker-side reduction for covered types and fall
/// back to driver-side per-partition processing otherwise. The `TypeId` scan is O(covered types),
/// once per action — it routes to existing `TASK_REGISTRY` entries, it does not create a dispatch
/// path. A `TypeId` cannot appear in a `match` pattern (it is a runtime value, not a structural
/// constant), so the covered set is a table scanned with `find_map`.
macro_rules! builtin_name_lookup {
    ($name:ident, $prefix:literal) => {
        pub fn $name<T: 'static>() -> Option<&'static str> {
            use std::any::TypeId;
            let t = TypeId::of::<T>();
            [
                (TypeId::of::<i32>(), concat!($prefix, "i32")),
                (TypeId::of::<i64>(), concat!($prefix, "i64")),
                (TypeId::of::<u32>(), concat!($prefix, "u32")),
                (TypeId::of::<u64>(), concat!($prefix, "u64")),
                (TypeId::of::<f32>(), concat!($prefix, "f32")),
                (TypeId::of::<f64>(), concat!($prefix, "f64")),
                (TypeId::of::<String>(), concat!($prefix, "String")),
            ]
            .into_iter()
            .find_map(|(id, name)| (id == t).then_some(name))
        }
    };
}

builtin_name_lookup!(max_task_name, "atomic::builtin::max::");
builtin_name_lookup!(min_task_name, "atomic::builtin::min::");
builtin_name_lookup!(top_k_task_name, "atomic::builtin::top_k::");
builtin_name_lookup!(take_ordered_task_name, "atomic::builtin::take_ordered::");
