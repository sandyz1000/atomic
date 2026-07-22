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
//! # Why only some implement `BinaryTask`
//!
//! The choice of trait is dictated by the reduction's *shape*, not by preference:
//!
//! - **`sum`, `max`, `min` implement [`BinaryTask<T>`](crate::task_traits::BinaryTask).**
//!   They are true monoids: `fn(T, T) -> T`, associative, with the accumulator type equal
//!   to the element type, so one trait method serves both the map-side fold and the
//!   driver-side merge.
//!
//! - **`count`, `mean` do *not* — their accumulator type differs from the element type.**
//!   `count` folds `T` into a running `u64` (`fn(u64, T) -> u64`); `mean` folds into a
//!   `(f64, u64)` sum/count pair. That asymmetric `seqOp: fn(A, T) -> A` shape cannot be
//!   typed by `BinaryTask<T>` (which forces both arguments to the same type) — it is the
//!   [`AggregateTask<A, T>`](crate::task_traits::AggregateTask) shape. These are hand-written
//!   dispatch handlers today; they are the natural candidates to migrate onto `AggregateTask`
//!   (see `notes/improvement-plan.md`).
//!
//! - **`distinct`, `sort`, `topk`, `take_ordered` have no element-level associative
//!   combine at all.** They operate over the whole partition `Vec<T>` (dedup, sort,
//!   local top-k), so they are neither `BinaryTask` nor `AggregateTask` — the handler
//!   consumes the full partition and emits a partition, with the cross-partition merge
//!   done driver-side.
//!
//! # Worker registration
//!
//! Each task registers itself in the compile-time dispatch table.  Because these
//! are in the `atomic-compute` crate (which is linked into every driver and worker
//! binary), the handlers are always present — no user action required.

pub mod distinct;
pub mod max;
pub mod mean;
pub mod min;
pub mod sort;
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
