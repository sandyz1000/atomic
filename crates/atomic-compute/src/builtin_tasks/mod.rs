//! Built-in framework tasks registered via `inventory`.
//!
//! These implement `BinaryTask<T>` or `UnaryTask<T, U>` and are dispatched on
//! workers exactly like user `#[task]` functions — the driver sends an `op_id`
//! string and the worker binary contains the handler via `inventory::submit!`.
//!
//! Built-in tasks power `TypedRdd::max()`, `min()`, `count()`, and can be used
//! directly with `fold_task` / `map_task`.
//!
//! # Worker registration
//!
//! Each task registers itself in the compile-time dispatch table.  Because these
//! are in the `atomic-compute` crate (which is linked into every driver and worker
//! binary), the handlers are always present — no user action required.

pub mod count;
pub mod distinct;
pub mod max;
pub mod mean;
pub mod min;
pub mod sort;
pub mod sum;
pub mod take_ordered;
pub mod topk;
