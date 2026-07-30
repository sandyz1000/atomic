//! Implementation logic for the 6 `register_*!` hand-registration macros, one file per
//! macro. `#[proc_macro]` functions must live at the crate root (see `lib.rs`), so each
//! module here exposes a plain `..._impl` function that the root re-exports.

pub(crate) mod aggregate_task;
pub(crate) mod binary_task;
pub(crate) mod combine_by_key;
pub(crate) mod partition_task;
pub(crate) mod partitioner;
pub(crate) mod shuffle_map;
pub(crate) mod state_merge;
