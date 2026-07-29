//! Codegen crate for the Atomic distributed compute engine — every macro that
//! generates compile-time task/registry registration lives here, one file per
//! macro, mirroring `rusty-celery`'s `celery-codegen` crate. All generated code
//! hardcodes the absolute path back to the runtime crate
//! (`::atomic_compute::__macro_support::...`) rather than relying on `$crate`,
//! since these are proc-macros, not `macro_rules!`.
//!
//! Rust requires every `#[proc_macro]` / `#[proc_macro_attribute]` function to live
//! at the crate root, so each module below exposes a plain `..._impl` function and
//! this file re-exports it through a one-line tagged wrapper. The 6 `register_*!`
//! macros' impls live under `register/` (grouped by their shared prefix); `task`/`task_fn`
//! stay at the top level since they don't share that prefix with anything else.

use proc_macro::TokenStream;

mod common;
mod register;
mod task;
mod task_fn;

#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    task::task_impl(attr, item)
}

#[proc_macro]
pub fn task_fn(input: TokenStream) -> TokenStream {
    task_fn::task_fn_impl(input)
}

#[proc_macro]
pub fn register_shuffle_map(input: TokenStream) -> TokenStream {
    register::shuffle_map::register_shuffle_map_impl(input)
}

#[proc_macro]
pub fn register_sort_shuffle_map(input: TokenStream) -> TokenStream {
    register::shuffle_map::register_sort_shuffle_map_impl(input)
}

#[proc_macro]
pub fn register_partitioner(input: TokenStream) -> TokenStream {
    register::partitioner::register_partitioner_impl(input)
}

#[proc_macro]
pub fn register_state_merge(input: TokenStream) -> TokenStream {
    register::state_merge::register_state_merge_impl(input)
}

#[proc_macro]
pub fn register_aggregate_task(input: TokenStream) -> TokenStream {
    register::aggregate_task::register_aggregate_task_impl(input)
}

#[proc_macro]
pub fn register_binary_task(input: TokenStream) -> TokenStream {
    register::binary_task::register_binary_task_impl(input)
}

#[proc_macro]
pub fn register_partition_task(input: TokenStream) -> TokenStream {
    register::partition_task::register_partition_task_impl(input)
}
