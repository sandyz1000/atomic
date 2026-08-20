//! Codegen crate for the Atomic distributed compute engine — every macro that
//! generates compile-time task/registry registration lives here, one file per
//! macro, mirroring `rusty-celery`'s `celery-codegen` crate. All generated code
//! hardcodes the absolute path back to the runtime crate
//! (`::atomic_compute::__macro_support::...`) rather than relying on `$crate`,
//! since these are proc-macros, not `macro_rules!`.
//!
//! Rust requires every `#[proc_macro]` / `#[proc_macro_attribute]` function to live
//! at the crate root (empirically confirmed — the compiler rejects any other
//! placement), so each module below exposes a plain `expand` function holding the
//! actual codegen and this file re-exports it through a one-line tagged wrapper —
//! the same shape `thiserror-impl`/`syn`'s derive macros use, not a `_impl`-suffixed
//! double of the same name. The 6 `register_*!` macros' `expand` fns live under
//! `register/` (grouped by their shared prefix); `task`/`task_fn` stay at the top
//! level since they don't share that prefix with anything else.

use proc_macro::TokenStream;

mod body_hash;
mod register;
mod shape;
mod task;
mod task_fn;

#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    task::expand(attr, item)
}

#[proc_macro]
pub fn task_fn(input: TokenStream) -> TokenStream {
    task_fn::expand(input)
}

#[proc_macro]
pub fn register_shuffle_map(input: TokenStream) -> TokenStream {
    register::shuffle_map::expand(input)
}

#[proc_macro]
pub fn register_sort_shuffle_map(input: TokenStream) -> TokenStream {
    register::shuffle_map::expand_sort(input)
}

#[proc_macro]
pub fn register_combine(input: TokenStream) -> TokenStream {
    register::combine_by_key::expand(input)
}

#[proc_macro]
pub fn register_combine_lift(input: TokenStream) -> TokenStream {
    register::combine_by_key::expand_lift(input)
}

#[proc_macro]
pub fn register_partitioner(input: TokenStream) -> TokenStream {
    register::partitioner::expand(input)
}

#[proc_macro]
pub fn register_state_merge(input: TokenStream) -> TokenStream {
    register::state_merge::expand(input)
}

#[proc_macro]
pub fn register_aggregate_task(input: TokenStream) -> TokenStream {
    register::aggregate_task::expand(input)
}

#[proc_macro]
pub fn register_binary_task(input: TokenStream) -> TokenStream {
    register::binary_task::expand(input)
}

#[proc_macro]
pub fn register_partition_task(input: TokenStream) -> TokenStream {
    register::partition_task::expand(input)
}
