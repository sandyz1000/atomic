use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::Type;
use syn::parse_macro_input;

use super::KvPair;

/// The base hash-partitioned handler registration, shared by both macros below:
/// `register_sort_shuffle_map!` registers this too (matching the original
/// `macro_rules!`, which called `$crate::register_shuffle_map!($K, $V)` as its
/// first step) plus its own sorted-handler entry.
///
/// Submits a plain `TaskEntry` — shuffle-map handlers dispatch through the same
/// `TASK_REGISTRY`/`TaskHandlerFn` every other registered task does, not a separate
/// shuffle-specific registry/ABI.
fn shuffle_map_tokens(k: &Type, v: &Type) -> TokenStream2 {
    quote! {
        ::atomic_compute::__macro_support::inventory::submit!(
            ::atomic_compute::__macro_support::TaskEntry {
                task_name: concat!(stringify!(#k), "::", stringify!(#v)),
                body_hash: 0,
                handler: ::atomic_compute::__macro_support::shuffle_map_handler::<#k, #v>,
            }
        );
        ::atomic_compute::__macro_support::inventory::submit!(
            ::atomic_compute::__macro_support::ShuffleKeyEntry {
                type_id: || ::std::any::TypeId::of::<(#k, #v)>(),
                key: concat!(stringify!(#k), "::", stringify!(#v)),
            }
        );
    }
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
pub(crate) fn expand(input: TokenStream) -> TokenStream {
    let KvPair { k, v } = parse_macro_input!(input as KvPair);
    TokenStream::from(shuffle_map_tokens(&k, &v))
}

/// Register a **sorted** shuffle-write handler for `(K, V)` where `K: Ord`.
///
/// Use this (instead of / in addition to `register_shuffle_map!`) for key types that are
/// `Ord` and may be shuffled with a range partitioner (e.g. via `sort_by_key`). It registers
/// the base hash handler **and** a sorted handler: in distributed mode the worker then
/// partitions with the RDD's real partitioner and writes sorted runs, so the driver-side reduce
/// produces globally-ordered output. For non-`Ord` keys, use `register_shuffle_map!`.
///
/// ```rust,ignore
/// atomic_compute::register_sort_shuffle_map!(i64, f64);
/// ```
pub(crate) fn expand_sort(input: TokenStream) -> TokenStream {
    let KvPair { k, v } = parse_macro_input!(input as KvPair);
    let base = shuffle_map_tokens(&k, &v);

    TokenStream::from(quote! {
        #base
        ::atomic_compute::__macro_support::inventory::submit!(
            ::atomic_compute::__macro_support::TaskEntry {
                task_name: concat!(stringify!(#k), "::", stringify!(#v), "::sorted"),
                body_hash: 0,
                handler: ::atomic_compute::__macro_support::sort_shuffle_map_handler::<#k, #v>,
            }
        );
    })
}
