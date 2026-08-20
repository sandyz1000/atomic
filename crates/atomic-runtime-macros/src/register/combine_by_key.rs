use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Token, Type, parse_macro_input};

use super::KvPair;

/// `register_combine_lift!` input: `$K:ty, $V:ty, $C:ty`.
struct KvcTriple {
    k: Type,
    v: Type,
    c: Type,
}

impl Parse for KvcTriple {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let k: Type = input.parse()?;
        input.parse::<Token![,]>()?;
        let v: Type = input.parse()?;
        input.parse::<Token![,]>()?;
        let c: Type = input.parse()?;
        Ok(KvcTriple { k, v, c })
    }
}

/// Enable **opt-in map-side pre-combine** for `reduce_by_key_task` / `fold_by_key_task`
/// on `TypedRdd<(K, V)>` (the `C == V` case).
///
/// Place this once in the binary alongside `register_shuffle_map!(K, V)`. When a `_task`
/// pipeline precedes the shuffle in distributed mode, the worker groups same-key values in
/// each map partition and folds them with the reduction *before* the shuffle write, so fewer
/// pairs cross the network. Without this call, behaviour is unchanged (raw pairs shuffled).
///
/// The dispatch key (`"K::V::combine"`) is generated from the source-level token text of `K`
/// and `V` via `stringify!`, matching `register_shuffle_map!`. The `::combine` suffix keeps it
/// distinct from the shuffle-map handler's `"K::V"` key in the shared `TASK_REGISTRY`.
///
/// ```rust,ignore
/// atomic_compute::register_shuffle_map!(String, i64);
/// atomic_compute::register_combine!(String, i64);
/// ```
pub(crate) fn expand(input: TokenStream) -> TokenStream {
    let KvPair { k, v } = parse_macro_input!(input as KvPair);
    TokenStream::from(quote! {
        ::atomic_compute::__macro_support::inventory::submit!(
            ::atomic_compute::__macro_support::TaskEntry {
                task_name: concat!(stringify!(#k), "::", stringify!(#v), "::combine"),
                body_hash: 0,
                handler: ::atomic_compute::__macro_support::combine_handler::<#k, #v>,
            }
        );
        ::atomic_compute::__macro_support::inventory::submit!(
            ::atomic_compute::__macro_support::CombineKeyEntry {
                type_id: || ::std::any::TypeId::of::<(#k, #v)>(),
                key: concat!(stringify!(#k), "::", stringify!(#v), "::combine"),
            }
        );
    })
}

/// Enable **opt-in map-side pre-combine** for `aggregate_by_key_task` on `TypedRdd<(K, V)>`
/// with a distinct accumulator type `C` (the `C != V` case).
///
/// Beyond the combine handler, this also registers the shuffle-map handler for `(K, C)` —
/// the pre-combined pairs shipped over the wire are `(K, C)`, not `(K, V)` — so you do **not**
/// separately call `register_shuffle_map!(K, C)`. (You still register the base `(K, V)`
/// shuffle handler for the non-combined fallback path.)
///
/// ```rust,ignore
/// atomic_compute::register_shuffle_map!(MovieId, f64);              // fallback path
/// atomic_compute::register_combine_lift!(MovieId, f64, (f64, u64));
/// ```
pub(crate) fn expand_lift(input: TokenStream) -> TokenStream {
    let KvcTriple { k, v, c } = parse_macro_input!(input as KvcTriple);
    TokenStream::from(quote! {
        // Combine-lift handler, keyed by the (K, V, C) dispatch string (`::combine` suffix
        // keeps it distinct from any shuffle-map key in the shared TASK_REGISTRY).
        ::atomic_compute::__macro_support::inventory::submit!(
            ::atomic_compute::__macro_support::TaskEntry {
                task_name: concat!(stringify!(#k), "::", stringify!(#v), "::", stringify!(#c), "::combine"),
                body_hash: 0,
                handler: ::atomic_compute::__macro_support::combine_lift_handler::<#k, #v, #c>,
            }
        );
        ::atomic_compute::__macro_support::inventory::submit!(
            ::atomic_compute::__macro_support::CombineKeyEntry {
                type_id: || ::std::any::TypeId::of::<(#k, #v, #c)>(),
                key: concat!(stringify!(#k), "::", stringify!(#v), "::", stringify!(#c), "::combine"),
            }
        );
        // The post-combine shuffle carries (K, C) pairs, so register that shuffle handler too.
        ::atomic_compute::__macro_support::inventory::submit!(
            ::atomic_compute::__macro_support::TaskEntry {
                task_name: concat!(stringify!(#k), "::", stringify!(#c)),
                body_hash: 0,
                handler: ::atomic_compute::__macro_support::shuffle_map_handler::<#k, #c>,
            }
        );
        ::atomic_compute::__macro_support::inventory::submit!(
            ::atomic_compute::__macro_support::ShuffleKeyEntry {
                type_id: || ::std::any::TypeId::of::<(#k, #c)>(),
                key: concat!(stringify!(#k), "::", stringify!(#c)),
            }
        );
    })
}
