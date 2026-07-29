use proc_macro::TokenStream;
use quote::quote;
use syn::{Type, parse_macro_input};

/// Register a [`NamedPartitioner`](atomic_data::partitioner::NamedPartitioner) so
/// distributed `partition_by_named` can ship it to workers by name (no closure
/// serialization). Place this once in the binary, like `register_shuffle_map!`.
///
/// ```rust,ignore
/// atomic_compute::register_partitioner!(ModPartitioner);
/// ```
pub(crate) fn register_partitioner_impl(input: TokenStream) -> TokenStream {
    let p = parse_macro_input!(input as Type);

    TokenStream::from(quote! {
        ::atomic_compute::__macro_support::inventory::submit!(
            ::atomic_compute::__macro_support::PartitionerEntry {
                name: || <#p as ::atomic_compute::__macro_support::NamedPartitioner>::NAME,
                factory: |n| ::atomic_compute::__macro_support::Partitioner::from_named::<#p>(n),
            }
        );
    })
}
