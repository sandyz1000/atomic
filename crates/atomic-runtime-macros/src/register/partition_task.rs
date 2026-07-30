use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Token, Type, parse_macro_input};

/// `register_partition_task!` input: `$task:ty, $elem:ty`.
struct PartitionTaskInput {
    task: Type,
    elem: Type,
}

impl Parse for PartitionTaskInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let task: Type = input.parse()?;
        input.parse::<Token![,]>()?;
        let elem: Type = input.parse()?;
        Ok(PartitionTaskInput { task, elem })
    }
}

/// Register a [`PartitionTask`](atomic_compute::task_traits::PartitionTask) as a
/// dispatchable builtin, keyed by its `NAME`. Handles the `Map` / `Collect` actions by
/// decoding the partition into a `Vec`, running `PartitionTask::transform` (which reads
/// the op `payload`), and re-encoding. Use this for whole-partition reductions with no
/// element-level combine (`top_k`, `take_ordered`, `distinct`, `sort`).
pub(crate) fn expand(input: TokenStream) -> TokenStream {
    let PartitionTaskInput { task, elem } = parse_macro_input!(input as PartitionTaskInput);

    TokenStream::from(quote! {
        ::atomic_compute::__macro_support::inventory::submit! {
            ::atomic_compute::__macro_support::TaskEntry {
                task_name: <#task as ::atomic_compute::__macro_support::PartitionTask<#elem>>::NAME,
                body_hash: 0,
                handler: |action, payload, data| {
                    use ::atomic_compute::__macro_support::{
                        PartitionTask, TaskAction, WireDecode, WireEncode,
                    };
                    match action {
                        TaskAction::Map | TaskAction::Collect => {
                            let items = ::std::vec::Vec::<#elem>::decode_wire(data).map_err(|e| e.to_string())?;
                            let out = <#task>::default()
                                .transform(items, payload)
                                .map_err(|e| e.to_string())?;
                            out.encode_wire().map_err(|e| e.to_string())
                        }
                        other => ::std::result::Result::Err(::std::format!(
                            "partition task '{}' does not support action {:?}",
                            <#task as PartitionTask<#elem>>::NAME,
                            other
                        )),
                    }
                },
            }
        }
    })
}
