use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Token, Type, parse_macro_input};

/// `register_binary_task!` input: `$task:ty, $elem:ty`.
struct BinaryTaskInput {
    task: Type,
    elem: Type,
}

impl Parse for BinaryTaskInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let task: Type = input.parse()?;
        input.parse::<Token![,]>()?;
        let elem: Type = input.parse()?;
        Ok(BinaryTaskInput { task, elem })
    }
}

/// Register a [`BinaryTask`](atomic_compute::task_traits::BinaryTask) as a dispatchable
/// builtin, keyed by its `NAME`. Handles the `Fold` / `Aggregate` / `Reduce` actions by
/// folding the partition through `BinaryTask::call`, seeded by the first element — so an
/// empty partition returns empty bytes (the driver skips it) rather than needing an
/// identity value. Use this for monoid-shaped reductions with no identity element (`max`,
/// `min`); reductions that fold from a zero payload (`sum`) register their own handler.
pub(crate) fn register_binary_task_impl(input: TokenStream) -> TokenStream {
    let BinaryTaskInput { task, elem } = parse_macro_input!(input as BinaryTaskInput);

    TokenStream::from(quote! {
        ::atomic_compute::__macro_support::inventory::submit! {
            ::atomic_compute::__macro_support::TaskEntry {
                task_name: <#task as ::atomic_compute::__macro_support::BinaryTask<#elem>>::NAME,
                body_hash: 0,
                handler: |action, payload, data| {
                    use ::atomic_compute::__macro_support::{BinaryTask, TaskAction, WireDecode, WireEncode};
                    let _ = payload;
                    match action {
                        TaskAction::Fold | TaskAction::Aggregate | TaskAction::Reduce => {
                            let items = ::std::vec::Vec::<#elem>::decode_wire(data).map_err(|e| e.to_string())?;
                            let mut iter = items.into_iter();
                            let ::std::option::Option::Some(first) = iter.next() else {
                                return ::std::result::Result::Ok(Vec::new());
                            };
                            let task = <#task>::default();
                            let result = iter.fold(first, |a, b| task.call(a, b));
                            result.encode_wire().map_err(|e| e.to_string())
                        }
                        other => ::std::result::Result::Err(::std::format!(
                            "binary task '{}' does not support action {:?}",
                            <#task as BinaryTask<#elem>>::NAME,
                            other
                        )),
                    }
                },
            }
        }
    })
}
