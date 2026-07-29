use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Token, Type, parse_macro_input};

/// `register_aggregate_task!` input: `$task:ty, $acc:ty, $elem:ty`.
struct AggregateTaskInput {
    task: Type,
    acc: Type,
    elem: Type,
}

impl Parse for AggregateTaskInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let task: Type = input.parse()?;
        input.parse::<Token![,]>()?;
        let acc: Type = input.parse()?;
        input.parse::<Token![,]>()?;
        let elem: Type = input.parse()?;
        Ok(AggregateTaskInput { task, acc, elem })
    }
}

/// Register a worker dispatch handler for a type implementing
/// [`AggregateTask<Acc, Elem>`](atomic_compute::task_traits::AggregateTask).
///
/// The `#[task]` macro only covers unary (`fn(T) -> U`) and binary (`fn(T, T) -> T`) shapes;
/// an aggregate has two functions with a distinct accumulator type, so it registers through
/// this macro instead (the same hand-registration path the numeric builtins use). The handler
/// folds a partition into one `Acc` via `AggregateTask::seq` on the worker; the driver merges
/// the per-partition accumulators with `AggregateTask::comb`
/// (see `TypedRdd::aggregate_task`).
///
/// `Acc` (decoded from `Step.payload`) is the zero accumulator; the task type must be `Default`.
///
/// ```rust,ignore
/// atomic_compute::register_aggregate_task!(MyAgg, (f64, u64), f64);
/// ```
pub(crate) fn register_aggregate_task_impl(input: TokenStream) -> TokenStream {
    let AggregateTaskInput { task, acc, elem } = parse_macro_input!(input as AggregateTaskInput);

    TokenStream::from(quote! {
        ::atomic_compute::__macro_support::inventory::submit! {
            ::atomic_compute::__macro_support::TaskEntry {
                task_name:
                    <#task as ::atomic_compute::__macro_support::AggregateTask<#acc, #elem>>::NAME,
                body_hash: 0,
                handler: |action, payload, data| {
                    use ::atomic_compute::__macro_support::{
                        AggregateTask, TaskAction, WireDecode, WireEncode,
                    };
                    match action {
                        TaskAction::Aggregate | TaskAction::Fold => {
                            let zero = <#acc>::decode_wire(payload).map_err(|e| e.to_string())?;
                            let items = ::std::vec::Vec::<#elem>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let task = <#task>::default();
                            let acc = items.into_iter().fold(zero, |a, x| task.seq(a, x));
                            acc.encode_wire().map_err(|e| e.to_string())
                        }
                        other => ::std::result::Result::Err(::std::format!(
                            "aggregate task '{}' does not support action {:?}",
                            <#task as AggregateTask<#acc, #elem>>::NAME,
                            other
                        )),
                    }
                },
            }
        }
    })
}
