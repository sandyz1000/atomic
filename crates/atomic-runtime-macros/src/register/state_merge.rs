use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Expr, Path, Token, parse_macro_input};

/// `register_state_merge!` input: `$name:expr, $handler:path`.
struct StateMergeInput {
    name: Expr,
    handler: Path,
}

impl Parse for StateMergeInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let handler: Path = input.parse()?;
        Ok(StateMergeInput { name, handler })
    }
}

/// Register a state-merge function for distributed stateful streaming under a
/// stable `name`. Place this once in the binary (driver and workers run the same
/// binary). The worker looks up `name` in `STATE_MERGE_REGISTRY` when it handles a
/// [`StepKind::MergeState`](atomic_data::distributed::StepKind::MergeState).
///
/// ```rust,ignore
/// atomic_compute::register_state_merge!("atomic_structured::windowed_v1", windowed_state_merge);
/// ```
pub(crate) fn register_state_merge_impl(input: TokenStream) -> TokenStream {
    let StateMergeInput { name, handler } = parse_macro_input!(input as StateMergeInput);

    TokenStream::from(quote! {
        ::atomic_compute::__macro_support::inventory::submit!(
            ::atomic_compute::__macro_support::StateMergeEntry {
                name: #name,
                handler: #handler,
            }
        );
    })
}
