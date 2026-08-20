//! Implementation logic for the 6 `register_*!` hand-registration macros, one file per
//! macro. `#[proc_macro]` functions must live at the crate root (see `lib.rs`), so each
//! module here exposes a plain `..._impl` function that the root re-exports.

use syn::parse::{Parse, ParseStream};
use syn::{Token, Type};

pub(crate) mod aggregate_task;
pub(crate) mod binary_task;
pub(crate) mod combine_by_key;
pub(crate) mod partition_task;
pub(crate) mod partitioner;
pub(crate) mod shuffle_map;
pub(crate) mod state_merge;

/// `$K:ty, $V:ty` macro input, shared by `register_shuffle_map!`/`register_sort_shuffle_map!`
/// and `register_combine!`.
pub(crate) struct KvPair {
    pub(crate) k: Type,
    pub(crate) v: Type,
}

impl Parse for KvPair {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let k: Type = input.parse()?;
        input.parse::<Token![,]>()?;
        let v: Type = input.parse()?;
        Ok(KvPair { k, v })
    }
}
