use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{ItemFn, LitStr, ReturnType, Type, parse_macro_input};

/// Attribute macro for defining a distributed Atomic task function.
///
/// Annotating a function with `#[task]` does two things at compile time:
///
/// 1. **Preserves the original function** exactly as written — it can still be
///    called directly in local mode, tests, or anywhere in the same binary.
///
/// 2. **Registers a dispatch handler** into the global compile-time task registry
///    via the `inventory` crate. When the binary runs as a worker, incoming
///    `TaskEnvelope` messages are dispatched to the handler by `op_id`.
///
/// The `op_id` defaults to `"<crate>::<module>::<fn_name>"` but can be overridden:
///
/// ```ignore
/// #[task]                        // op_id = "mycrate::mymod::double"
/// fn double(x: i32) -> i32 { x * 2 }
///
/// #[task(name = "custom.op.v1")]  // op_id = "custom.op.v1"
/// fn double_v1(x: i32) -> i32 { x * 2 }
/// ```
///
/// # Dispatch handler
///
/// For each `#[task]`-annotated function the macro generates a private function
/// `__atomic_dispatch_<fn_name>` that accepts a `TaskAction`, a `payload` byte
/// slice (rkyv-encoded action config), and a `data` byte slice (rkyv-encoded
/// `Vec<T>` partition elements). It returns `Result<Vec<u8>, String>`.
///
/// Supported actions depend on function signature:
///
/// **Unary `fn(T) -> U`** (single argument):
/// - `TaskAction::Map`     — apply element-wise, return `Vec<U>`
/// - `TaskAction::Filter`  — only if U is `bool`: keep elements where fn returns true
/// - `TaskAction::FlatMap` — U must implement `IntoIterator`; flatten results
/// - `TaskAction::Collect` — identity pass-through
///
/// **Binary `fn(T, T) -> T`** (two arguments, same type):
/// - `TaskAction::Fold`    — fold with rkyv-decoded zero from `payload`
/// - `TaskAction::Reduce`  — reduce using fn as combiner (error on empty partition)
/// - `TaskAction::Aggregate` — same as Fold
///
/// # Input/output requirements
///
/// Types must implement rkyv `Archive`, `Serialize`, `Deserialize`, plus
/// `bytecheck::CheckBytes`. Primitive types satisfy these automatically.
///
/// # Example — unary map
/// ```ignore
/// use atomic_runtime::task;
///
/// #[task]
/// fn double(x: i32) -> i32 { x * 2 }
/// ```
///
/// # Example — binary reduce
/// ```ignore
/// #[task]
/// fn add(a: i32, b: i32) -> i32 { a + b }
///
/// ctx.parallelize_typed(vec![1, 2, 3], 2).fold(0, add)?;
/// ```
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    // Parse optional `name = "custom.op.id"` attribute argument.
    let custom_name: Option<String> = if attr.is_empty() {
        None
    } else {
        syn::parse::<LitStr>(attr.clone())
            .ok()
            .map(|lit| lit.value())
            .or_else(|| {
                let attr2: proc_macro2::TokenStream = attr.into();
                syn::parse2::<syn::MetaNameValue>(attr2).ok().and_then(|mnv| {
                    if mnv.path.is_ident("name") {
                        if let syn::Expr::Lit(syn::ExprLit {
                            lit: syn::Lit::Str(s), ..
                        }) = mnv.value
                        {
                            Some(s.value())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
            })
    };

    let fn_name = &input.sig.ident;
    let fn_name_str = fn_name.to_string();
    let dispatch_fn_name =
        syn::Ident::new(&format!("__atomic_dispatch_{}", fn_name), Span::call_site());

    let fn_vis = &input.vis;
    let fn_sig = &input.sig;
    let fn_block = &input.block;
    let fn_attrs = &input.attrs;

    // Number of arguments determines dispatch shape.
    let num_args = input.sig.inputs.len();

    // First argument type — the partition element type T.
    let input_type: proc_macro2::TokenStream = input
        .sig
        .inputs
        .iter()
        .next()
        .and_then(|arg| match arg {
            syn::FnArg::Typed(pat_type) => Some(*pat_type.ty.clone()),
            _ => None,
        })
        .map(|ty| quote! { #ty })
        .unwrap_or_else(|| quote! { Vec<u8> });

    // Return type U.
    let output_type: proc_macro2::TokenStream = match &input.sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => {
            let ty: &Type = ty;
            quote! { #ty }
        }
    };

    // Whether the return type is plain `bool` — determines if Filter is generated.
    let is_bool_return = match &input.sig.output {
        ReturnType::Type(_, ty) => {
            if let Type::Path(tp) = ty.as_ref() {
                tp.path.is_ident("bool")
            } else {
                false
            }
        }
        _ => false,
    };

    // Whether the return type is `Vec<_>` — determines if FlatMap is generated.
    let is_vec_return = match &input.sig.output {
        ReturnType::Type(_, ty) => {
            if let Type::Path(tp) = ty.as_ref() {
                tp.path.segments.last()
                    .map(|s| s.ident == "Vec")
                    .unwrap_or(false)
            } else {
                false
            }
        }
        _ => false,
    };

    // op_id expression resolved at compile time in the user crate.
    let op_id_expr: proc_macro2::TokenStream = if let Some(name) = custom_name {
        quote! { #name }
    } else {
        quote! { concat!(module_path!(), "::", #fn_name_str) }
    };

    // ── Generate dispatch arms based on function signature ────────────────────
    //
    // Unary fns (1 arg): Map, Collect, (Filter if bool), (FlatMap if IntoIterator output)
    // Binary fns (2 args, T,T->T): Fold, Reduce, Aggregate
    // All other arms fall through to an unsupported error.

    let dispatch_body = if num_args == 2 {
        // Binary function: (T, T) -> T — supports Fold, Reduce, Aggregate.
        quote! {
            use ::atomic_compute::__macro_support::{TaskAction, WireDecode, WireEncode};
            match action {
                TaskAction::Fold | TaskAction::Aggregate => {
                    let zero = <#input_type>::decode_wire(payload)
                        .map_err(|e| e.to_string())?;
                    let items = ::std::vec::Vec::<#input_type>::decode_wire(data)
                        .map_err(|e| e.to_string())?;
                    let result = items.into_iter().fold(zero, |acc, x| #fn_name(acc, x));
                    result.encode_wire().map_err(|e| e.to_string())
                }
                TaskAction::Reduce => {
                    let items = ::std::vec::Vec::<#input_type>::decode_wire(data)
                        .map_err(|e| e.to_string())?;
                    let mut iter = items.into_iter();
                    let first = iter.next()
                        .ok_or_else(|| "reduce called on empty partition".to_string())?;
                    let result = iter.fold(first, |acc, x| #fn_name(acc, x));
                    result.encode_wire().map_err(|e| e.to_string())
                }
                other => Err(::std::format!(
                    "task '{}' (binary fn) does not support action {:?}",
                    #fn_name_str, other
                )),
            }
        }
    } else if is_bool_return {
        // Unary function returning bool: supports Map (→ Vec<bool>), Filter, Collect.
        quote! {
            use ::atomic_compute::__macro_support::{TaskAction, WireDecode, WireEncode};
            match action {
                TaskAction::Map | TaskAction::Collect => {
                    let items = ::std::vec::Vec::<#input_type>::decode_wire(data)
                        .map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<bool> =
                        items.into_iter().map(#fn_name).collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                TaskAction::Filter => {
                    let items = ::std::vec::Vec::<#input_type>::decode_wire(data)
                        .map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<#input_type> =
                        items.into_iter().filter(|x| #fn_name(x.clone())).collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                other => Err(::std::format!(
                    "task '{}' (predicate fn) does not support action {:?}",
                    #fn_name_str, other
                )),
            }
        }
    } else if is_vec_return {
        // Unary function T -> Vec<U>: supports Map (returns Vec<Vec<U>>), FlatMap, Collect.
        quote! {
            use ::atomic_compute::__macro_support::{TaskAction, WireDecode, WireEncode};
            match action {
                TaskAction::Map | TaskAction::Collect => {
                    let items = ::std::vec::Vec::<#input_type>::decode_wire(data)
                        .map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<#output_type> =
                        items.into_iter().map(#fn_name).collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                TaskAction::FlatMap => {
                    let items = ::std::vec::Vec::<#input_type>::decode_wire(data)
                        .map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<_> =
                        items.into_iter().flat_map(#fn_name).collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                other => Err(::std::format!(
                    "task '{}' does not support action {:?}",
                    #fn_name_str, other
                )),
            }
        }
    } else {
        // Unary function T -> U (non-bool, non-Vec): supports Map, Collect only.
        quote! {
            use ::atomic_compute::__macro_support::{TaskAction, WireDecode, WireEncode};
            match action {
                TaskAction::Map | TaskAction::Collect => {
                    let items = ::std::vec::Vec::<#input_type>::decode_wire(data)
                        .map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<#output_type> =
                        items.into_iter().map(#fn_name).collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                other => Err(::std::format!(
                    "task '{}' does not support action {:?}",
                    #fn_name_str, other
                )),
            }
        }
    };

    TokenStream::from(quote! {
        // 1. Preserve the original function unchanged — callable directly anywhere.
        #(#fn_attrs)*
        #fn_vis #fn_sig {
            #fn_block
        }

        // 2. Dispatch handler: decodes partition bytes, applies the requested action,
        //    re-encodes the result. Called by NativeBackend when op_id matches.
        #[doc(hidden)]
        fn #dispatch_fn_name(
            action: &::atomic_compute::__macro_support::TaskAction,
            payload: &[u8],
            data: &[u8],
        ) -> ::std::result::Result<::std::vec::Vec<u8>, ::std::string::String> {
            #dispatch_body
        }

        // 3. Compile-time registration via inventory.
        //    The worker binary collects all submitted entries at startup.
        ::atomic_compute::__macro_support::inventory::submit! {
            ::atomic_compute::__macro_support::TaskEntry {
                op_id: #op_id_expr,
                handler: #dispatch_fn_name,
            }
        }
    })
}
