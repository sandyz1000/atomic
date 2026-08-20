use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{ExprClosure, Ident, ReturnType, Token, Type, bracketed, parse_macro_input};

use crate::body_hash::body_hash_parts;
use crate::shape::{is_bool_type, is_vec_type};

/// A single `name: Type` entry in a `task_fn!` capture list.
struct CaptureItem {
    name: Ident,
    ty: Type,
}

impl Parse for CaptureItem {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        input.parse::<Token![:]>()?;
        let ty: Type = input.parse()?;
        Ok(CaptureItem { name, ty })
    }
}

/// `task_fn!` input: an optional `[a: T, b: U]` capture list followed by a closure.
struct TaskFnInput {
    captures: Vec<CaptureItem>,
    closure: ExprClosure,
}

impl Parse for TaskFnInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let captures = if input.peek(syn::token::Bracket) {
            let content;
            bracketed!(content in input);
            content
                .parse_terminated(CaptureItem::parse, Token![,])?
                .into_iter()
                .collect()
        } else {
            Vec::new()
        };
        let closure: ExprClosure = input.parse()?;
        Ok(TaskFnInput { captures, closure })
    }
}

/// Wrap an inline non-capturing closure into a zero-sized task struct that implements
/// `UnaryTask` or `BinaryTask`, enabling it to run on distributed workers.
///
/// Arguments **must be explicitly typed**. For unary closures, the return type
/// **must be annotated** with `-> ReturnType` so the dispatch handler can be generated.
/// Binary (fold) closures infer the return type from the first argument type.
///
/// The generated struct is registered in the compile-time task registry using the
/// source location (`file:line:column`) as its stable `task_name`.
///
/// # Usage
///
/// ```ignore
/// // Map — fn(T) -> U  (return type required)
/// rdd.map_task(task_fn!(|x: i32| -> i32 { x * 2 }))
///
/// // Filter — fn(T) -> bool  (return type required)
/// rdd.filter_task(task_fn!(|x: i32| -> bool { x > 0 }))
///
/// // FlatMap — fn(T) -> Vec<U>  (return type required)
/// rdd.flat_map_task(task_fn!(|x: i32| -> Vec<i32> { vec![x, -x] }))
///
/// // Fold — fn(T, T) -> T  (return type inferred from first arg)
/// rdd.fold_task(0i32, task_fn!(|a: i32, b: i32| a + b))
/// ```
///
/// # Equivalence with `#[task]`
///
/// `task_fn!(|x: i32| -> i32 { x * 2 })` generates the same `UnaryTask<i32, i32>` as:
/// ```ignore
/// #[task] fn double(x: i32) -> i32 { x * 2 }
/// ```
/// Both are dispatched identically on workers.
pub(crate) fn expand(input: TokenStream) -> TokenStream {
    let TaskFnInput { captures, closure } = parse_macro_input!(input as TaskFnInput);
    let cap_names: Vec<&Ident> = captures.iter().map(|c| &c.name).collect();
    let cap_types: Vec<&Type> = captures.iter().map(|c| &c.ty).collect();
    let has_captures = !captures.is_empty();

    let inputs = &closure.inputs;
    let num_inputs = inputs.len();

    // Capture lists are only meaningful on unary (map/filter/flat_map) closures. A binary
    // (fold/reduce) op already uses `payload` for its zero value, so it cannot also carry
    // captured parameters there.
    if has_captures && num_inputs == 2 {
        return TokenStream::from(quote! {
            compile_error!("task_fn! capture lists are only supported on unary closures")
        });
    }

    // Extract (pat, ty) pairs from typed closure args.
    // Each arg must be `pat: Type` (Pat::Type).
    let typed_args: Vec<(proc_macro2::TokenStream, proc_macro2::TokenStream)> = inputs
        .iter()
        .map(|pat| match pat {
            syn::Pat::Type(pt) => {
                let p = &*pt.pat;
                let t = &*pt.ty;
                (quote! { #p }, quote! { #t })
            }
            other => (quote! { #other }, quote! { _ }),
        })
        .collect();

    let body = &closure.body;

    let struct_ident = syn::Ident::new("__TaskFnStruct", Span::call_site());
    let dispatch_fn_ident = syn::Ident::new("__task_fn_dispatch", Span::call_site());

    // ── capture-derived codegen (unary path) ──
    // A zero-capture closure keeps the original zero-sized-struct shape; a capturing one
    // becomes a struct whose fields are the captured values, rkyv-encoded into the op
    // payload at the call site and decoded on the worker before the body runs.
    let cap_struct_decl = if has_captures {
        quote! {
            #[allow(non_camel_case_types)]
            #[derive(Clone)]
            struct #struct_ident { #(#cap_names: #cap_types),* }
        }
    } else {
        quote! {
            #[allow(non_camel_case_types)]
            #[derive(Clone)]
            struct #struct_ident;
        }
    };
    let cap_construct = if has_captures {
        quote! { #struct_ident { #(#cap_names),* } }
    } else {
        quote! { #struct_ident }
    };
    let cap_encode_params = if has_captures {
        quote! {
            fn encode_params(&self) -> ::std::vec::Vec<u8> {
                ::atomic_compute::__macro_support::WireEncode::encode_wire(
                    &( #(self.#cap_names.clone(),)* )
                )
                .unwrap_or_default()
            }
        }
    } else {
        quote! {}
    };
    let cap_decode_instance = if has_captures {
        quote! {
            let ( #(#cap_names,)* ): ( #(#cap_types,)* ) =
                ::atomic_compute::__macro_support::WireDecode::decode_wire(payload)
                    .map_err(|e| e.to_string())?;
            let __task = #struct_ident { #(#cap_names),* };
        }
    } else {
        quote! {
            let _ = payload;
            let __task = #struct_ident;
        }
    };
    let cap_body = quote! {
        #(let #cap_names = self.#cap_names.clone();)*
        #body
    };

    //
    // Format: "task_fn::{module_path}::{Action}<{types}>::{short_hash}"
    //
    // Components:
    //   module_path  — from module_path!() at the call site; stable to line/column
    //                  changes and reformatting; changes only on module reorganisation.
    //   Action       — derived from the closure signature: Map / Filter / FlatMap / Reduce.
    //   types        — comma-separated input/output type names (whitespace-normalised).
    //   short_hash   — 8-hex FNV-1a of the BODY tokens only; disambiguates two closures
    //                  with the same module + action + types but different logic.
    //
    // Stability properties:
    //   ✓  Line-number changes (adding code above/below)
    //   ✓  rustfmt / reformatting
    //   ✓  File rename within same module structure
    //   ✗  Moving to a different module (intentional — that IS a different location)
    //   ✗  Changing the closure body (intentional — short_hash catches this)
    //
    // Duplicate bodies: two closures with identical bodies in the same module at the
    // same action+types share the same task_name. This is safe — their handlers are
    // functionally identical and the registry deduplicates them at startup.

    // Hash only the body, not the full closure, so argument names (x vs item) and
    // argument patterns don't affect the id — only the actual logic does.
    let (short_hash, body_hash) = body_hash_parts(&quote! { #body }.to_string());

    // Normalise a type token stream to a compact string: remove whitespace.
    let normalise_ty = |ts: &proc_macro2::TokenStream| -> String {
        ts.to_string()
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect()
    };

    // Return-type shape, computed once and reused both for the task_name (below) and to
    // pick the unary dispatch arms further down — only meaningful when num_inputs != 2.
    let is_bool_ret = matches!(&closure.output, ReturnType::Type(_, ty) if is_bool_type(ty));
    let is_vec_ret = matches!(&closure.output, ReturnType::Type(_, ty) if is_vec_type(ty));

    // Determine Action label and type string from the signature.
    let (action_label, types_str): (String, String) = if num_inputs == 2 {
        let (_, t) = &typed_args[0];
        ("Reduce".to_owned(), normalise_ty(t))
    } else {
        let (_, t) = &typed_args[0];
        let input_ty = normalise_ty(t);
        match &closure.output {
            ReturnType::Type(_, ret_ty) => {
                let ret_ts = quote! { #ret_ty };
                let ret_str = normalise_ty(&ret_ts);
                if is_bool_ret {
                    ("Filter".to_owned(), input_ty)
                } else if is_vec_ret {
                    ("FlatMap".to_owned(), format!("{input_ty},{ret_str}"))
                } else {
                    ("Map".to_owned(), format!("{input_ty},{ret_str}"))
                }
            }
            ReturnType::Default => ("Map".to_owned(), input_ty),
        }
    };

    // The full task_name is built at compile time using module_path!() so it picks up the
    // correct module at the call site, not in the macro crate itself.
    let op_id_suffix = format!("{action_label}<{types_str}>::{short_hash}");
    let op_id_expr = quote! {
        concat!(module_path!(), "::task_fn::", #op_id_suffix)
    };

    if num_inputs == 2 {
        // Binary fn(T, T) -> T → BinaryTask<T>
        // Return type is the same as the first arg type.
        let (pat0, t) = &typed_args[0];
        let (pat1, _) = &typed_args[1];

        TokenStream::from(quote! {
            {
                #[allow(non_camel_case_types)]
                #[derive(Clone)]
                struct #struct_ident;

                impl ::atomic_compute::__macro_support::BinaryTask<#t> for #struct_ident {
                    const NAME: &'static str = #op_id_expr;
                    fn call(&self, #pat0: #t, #pat1: #t) -> #t {
                        #body
                    }
                }

                #[doc(hidden)]
                fn #dispatch_fn_ident(
                    action: &::atomic_compute::__macro_support::TaskAction,
                    payload: &[u8],
                    data: &[u8],
                ) -> ::std::result::Result<::std::vec::Vec<u8>, ::std::string::String> {
                    use ::atomic_compute::__macro_support::{
                        BinaryTask, TaskAction, WireDecode, WireEncode,
                    };
                    let __task = #struct_ident;
                    match action {
                        TaskAction::Fold | TaskAction::Aggregate => {
                            let zero = <#t>::decode_wire(payload).map_err(|e| e.to_string())?;
                            let items = ::std::vec::Vec::<#t>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let result = items
                                .into_iter()
                                .fold(zero, |acc, x| __task.call(acc, x));
                            result.encode_wire().map_err(|e| e.to_string())
                        }
                        TaskAction::Reduce => {
                            let items = ::std::vec::Vec::<#t>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let mut iter = items.into_iter();
                            let first = iter
                                .next()
                                .ok_or_else(|| "reduce on empty partition".to_string())?;
                            let result = iter.fold(first, |acc, x| {
                                __task.call(acc, x)
                            });
                            result.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(::std::format!(
                            "task_fn (binary) does not support action {:?}", other
                        )),
                    }
                }

                ::atomic_compute::__macro_support::inventory::submit! {
                    ::atomic_compute::__macro_support::TaskEntry {
                        task_name: #op_id_expr,
                        body_hash: #body_hash,
                        handler: #dispatch_fn_ident,
                    }
                }

                #struct_ident
            }
        })
    } else {
        // Unary fn(T) -> U → UnaryTask<T, U>
        // Return type must be explicitly annotated on the closure.
        let (pat0, t) = &typed_args[0];

        let ret_type: proc_macro2::TokenStream = match &closure.output {
            ReturnType::Type(_, ty) => quote! { #ty },
            ReturnType::Default => {
                return TokenStream::from(quote! {
                    compile_error!(
                        "task_fn! unary closures require an explicit return type: `|x: T| -> U { … }`"
                    )
                });
            }
        };

        // is_bool_ret/is_vec_ret (computed above) pick the dispatch shape: bool → Filter,
        // Vec<_> → FlatMap, else Map.
        let dispatch_arms = if is_bool_ret {
            quote! {
                TaskAction::Map | TaskAction::Collect => {
                    let items = ::std::vec::Vec::<#t>::decode_wire(data).map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<bool> = items.into_iter()
                        .map(|x| __task.call(x))
                        .collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                TaskAction::Filter => {
                    let items = ::std::vec::Vec::<#t>::decode_wire(data).map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<#t> = items.into_iter()
                        .filter(|x| __task.call(x.clone()))
                        .collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                other => Err(::std::format!("task_fn (predicate) does not support action {:?}", other)),
            }
        } else if is_vec_ret {
            quote! {
                TaskAction::Map | TaskAction::Collect => {
                    let items = ::std::vec::Vec::<#t>::decode_wire(data).map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<#ret_type> = items.into_iter()
                        .map(|x| __task.call(x))
                        .collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                TaskAction::FlatMap => {
                    let items = ::std::vec::Vec::<#t>::decode_wire(data).map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<_> = items.into_iter()
                        .flat_map(|x| __task.call(x))
                        .collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                TaskAction::MapPartitions => {
                    let items = <#t>::decode_wire(data).map_err(|e| e.to_string())?;
                    let result = __task.call(items);
                    result.encode_wire().map_err(|e| e.to_string())
                }
                other => Err(::std::format!("task_fn (vec) does not support action {:?}", other)),
            }
        } else {
            quote! {
                TaskAction::Map | TaskAction::Collect => {
                    let items = ::std::vec::Vec::<#t>::decode_wire(data).map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<#ret_type> = items.into_iter()
                        .map(|x| __task.call(x))
                        .collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                TaskAction::Foreach => {
                    let items = ::std::vec::Vec::<#t>::decode_wire(data).map_err(|e| e.to_string())?;
                    for x in items {
                        __task.call(x);
                    }
                    ::std::result::Result::Ok(::std::vec::Vec::new())
                }
                other => Err(::std::format!("task_fn (unary) does not support action {:?}", other)),
            }
        };

        TokenStream::from(quote! {
            {
                #cap_struct_decl

                impl ::atomic_compute::__macro_support::UnaryTask<#t, #ret_type> for #struct_ident {
                    const NAME: &'static str = #op_id_expr;
                    fn call(&self, #pat0: #t) -> #ret_type {
                        #cap_body
                    }
                    #cap_encode_params
                }

                #[doc(hidden)]
                fn #dispatch_fn_ident(
                    action: &::atomic_compute::__macro_support::TaskAction,
                    payload: &[u8],
                    data: &[u8],
                ) -> ::std::result::Result<::std::vec::Vec<u8>, ::std::string::String> {
                    use ::atomic_compute::__macro_support::{
                        TaskAction, UnaryTask, WireDecode, WireEncode,
                    };
                    #cap_decode_instance
                    match action {
                        #dispatch_arms
                    }
                }

                ::atomic_compute::__macro_support::inventory::submit! {
                    ::atomic_compute::__macro_support::TaskEntry {
                        task_name: #op_id_expr,
                        body_hash: #body_hash,
                        handler: #dispatch_fn_ident,
                    }
                }

                #cap_construct
            }
        })
    }
}
