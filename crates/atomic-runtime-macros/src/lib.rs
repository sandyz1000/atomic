use heck::ToUpperCamelCase;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{ExprClosure, ItemFn, LitStr, ReturnType, Type, parse_macro_input};

/// Attribute macro for defining a distributed Atomic task function.
///
/// Annotating a function with `#[task]` does three things at compile time:
///
/// 1. **Preserves the original function** exactly as written — it can still be
///    called directly in local mode, tests, or anywhere in the same binary.
///
/// 2. **Generates a zero-sized task struct** (PascalCase of the function name) that
///    implements `UnaryTask` or `BinaryTask` from `atomic_compute::__macro_support`.
///    The struct carries `const NAME: &'static str` matching the inventory registration,
///    so the op-id is available statically at the call site — inspired by rusty-celery.
///
/// 3. **Registers a dispatch handler** into the global compile-time task registry
///    via the `inventory` crate. When the binary runs as a worker, incoming
///    `TaskEnvelope` messages are dispatched to the handler by `op_id`.
///
/// # Generated names
///
/// | Function | Task struct |
/// |---|---|
/// | `fn double` | `struct Double` |
/// | `fn is_positive` | `struct IsPositive` |
/// | `fn flat_map_words` | `struct FlatMapWords` |
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
/// # Usage in TypedRdd
///
/// ```ignore
/// ctx.parallelize_typed(data, 2).map_task(Double).collect()?;
/// ctx.parallelize_typed(data, 2).fold_task(0i32, Add)?;
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

    // PascalCase struct name: double → Double, is_positive → IsPositive
    let struct_name_str = fn_name_str.to_upper_camel_case();
    let struct_name = syn::Ident::new(&struct_name_str, Span::call_site());

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
    let op_id_expr: proc_macro2::TokenStream = if let Some(ref name) = custom_name {
        quote! { #name }
    } else {
        quote! { concat!(module_path!(), "::", #fn_name_str) }
    };

    // ── Generate dispatch arms based on function signature ────────────────────
    let dispatch_body = if num_args == 2 {
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

    // ── Generate the task struct + trait impl ─────────────────────────────────
    //
    // Binary fn(T, T) -> T  →  BinaryTask<T>
    // Unary fn(T) -> U      →  UnaryTask<T, U>
    let task_struct_impl = if num_args == 2 {
        quote! {
            /// Zero-sized task struct for [`#fn_name`]. Implements
            /// `::atomic_compute::__macro_support::BinaryTask`.
            #[allow(non_camel_case_types)]
            #fn_vis struct #struct_name;

            impl ::atomic_compute::__macro_support::BinaryTask<#input_type> for #struct_name {
                const NAME: &'static str = #op_id_expr;
                fn call(a: #input_type, b: #input_type) -> #input_type {
                    #fn_name(a, b)
                }
            }
        }
    } else {
        quote! {
            /// Zero-sized task struct for [`#fn_name`]. Implements
            /// `::atomic_compute::__macro_support::UnaryTask`.
            #[allow(non_camel_case_types)]
            #fn_vis struct #struct_name;

            impl ::atomic_compute::__macro_support::UnaryTask<#input_type, #output_type>
                for #struct_name
            {
                const NAME: &'static str = #op_id_expr;
                fn call(input: #input_type) -> #output_type {
                    #fn_name(input)
                }
            }
        }
    };

    TokenStream::from(quote! {
        // 1. Preserve the original function unchanged — callable directly anywhere.
        #(#fn_attrs)*
        #fn_vis #fn_sig {
            #fn_block
        }

        // 2. Zero-sized task struct with statically-known NAME.
        //    Inspired by rusty-celery: the struct is what you pass to map_task / fold_task,
        //    giving the RDD API access to the op_id without runtime lookup.
        #task_struct_impl

        // 3. Dispatch handler: decodes partition bytes, applies the requested action,
        //    re-encodes the result. Called by NativeBackend when op_id matches.
        #[doc(hidden)]
        fn #dispatch_fn_name(
            action: &::atomic_compute::__macro_support::TaskAction,
            payload: &[u8],
            data: &[u8],
        ) -> ::std::result::Result<::std::vec::Vec<u8>, ::std::string::String> {
            #dispatch_body
        }

        // 4. Compile-time registration via inventory.
        //    The worker binary collects all submitted entries at startup.
        ::atomic_compute::__macro_support::inventory::submit! {
            ::atomic_compute::__macro_support::TaskEntry {
                op_id: #op_id_expr,
                handler: #dispatch_fn_name,
            }
        }
    })
}

/// Wrap an inline non-capturing closure into a zero-sized task struct that implements
/// `UnaryTask` or `BinaryTask`, enabling it to run on distributed workers.
///
/// Arguments **must be explicitly typed**. For unary closures, the return type
/// **must be annotated** with `-> ReturnType` so the dispatch handler can be generated.
/// Binary (fold) closures infer the return type from the first argument type.
///
/// The generated struct is registered in the compile-time task registry using the
/// source location (`file:line:column`) as its stable `op_id`.
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
#[proc_macro]
pub fn task_fn(input: TokenStream) -> TokenStream {
    let closure = parse_macro_input!(input as ExprClosure);

    let inputs = &closure.inputs;
    let num_inputs = inputs.len();

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

    // Stable unique struct ident — proc_macro2 gives us only call_site span,
    // but the inventory key is the op_id string which uses concat!(file!, line!, col!).
    let struct_ident = syn::Ident::new("__TaskFnStruct", Span::call_site());
    let dispatch_fn_ident = syn::Ident::new("__task_fn_dispatch", Span::call_site());
    let op_id_expr = quote! { concat!(file!(), ":", line!(), ":", column!()) };

    if num_inputs == 2 {
        // Binary fn(T, T) -> T → BinaryTask<T>
        // Return type is the same as the first arg type.
        let (pat0, t) = &typed_args[0];
        let (pat1, _) = &typed_args[1];

        TokenStream::from(quote! {
            {
                #[allow(non_camel_case_types)]
                struct #struct_ident;

                impl ::atomic_compute::__macro_support::BinaryTask<#t> for #struct_ident {
                    const NAME: &'static str = #op_id_expr;
                    fn call(#pat0: #t, #pat1: #t) -> #t {
                        #body
                    }
                }

                #[doc(hidden)]
                fn #dispatch_fn_ident(
                    action: &::atomic_compute::__macro_support::TaskAction,
                    payload: &[u8],
                    data: &[u8],
                ) -> ::std::result::Result<::std::vec::Vec<u8>, ::std::string::String> {
                    use ::atomic_compute::__macro_support::{TaskAction, WireDecode, WireEncode};
                    match action {
                        TaskAction::Fold | TaskAction::Aggregate => {
                            let zero = <#t>::decode_wire(payload).map_err(|e| e.to_string())?;
                            let items = ::std::vec::Vec::<#t>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let result = items
                                .into_iter()
                                .fold(zero, |acc, x| <#struct_ident as ::atomic_compute::__macro_support::BinaryTask<#t>>::call(acc, x));
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
                                <#struct_ident as ::atomic_compute::__macro_support::BinaryTask<#t>>::call(acc, x)
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
                        op_id: #op_id_expr,
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

        // Detect if return type is bool → Filter dispatch; Vec<_> → FlatMap; else Map.
        let is_bool = match &closure.output {
            ReturnType::Type(_, ty) => {
                if let Type::Path(tp) = ty.as_ref() { tp.path.is_ident("bool") } else { false }
            }
            _ => false,
        };
        let is_vec = match &closure.output {
            ReturnType::Type(_, ty) => {
                if let Type::Path(tp) = ty.as_ref() {
                    tp.path.segments.last().map(|s| s.ident == "Vec").unwrap_or(false)
                } else { false }
            }
            _ => false,
        };

        let dispatch_arms = if is_bool {
            quote! {
                TaskAction::Map | TaskAction::Collect => {
                    let items = ::std::vec::Vec::<#t>::decode_wire(data).map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<bool> = items.into_iter()
                        .map(|x| <#struct_ident as ::atomic_compute::__macro_support::UnaryTask<#t, #ret_type>>::call(x))
                        .collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                TaskAction::Filter => {
                    let items = ::std::vec::Vec::<#t>::decode_wire(data).map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<#t> = items.into_iter()
                        .filter(|x| <#struct_ident as ::atomic_compute::__macro_support::UnaryTask<#t, #ret_type>>::call(x.clone()))
                        .collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                other => Err(::std::format!("task_fn (predicate) does not support action {:?}", other)),
            }
        } else if is_vec {
            quote! {
                TaskAction::Map | TaskAction::Collect => {
                    let items = ::std::vec::Vec::<#t>::decode_wire(data).map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<#ret_type> = items.into_iter()
                        .map(|x| <#struct_ident as ::atomic_compute::__macro_support::UnaryTask<#t, #ret_type>>::call(x))
                        .collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                TaskAction::FlatMap => {
                    let items = ::std::vec::Vec::<#t>::decode_wire(data).map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<_> = items.into_iter()
                        .flat_map(|x| <#struct_ident as ::atomic_compute::__macro_support::UnaryTask<#t, #ret_type>>::call(x))
                        .collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                other => Err(::std::format!("task_fn (vec) does not support action {:?}", other)),
            }
        } else {
            quote! {
                TaskAction::Map | TaskAction::Collect => {
                    let items = ::std::vec::Vec::<#t>::decode_wire(data).map_err(|e| e.to_string())?;
                    let result: ::std::vec::Vec<#ret_type> = items.into_iter()
                        .map(|x| <#struct_ident as ::atomic_compute::__macro_support::UnaryTask<#t, #ret_type>>::call(x))
                        .collect();
                    result.encode_wire().map_err(|e| e.to_string())
                }
                other => Err(::std::format!("task_fn (unary) does not support action {:?}", other)),
            }
        };

        TokenStream::from(quote! {
            {
                #[allow(non_camel_case_types)]
                struct #struct_ident;

                impl ::atomic_compute::__macro_support::UnaryTask<#t, #ret_type> for #struct_ident {
                    const NAME: &'static str = #op_id_expr;
                    fn call(#pat0: #t) -> #ret_type {
                        #body
                    }
                }

                #[doc(hidden)]
                fn #dispatch_fn_ident(
                    action: &::atomic_compute::__macro_support::TaskAction,
                    payload: &[u8],
                    data: &[u8],
                ) -> ::std::result::Result<::std::vec::Vec<u8>, ::std::string::String> {
                    use ::atomic_compute::__macro_support::{TaskAction, WireDecode, WireEncode};
                    let _ = payload;
                    match action {
                        #dispatch_arms
                    }
                }

                ::atomic_compute::__macro_support::inventory::submit! {
                    ::atomic_compute::__macro_support::TaskEntry {
                        op_id: #op_id_expr,
                        handler: #dispatch_fn_ident,
                    }
                }

                #struct_ident
            }
        })
    }
}
