use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, ItemFn, ReturnType, Type};

/// Attribute macro for defining an Atomic task function.
///
/// Expands differently depending on the compile target:
///
/// - **`wasm32`**: generates `alloc`/`dealloc` C ABI exports, a bump allocator,
///   and an exported C ABI entrypoint that decodes the input, calls the user
///   function, encodes the result, and packs (result_ptr << 32 | result_len) i64.
///
/// - **native (Docker / local)**: generates `fn main()` that runs a
///   stdin/stdout framing loop (`serve` or `serve_json`).
///
/// # Codec options
///
/// `#[task]` — default rkyv codec. Input/output types must derive
/// `rkyv::Archive`, `rkyv::Serialize`, `rkyv::Deserialize`.
///
/// `#[task(json)]` — JSON codec via serde_json. Input/output types must
/// derive `serde::Deserialize` / `serde::Serialize`. Use when the task
/// needs to be callable from Python or JavaScript.
///
/// # Example — rkyv (Rust-native, any rkyv type)
/// ```ignore
/// #[atomic_runtime::task]
/// fn run_word_count(words: Vec<String>) -> Vec<(String, u32)> {
///     let mut map = std::collections::BTreeMap::new();
///     for w in words { *map.entry(w).or_insert(0u32) += 1; }
///     map.into_iter().collect()
/// }
/// ```
///
/// # Example — json (cross-language, callable from Python/JS)
/// ```ignore
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Input { words: Vec<String> }
///
/// #[derive(Serialize)]
/// struct Output { counts: Vec<(String, u32)> }
///
/// #[atomic_runtime::task(json)]
/// fn run_word_count(input: Input) -> Output {
///     let mut map = std::collections::BTreeMap::new();
///     for w in input.words { *map.entry(w).or_insert(0u32) += 1; }
///     Output { counts: map.into_iter().collect() }
/// }
/// ```
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    // Detect `json` flag in attribute args (e.g. `#[task(json)]`).
    let attr_str = attr.to_string();
    let use_json = attr_str.contains("json");

    let fn_name = &input.sig.ident;
    let fn_name_impl = syn::Ident::new(&format!("{}_impl", fn_name), Span::call_site());
    let fn_vis = &input.vis;
    let fn_inputs = &input.sig.inputs;
    let fn_block = &input.block;

    // Extract the single argument type (the partition input type).
    let input_type: proc_macro2::TokenStream = fn_inputs
        .iter()
        .next()
        .and_then(|arg| match arg {
            syn::FnArg::Typed(pat_type) => Some(*pat_type.ty.clone()),
            _ => None,
        })
        .map(|ty| quote! { #ty })
        .unwrap_or_else(|| quote! { Vec<u8> });

    // Extract the return type.
    let output_type: proc_macro2::TokenStream = match &input.sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => {
            let ty: &Type = ty;
            quote! { #ty }
        }
    };

    // Detect compile target at macro expansion time.
    let is_wasm = std::env::var("CARGO_CFG_TARGET_ARCH")
        .map(|arch| arch == "wasm32")
        .unwrap_or(false);

    if is_wasm {
        // ── WASM target ───────────────────────────────────────────────────────
        // Generate:
        //   1. The user function renamed to `{name}_impl`
        //   2. A static bump allocator (alloc / dealloc exports)
        //   3. The C ABI entrypoint dispatching to call_wasm or call_wasm_json
        let call_fn = if use_json {
            quote! { ::atomic_runtime::__internal::call_wasm_json::<#input_type, #output_type, _> }
        } else {
            quote! { ::atomic_runtime::__internal::call_wasm::<#input_type, #output_type, _> }
        };

        TokenStream::from(quote! {
            fn #fn_name_impl(#fn_inputs) -> #output_type {
                #fn_block
            }

            // Static bump allocator — required by the wasmtime host to write
            // input bytes into linear memory before calling the entrypoint.
            static __ATOMIC_HEAP: ::core::sync::atomic::AtomicUsize =
                ::core::sync::atomic::AtomicUsize::new(65536);

            #[no_mangle]
            pub unsafe extern "C" fn alloc(len: i32) -> i32 {
                __ATOMIC_HEAP.fetch_add(
                    len as usize,
                    ::core::sync::atomic::Ordering::SeqCst,
                ) as i32
            }

            #[no_mangle]
            pub unsafe extern "C" fn dealloc(_ptr: i32, _len: i32) {}

            #[no_mangle]
            pub unsafe extern "C" fn #fn_name(ptr: i32, len: i32) -> i64 {
                #call_fn(ptr, len, #fn_name_impl)
            }
        })
    } else {
        // ── Native target (Docker / local binary) ─────────────────────────────
        // Generate:
        //   1. The user function renamed to `{name}_impl`
        //   2. `fn main()` calling serve or serve_json
        let serve_call = if use_json {
            quote! { ::atomic_runtime::serve_json(#fn_name_impl); }
        } else {
            quote! { ::atomic_runtime::serve(#fn_name_impl); }
        };

        TokenStream::from(quote! {
            #fn_vis fn #fn_name_impl(#fn_inputs) -> #output_type {
                #fn_block
            }

            fn main() {
                #serve_call
            }
        })
    }
}
