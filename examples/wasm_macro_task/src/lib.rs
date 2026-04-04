use atomic_runtime::task;

/// Word-frequency count over a partition of words.
///
/// This demonstrates that `#[task]` works with any rkyv-serializable type —
/// not just `Vec<u8>`. The input is a `Vec<String>` (a list of words) and the
/// output is a `Vec<(String, u32)>` (word → count pairs), both serialized
/// transparently by rkyv. No manual byte packing is required.
///
/// Valid input / output types for `#[task]`:
///   - Primitive integers and floats: u8, i32, f64, …
///   - String, bool
///   - Vec<T>, Option<T>, tuples, arrays — when T is also rkyv-serializable
///   - Custom structs / enums annotated with
///       #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
///
/// On `wasm32` targets the macro generates:
///   - A static bump allocator (`alloc` / `dealloc` C ABI exports)
///   - A C ABI entrypoint that rkyv-decodes the input from linear memory,
///     calls this function, rkyv-encodes the result, and returns a packed
///     `(result_ptr << 32 | result_len)` i64
///
/// Build:
///   cargo run -p atomic-cli -- wasm build \
///     --source examples/wasm_macro_task \
///     --out    examples/wasm_macro_task/build \
///     --map_op_id      demo.wc.v1 \
///     --map_entrypoint run_word_count
///
/// NOTE: `#[task]` may only appear once per compilation unit because it emits
/// the `alloc` / `dealloc` symbols. For multiple entrypoints write separate
/// cdylib crates or use the manual C ABI approach in `examples/rust_wasm_task`.
#[task]
fn run_word_count(words: Vec<String>) -> Vec<(String, u32)> {
    let mut map = std::collections::BTreeMap::new();
    for w in words {
        *map.entry(w).or_insert(0u32) += 1;
    }
    map.into_iter().collect()
}
