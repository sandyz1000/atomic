use atomic_runtime::task;
use serde::{Deserialize, Serialize};

/// Input/output types are plain serde structs — no rkyv derives needed.
/// Python and JavaScript callers can produce and consume these as JSON naturally.
///
/// # How the JSON codec works
///
/// `#[task(json)]` generates a C ABI entrypoint (on wasm32) or `fn main()` (native)
/// that uses serde_json instead of rkyv for serialization:
///   - Input bytes are parsed with `serde_json::from_slice`
///   - Output is serialized with `serde_json::to_vec`
///
/// This means any caller that can produce JSON can invoke this task — Python
/// dictionaries, JavaScript objects, curl, etc. No rkyv dependency on the caller.
///
/// # Build
/// ```bash
/// cargo build --release \
///   --manifest-path examples/wasm_json_task/Cargo.toml \
///   --target wasm32-wasip2
/// ```
///
/// # Call from Python
/// ```python
/// import atomic
/// ctx = atomic.Context(default_parallelism=2)
/// stub = atomic.DockerStub.from_manifest("manifest.toml", "demo.wc.json.v1")
/// result = ctx.parallelize([
///     {"words": ["hello", "world", "hello"]},
///     {"words": ["atomic", "is", "fast"]},
/// ]).map_via(stub)
/// ```

#[derive(Deserialize)]
struct WordCountInput {
    words: Vec<String>,
}

#[derive(Serialize)]
struct WordCountOutput {
    counts: Vec<(String, u32)>,
}

/// Count the frequency of each word in the input list.
///
/// Returns a sorted list of (word, count) pairs — serialized as JSON so
/// Python and JavaScript clients can read the result directly.
#[task(json)]
fn run_word_count(input: WordCountInput) -> WordCountOutput {
    let mut map = std::collections::BTreeMap::new();
    for w in input.words {
        *map.entry(w).or_insert(0u32) += 1;
    }
    WordCountOutput {
        counts: map.into_iter().collect(),
    }
}
