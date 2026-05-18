/// Thread-local V8 JavaScript runtime for UDF execution (deno_core).
///
/// One [`deno_core::JsRuntime`] is kept per tokio blocking thread.  Because V8
/// isolates are `!Send`, thread-local storage is the correct placement and also
/// avoids any synchronisation cost on the hot path.
///
/// Each thread pays the V8 cold-start cost exactly once; subsequent tasks on
/// the same thread reuse the warm runtime at negligible overhead.
///
/// # Function compilation cache
///
/// UDF functions (e.g. `"x => x * 2"`) are compiled into `globalThis.__f<hash>`
/// on first use and referenced by name on all subsequent partitions.  V8 parses
/// each distinct function expression exactly once per thread rather than once per
/// partition call.  [`FN_CACHE`] tracks which functions have been compiled.
///
/// # Context capture
///
/// The caller may pass a `context_json` string (a JSON object).  It is injected
/// as `globalThis.__ctx` before the UDF script runs, allowing functions written
/// as `(x, ctx) => x > ctx.threshold` to access driver-side values without
/// closure serialization.
#[cfg(feature = "js-v8")]
use std::cell::RefCell;
#[cfg(feature = "js-v8")]
use std::collections::HashMap;

#[cfg(feature = "js-v8")]


// ── Thread-local state ────────────────────────────────────────────────────────

#[cfg(feature = "js-v8")]
thread_local! {
    static JS_RT: RefCell<Option<deno_core::JsRuntime>> = RefCell::new(None);
    /// FxHash64(fn_source) → JS globalThis identifier (e.g. "__f0a1b2c3d4e5f678").
    /// Entries persist for the thread's lifetime; valid as long as JS_RT is Some.
    static FN_CACHE: RefCell<HashMap<u64, String>> = RefCell::new(HashMap::new());
}

#[cfg(feature = "js-v8")]
fn with_js_runtime<F, R>(f: F) -> R
where
    F: FnOnce(&mut deno_core::JsRuntime) -> R,
{
    JS_RT.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            *borrow = Some(deno_core::JsRuntime::new(deno_core::RuntimeOptions::default()));
        }
        f(borrow.as_mut().unwrap())
    })
}

/// Force initialization of the thread-local [`JsRuntime`] on the calling thread.
///
/// Call this from `spawn_blocking` tasks at worker startup to amortize the V8
/// cold-start cost before the first real UDF task arrives.
#[cfg(feature = "js-v8")]
pub fn warmup_js_runtime() {
    with_js_runtime(|_| {});
}

// ── Function compilation cache ────────────────────────────────────────────────

/// Compile `fn_source` into `globalThis.__f<hash>` on first call per thread.
///
/// Returns the JS identifier to use in place of the raw function source.
/// On subsequent calls with the same source, returns the cached identifier
/// without re-running any script.
#[cfg(feature = "js-v8")]
fn ensure_fn_compiled(
    rt: &mut deno_core::JsRuntime,
    fn_source: &str,
) -> Result<String, String> {
    use std::hash::{Hash, Hasher};
    let mut hasher = rustc_hash::FxHasher::default();
    fn_source.hash(&mut hasher);
    let key = hasher.finish();

    // Fast path — already compiled on this thread.
    if let Some(js_name) = FN_CACHE.with(|c| c.borrow().get(&key).cloned()) {
        return Ok(js_name);
    }

    // Slow path — first time seeing this fn_source on this thread.
    let js_name = format!("__f{key:016x}");
    rt.execute_script(
        "<fn_init>",
        format!("globalThis.{js_name} = ({fn_source});"),
    )
    .map_err(|e| format!("fn compile: {e}"))?;

    FN_CACHE.with(|c| c.borrow_mut().insert(key, js_name.clone()));
    Ok(js_name)
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Evaluate a UDF partition operation using the thread-local V8 runtime.
///
/// - `fn_source` — JavaScript function expression (from `fn.toString()` on the driver).
///   May reference `__ctx` for captured driver values.
/// - `context_json` — optional JSON object injected as `globalThis.__ctx`.
/// - `data_str` — JSON-encoded `Vec` of partition elements (the current partition).
/// - `zero_json` — JSON-encoded fold zero value; empty string for non-fold ops.
///
/// Returns JSON-encoded result bytes.
#[cfg(feature = "js-v8")]
pub fn eval_partition(
    fn_source: &str,
    context_json: Option<&str>,
    data_str: &str,
) -> Result<Vec<u8>, String> {
    with_js_runtime(|rt| {
        // 1. Inject driver context (or clear to avoid stale values from prior tasks).
        let ctx_js = match context_json {
            Some(ctx) => format!("globalThis.__ctx = {ctx};"),
            None => "globalThis.__ctx = undefined;".to_string(),
        };
        rt.execute_script("<ctx>", ctx_js)
            .map_err(|e| format!("ctx inject: {e}"))?;

        // 2. Ensure the UDF function is compiled and cached; get its JS identifier.
        let fn_var = ensure_fn_compiled(rt, fn_source)?;

        // 3. Build and run the partition-level function (fn_source is already partition-level).
        let script = format!("JSON.stringify(({})({}));", fn_var, data_str);
        let result = rt
            .execute_script("<udf>", script)
            .map_err(|e| format!("JS eval: {e}"))?;

        // 5. Extract the string result from the V8 Global<Value>.
        // v8-147 typestate: ScopeStorage::init() via Pin::new_unchecked gives PinnedRef.
        // ContextScope layers a context on top; &**cs gives PinnedRef<HandleScope<()>>
        // for Local::new, and &*cs gives PinnedRef<HandleScope<Context>> for to_rust_string_lossy.
        let context = rt.main_context();
        let mut hs = deno_core::v8::HandleScope::new(rt.v8_isolate());
        // SAFETY: hs (ScopeStorage) is immediately shadowed; it stays on the stack, no moves.
        let mut hs = {
            let pinned = unsafe { std::pin::Pin::new_unchecked(&mut hs) };
            pinned.init()
        };
        let hs = &mut hs;
        let ctx_local = deno_core::v8::Local::new(&*hs, context);
        let mut cs = deno_core::v8::ContextScope::new(hs, ctx_local);
        let local = deno_core::v8::Local::new(&**cs, result);
        Ok(local.to_rust_string_lossy(&*cs).into_bytes())
    })
}

// ── Script builder ────────────────────────────────────────────────────────────

/// Build the evaluation script for `action`, referencing the pre-compiled
/// function via `fn_var` (a `globalThis.__f…` identifier) rather than
/// embedding the raw source — this lets V8 reuse the already-compiled function.
///
/// For single-element operations (Map, Filter, FlatMap, KeyBy, MapValues,
/// FlatMapValues) the UDF is called as `fn(x, globalThis.__ctx)` so that
/// `(x, ctx) => ...` signatures receive the injected driver context rather
/// than the array index that JS array methods pass as their second argument.
/// For binary operations (Reduce, Fold) the standard `(acc, x)` callback
/// convention applies and no ctx wrapper is needed.

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "js-v8"))]
mod tests {
    use super::*;

    #[test]
    fn map_doubles_integers() {
        let result = eval_partition("(partition) => partition.map(x => x * 2)", None, "[1,2,3]")
            .expect("eval ok");
        let parsed: Vec<i64> = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed, vec![2, 4, 6]);
    }

    #[test]
    fn filter_with_context_json() {
        let result = eval_partition(
            "(partition, ctx) => partition.filter(x => x > ctx.threshold)",
            Some(r#"{"threshold": 2}"#),
            "[1,2,3,4,5]",
        )
        .expect("eval ok");
        let parsed: Vec<i64> = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed, vec![3, 4, 5]);
    }

    #[test]
    fn thread_local_runtime_reused() {
        // Two evals on the same thread reuse the same JsRuntime — verified by
        // setting a globalThis variable in the first eval and reading it in the second.
        eval_partition("(partition) => partition.map(x => { globalThis.__marker = 99; return x; })", None, "[1]").unwrap();
        let r2 = eval_partition("(partition) => partition.map(x => globalThis.__marker)", None, "[0]").unwrap();
        let v2: Vec<i64> = serde_json::from_slice(&r2).unwrap();
        assert_eq!(v2, vec![99]);
    }

    #[test]
    fn fn_compiled_once_per_thread() {
        // Same fn_source evaluated across multiple partition calls — FN_CACHE should
        // have exactly one entry for "x => x + 1" after both calls.
        let fn_source = "x => x + 1";
        eval_partition(fn_source, None, "[10]").unwrap();
        eval_partition(fn_source, None, "[20]").unwrap();

        // Verify the function is in the cache (compiled exactly once).
        use std::hash::{Hash, Hasher};
        let mut hasher = rustc_hash::FxHasher::default();
        fn_source.hash(&mut hasher);
        let key = hasher.finish();
        let cached = FN_CACHE.with(|c| c.borrow().contains_key(&key));
        assert!(cached, "fn_source should be in FN_CACHE after first call");

        // Results must still be correct on the second call (reused compiled fn).
        let r = eval_partition(fn_source, None, "[99]").unwrap();
        let v: Vec<i64> = serde_json::from_slice(&r).unwrap();
        assert_eq!(v, vec![100]);
    }
}
