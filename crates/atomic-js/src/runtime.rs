use std::cell::RefCell;
use std::sync::Arc;

use rquickjs::{Array, Class, Context, Ctx, IntoJs, Runtime, Value};
use rquickjs::prelude::{Opt, Rest};

use crate::rdd::JsRdd;

// QuickJS is single-threaded, so thread_local is the right tool for sharing
// the compute context into named `'js` functions without lifetime gymnastics.
thread_local! {
    static ATOMIC_CTX: RefCell<Option<Arc<atomic_compute::context::Context>>> =
        RefCell::new(None);
}

fn take_atomic_ctx() -> Arc<atomic_compute::context::Context> {
    ATOMIC_CTX.with(|c| {
        c.borrow()
            .as_ref()
            .expect("atomic context not set; call eval_script only via AtomicJsRuntime")
            .clone()
    })
}

/// The Atomic JS runtime — embeds QuickJS and exposes the `atomic` global.
///
/// Users write `.js` files that call `atomic.parallelize(...)`, `.map(...)`, etc.
/// The runtime evaluates the script inside QuickJS. No Node.js, no subprocess.
///
/// # Example JS script
/// ```javascript
/// const rdd = atomic.parallelize([1, 2, 3, 4]);
///
/// const result = rdd
///     .map(x => x + 1)
///     .filter(x => x > 2)
///     .collect();
///
/// atomic.print(result);
/// // [3, 4, 5]
/// ```
pub struct AtomicJsRuntime {
    runtime: Runtime,
    context: Arc<atomic_compute::context::Context>,
}

impl AtomicJsRuntime {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let runtime = Runtime::new()?;
        runtime.set_memory_limit(512 * 1024 * 1024); // 512 MB
        runtime.set_max_stack_size(4 * 1024 * 1024); // 4 MB
        let context = atomic_compute::context::Context::new()
            .map_err(|e| format!("atomic context init: {e}"))?;
        Ok(Self { runtime, context })
    }

    /// Evaluate a JS script string in a fresh context with the `atomic` global.
    pub fn eval_script(&self, script: &str) -> rquickjs::Result<()> {
        // Install the compute context into the thread-local before entering QuickJS.
        ATOMIC_CTX.with(|c| *c.borrow_mut() = Some(Arc::clone(&self.context)));
        let ctx = Context::full(&self.runtime)?;
        ctx.with(|ctx| -> rquickjs::Result<()> {
            setup_atomic_global(&ctx)?;
            ctx.eval::<(), _>(script.to_string())?;
            Ok(())
        })
    }

    /// Load and run a `.js` file.
    pub fn run_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let script = std::fs::read_to_string(path)?;
        self.eval_script(&script).map_err(|e| format!("JS error: {:?}", e).into())
    }
}

impl Default for AtomicJsRuntime {
    fn default() -> Self {
        Self::new().expect("QuickJS runtime initialisation failed")
    }
}

// ── Named helpers — explicit `'js` lifetime is required by rquickjs ──────────

fn js_parallelize<'js>(
    ctx: Ctx<'js>,
    arr: Array<'js>,
    num_partitions: Opt<usize>,
) -> rquickjs::Result<Class<'js, JsRdd>> {
    let atomic_ctx = take_atomic_ctx();
    let np = num_partitions.0.unwrap_or(2).max(1);
    let elements: Vec<Value<'js>> = arr.iter::<Value>().collect::<rquickjs::Result<_>>()?;
    let rdd = JsRdd::from_js_values(&ctx, elements, np, atomic_ctx)?;
    Class::instance(ctx, rdd)
}

fn js_range<'js>(
    ctx: Ctx<'js>,
    start: i64,
    end: i64,
    step: Opt<i64>,
    num_partitions: Opt<usize>,
) -> rquickjs::Result<Class<'js, JsRdd>> {
    let atomic_ctx = take_atomic_ctx();
    let step_val = step.0.unwrap_or(1);
    let np = num_partitions.0.unwrap_or(2).max(1);
    if step_val == 0 {
        return Err(rquickjs::Exception::throw_range(&ctx, "step cannot be zero"));
    }
    let values: Vec<Value<'js>> = (start..end)
        .step_by(step_val.unsigned_abs() as usize)
        .map(|i| i.into_js(&ctx))
        .collect::<rquickjs::Result<_>>()?;
    let rdd = JsRdd::from_js_values(&ctx, values, np, atomic_ctx)?;
    Class::instance(ctx, rdd)
}

/// Install the `atomic` global object into `ctx`.
///
/// Exposes:
///   `atomic.parallelize(array, numPartitions?)` → Rdd
///   `atomic.range(start, end, step?, numPartitions?)` → Rdd
///   `atomic.DockerStub.fromManifest(path, operationId)` → DockerStub (placeholder)
///   `atomic.print(...args)` → prints to stdout
fn setup_atomic_global(ctx: &rquickjs::Ctx<'_>) -> rquickjs::Result<()> {
    // Register the JsRdd class
    Class::<JsRdd>::define(&ctx.globals())?;

    let atomic_obj = rquickjs::Object::new(ctx.clone())?;

    atomic_obj.set("parallelize", rquickjs::Function::new(ctx.clone(), js_parallelize)?)?;
    atomic_obj.set("range", rquickjs::Function::new(ctx.clone(), js_range)?)?;

    // atomic.print(...args) → stdout
    let print_fn = rquickjs::Function::new(ctx.clone(), |args: Rest<String>| {
        println!("{}", args.0.join(" "));
    })?;
    atomic_obj.set("print", print_fn)?;

    // atomic.DockerStub placeholder
    let docker_stub_obj = rquickjs::Object::new(ctx.clone())?;
    let from_manifest = rquickjs::Function::new(
        ctx.clone(),
        |ctx: Ctx<'_>, _path: String, _op: String| -> rquickjs::Result<()> {
            Err(rquickjs::Exception::throw_type(
                &ctx,
                "DockerStub.fromManifest: not yet implemented in atomic-js",
            ))
        },
    )?;
    docker_stub_obj.set("fromManifest", from_manifest)?;
    atomic_obj.set("DockerStub", docker_stub_obj)?;

    ctx.globals().set("atomic", atomic_obj)?;
    Ok(())
}
