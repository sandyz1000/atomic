/// Simplest `#[task]` example — double, filter, and sum a dataset.
///
/// # What `#[task]` does
///
/// Annotating a function with `#[task]`:
/// 1. Keeps the function **callable normally** — in local mode, tests, closures.
/// 2. Generates a **zero-sized task struct** (PascalCase) that implements
///    `UnaryTask` or `BinaryTask` with a statically-known `NAME` (rusty-celery–inspired).
/// 3. Generates `__atomic_dispatch_<fn>` that decodes rkyv partition bytes,
///    applies the function for the requested `TaskAction`, and re-encodes results.
/// 4. Registers a `TaskEntry { op_id, handler }` via `inventory::submit!` so
///    the worker binary can dispatch by name at startup.
///
/// # Running locally (no workers needed)
///
/// ```bash
/// cargo run -p task_double
/// ```
///
/// # Running distributed
///
/// **Step 1** — Create `~/hosts.conf` listing master and workers.
/// Slave format is `"user@ip:port"`:
/// ```toml
/// master = "127.0.0.1:9000"
/// slaves = ["sandip.dey@127.0.0.1:10001", "sandip.dey@127.0.0.1:10002"]
/// ```
///
/// **Step 2** — Build and start workers (RUST_LOG makes their logs visible):
/// ```bash
/// cargo build -p task_double --release
/// RUST_LOG=info ./target/release/task_double --worker --port 10001 &
/// RUST_LOG=info ./target/release/task_double --worker --port 10002 &
/// ```
///
/// **Step 3** — Run the driver:
/// ```bash
/// RUST_LOG=info \
/// VEGA_DEPLOYMENT_MODE=distributed \
/// VEGA_LOCAL_IP=127.0.0.1 \
///   ./target/release/task_double --driver
/// ```
use atomic_compute::app::{AppRole, AtomicApp};
use atomic_compute::{task, task_fn};

// ── Task functions ─────────────────────────────────────────────────────────────
//
// `#[task]` keeps each function callable (double(x), add(a, b), …) AND generates
// a zero-sized struct (Double, Add, …) that implements UnaryTask / BinaryTask.
// Pass the struct to map_task / filter_task / flat_map_task / fold_task for
// distributed-compatible execution.

/// Double a number.
#[task]
fn double(x: i32) -> i32 {
    x * 2
}

/// Return true for positive numbers.
#[task]
fn is_positive(x: i32) -> bool {
    x > 0
}

/// Add two numbers.
#[task]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

/// Expand an integer into [x, -x].
#[task]
fn mirror(x: i32) -> Vec<i32> {
    vec![x, -x]
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // RUST_LOG=info  → enables task dispatch logs from NativeBackend and Executor.
    // RUST_LOG=debug → adds per-frame transport logs.
    env_logger::init();

    // AtomicApp::build() checks CLI args:
    //   --worker [--port N]  → starts executor loop and exits (never reaches below)
    //   --driver / no flags  → returns a live Context via driver_context()
    let app = AtomicApp::build().await?;

    match &app.role {
        AppRole::Worker { port } => {
            log::info!("worker exiting (port {})", port);
            return Ok(());
        }
        AppRole::Driver => {
            let mode = std::env::var("VEGA_DEPLOYMENT_MODE").unwrap_or_else(|_| "local".into());
            log::info!("driver starting in {} mode", mode);
            println!("[driver] mode={}", mode);
        }
    }

    let ctx = app.driver_context()?;

    let data = vec![-3i32, -1, 0, 2, 4, 6];
    println!("Input:    {:?}", data);

    // map_task — apply `Double` element-wise (works in local and distributed mode)
    let doubled = ctx
        .parallelize_typed(data.clone(), 2)
        .map_task(Double)
        .collect()?;
    println!("Doubled:  {:?}", doubled);

    // filter_task — keep only positive numbers using `IsPositive`
    let positives = ctx
        .parallelize_typed(data.clone(), 2)
        .filter_task(IsPositive)
        .collect()?;
    println!("Positive: {:?}", positives);

    // flat_map_task — expand each element into [x, -x] using `Mirror`
    let mirrored = ctx
        .parallelize_typed(data.clone(), 2)
        .flat_map_task(Mirror)
        .collect()?;
    println!("Mirrored: {:?}", mirrored);

    // fold_task — sum using `Add` as the combiner (works in local and distributed mode)
    let total = ctx
        .parallelize_typed(data.clone(), 2)
        .fold_task(0i32, Add)?;
    println!("Sum:      {}", total);

    // Chain: Double → IsPositive → sum  (fully distributed-compatible)
    let chain_sum = ctx
        .parallelize_typed(data.clone(), 2)
        .map_task(Double)
        .filter_task(IsPositive)
        .fold_task(0i32, Add)?;
    println!("Chain (double → keep positives → sum): {}", chain_sum);

    // task_fn! — inline closure wrapped into a distributed-compatible UnaryTask/BinaryTask.
    // Equivalent to #[task] fn but anonymous; uses source location as the stable op_id.
    // Unary closures require an explicit return type annotation (`-> T`).
    let doubled_inline = ctx
        .parallelize_typed(data.clone(), 2)
        .map_task(task_fn!(|x: i32| -> i32 { x * 2 }))
        .collect()?;
    println!("task_fn doubled: {:?}", doubled_inline);

    let positives_inline = ctx
        .parallelize_typed(data.clone(), 2)
        .filter_task(task_fn!(|x: i32| -> bool { x > 0 }))
        .collect()?;
    println!("task_fn positive: {:?}", positives_inline);

    // Binary (fold) closures infer return type from the first arg.
    let sum_inline = ctx
        .parallelize_typed(data, 2)
        .fold_task(0i32, task_fn!(|a: i32, b: i32| a + b))?;
    println!("task_fn sum: {}", sum_inline);

    Ok(())
}
