/// Simplest `#[task]` example — double, filter, and sum a dataset.
///
/// # What `#[task]` does
///
/// Annotating a function with `#[task]`:
/// 1. Keeps the function **callable normally** — in local mode, tests, closures.
/// 2. Generates `__atomic_dispatch_<fn>` that decodes rkyv partition bytes,
///    applies the function for the requested `TaskAction`, and re-encodes results.
/// 3. Registers a `TaskEntry { op_id, handler }` via `inventory::submit!` so
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
/// Start workers (same binary, different flag):
/// ```bash
/// cargo build -p task_double --release
/// ./target/release/task_double --worker --port 10001 &
/// ./target/release/task_double --worker --port 10002 &
/// ```
///
/// Run the driver:
/// ```bash
/// ATOMIC_DEPLOYMENT_MODE=distributed \
/// ATOMIC_IS_DRIVER=true \
///   ./target/release/task_double --driver
/// ```
use atomic_compute::app::{AppRole, AtomicApp};
use atomic_compute::task;

// ── Task functions ─────────────────────────────────────────────────────────────
//
// Unary T -> U functions → support TaskAction::Map and TaskAction::FlatMap
// Unary T -> bool functions → support TaskAction::Filter
// Binary (T, T) -> T functions → support TaskAction::Fold and TaskAction::Reduce

/// Double a number.  Registered as a Map task.
#[task]
fn double(x: i32) -> i32 {
    x * 2
}

/// Return true for positive numbers.  Registered as a Filter task (bool return).
#[task]
fn is_positive(x: i32) -> bool {
    x > 0
}

/// Add two numbers.  Registered as a Fold/Reduce task (binary, same type).
#[task]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

/// Expand an integer into [x, -x].  Registered as a FlatMap task.
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
            // Worker path: executor.worker() already ran and process::exit(0) was called
            // before reaching here. This branch is unreachable in practice but documents
            // the intent clearly.
            log::info!("worker exiting (port {})", port);
            return Ok(());
        }
        AppRole::Driver => {
            let mode = std::env::var("ATOMIC_DEPLOYMENT_MODE").unwrap_or_else(|_| "local".into());
            log::info!("driver starting in {} mode", mode);
            println!("[driver] mode={}", mode);
        }
    }

    let ctx = app.driver_context()?;

    let data = vec![-3i32, -1, 0, 2, 4, 6];
    println!("Input:    {:?}", data);

    // Map — apply `double` element-wise (TaskAction::Map in distributed mode)
    let doubled = ctx
        .parallelize_typed(data.clone(), 2)
        .map(double)
        .collect()?;
    println!("Doubled:  {:?}", doubled);

    // Filter — keep only positive numbers using `is_positive`
    let positives = ctx
        .parallelize_typed(data.clone(), 2)
        .filter(|x| is_positive(*x))
        .collect()?;
    println!("Positive: {:?}", positives);

    // FlatMap — expand each element into [x, -x] using `mirror`
    let mirrored = ctx
        .parallelize_typed(data.clone(), 2)
        .flat_map(|x| Box::new(mirror(x).into_iter()))
        .collect()?;
    println!("Mirrored: {:?}", mirrored);

    // Fold — sum using `add` as the combiner (binary task)
    let total = ctx
        .parallelize_typed(data.clone(), 2)
        .fold(0i32, add)?;
    println!("Sum:      {}", total);

    // Chain: double → filter positives → sum
    let chain_sum = ctx
        .parallelize_typed(data, 2)
        .map(double)
        .filter(|x| is_positive(*x))
        .fold(0i32, add)?;
    println!("Chain (double → keep positives → sum): {}", chain_sum);

    Ok(())
}
