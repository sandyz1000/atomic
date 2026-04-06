/// atomic-js — run a JavaScript file in the Atomic QuickJS environment.
///
/// # Usage
///
///   js_runner <script.js>
///
/// The script has access to the `atomic` global:
///
///   const rdd = atomic.parallelize([1, 2, 3, 4]);
///   const result = rdd.map(x => x + 1).collect();
///   atomic.print(result);
///
/// Exit codes:
///   0 — success
///   1 — JS runtime error or file not found
use atomic_js::AtomicJsRuntime;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: js_runner <script.js>");
        std::process::exit(1);
    }

    let path = &args[1];
    let runtime = AtomicJsRuntime::default();

    if let Err(e) = runtime.run_file(path) {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}
