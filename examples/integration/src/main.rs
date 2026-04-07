//! Self-contained distributed integration test helper.
//!
//! Run as worker:
//!   ./integration --worker --port 19201
//!   (env vars VEGA_DEPLOYMENT_MODE, VEGA_LOCAL_IP set automatically)
//!
//! Run as driver:
//!   ./integration --driver --workers 127.0.0.1:19201
//!   (env vars VEGA_DEPLOYMENT_MODE, VEGA_LOCAL_IP set automatically)
//!   Output: {"doubled":[2,4,6,8],"sum":10}

use atomic_compute::context::{Context, start_worker};
use atomic_compute::task;

#[task]
fn double(x: i32) -> i32 {
    x * 2
}

#[task]
fn add(a: i32, b: i32) -> i32 {
    a + b
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--worker") {
        let port: u16 = args
            .windows(2)
            .find(|w| w[0] == "--port")
            .and_then(|w| w[1].parse().ok())
            .expect("--worker requires --port N");

        // Configure as a distributed slave worker before the lazy OnceCell initializes.
        // Safety: this must happen before any call to Configuration::get().
        unsafe {
            std::env::set_var("VEGA_DEPLOYMENT_MODE", "distributed");
            std::env::set_var("VEGA_SLAVE_DEPLOYMENT", "true");
            std::env::set_var("VEGA_SLAVE_PORT", port.to_string());
            std::env::set_var("VEGA_LOCAL_IP", "127.0.0.1");
        }

        // start_worker() reads the env vars above and never returns.
        start_worker();
    } else if args.iter().any(|a| a == "--driver") {
        let workers: Vec<String> = args
            .windows(2)
            .find(|w| w[0] == "--workers")
            .map(|w| w[1].split(',').map(str::to_string).collect())
            .unwrap_or_default();

        // Write a hosts.conf into a temp dir and point HOME at it, so that
        // atomic_compute::hosts::Hosts::load() finds the workers we spawned.
        let home_dir = std::env::temp_dir().join("atomic-integration-driver");
        std::fs::create_dir_all(&home_dir).expect("failed to create temp home dir");

        let slaves_toml = workers
            .iter()
            .map(|w| format!("\"user@{}\"", w))
            .collect::<Vec<_>>()
            .join(", ");
        let hosts_content = format!(
            "master = \"127.0.0.1:0\"\nslaves = [{}]\n",
            slaves_toml
        );
        std::fs::write(home_dir.join("hosts.conf"), &hosts_content)
            .expect("failed to write hosts.conf");

        unsafe {
            std::env::set_var("HOME", &home_dir);
            std::env::set_var("VEGA_DEPLOYMENT_MODE", "distributed");
            std::env::set_var("VEGA_LOCAL_IP", "127.0.0.1");
        }

        let _ = env_logger::try_init();

        if let Err(e) = run_driver() {
            eprintln!("driver error: {e}");
            std::process::exit(1);
        }
    } else {
        eprintln!(
            "usage:\n  integration --worker --port N\n  integration --driver --workers host:port[,...]"
        );
        std::process::exit(1);
    }
}

fn run_driver() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::new()?;

    let input = vec![1i32, 2, 3, 4];

    let doubled = ctx
        .parallelize_typed(input.clone(), 2)
        .map_task(Double)
        .collect()?;

    let sum = ctx
        .parallelize_typed(input, 2)
        .fold_task(0i32, Add)?;

    // Machine-readable JSON output — parsed by the integration test.
    println!(
        "{}",
        serde_json::json!({ "doubled": doubled, "sum": sum })
    );

    Ok(())
}
