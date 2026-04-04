fn main() {
    if let Err(err) = atomic_compute::executor::run_worker_from_config() {
        eprintln!("worker failed: {}", err);
        std::process::exit(1);
    }
}
