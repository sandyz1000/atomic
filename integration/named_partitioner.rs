//! Integration binary for distributed named-partitioner test.
//!
//! Validates that a `NamedPartitioner` ships to workers **by name** in
//! distributed mode and is honored exactly (not degraded to hash partitioning,
//! the way an unregistered `CustomPartitioner` closure would be).
//!
//! Run as worker:
//!   ./integration_named_partitioner --worker --port 19501
//!
//! Run as driver:
//!   ./integration_named_partitioner --driver --workers 127.0.0.1:19501
//!   Output: {"partitions":[[[0,0],[3,3],[6,6],[9,9]],[[1,1],[4,4],[7,7],[10,10]],[[2,2],[5,5],[8,8],[11,11]]]}

use atomic_compute::context::{Context, start_worker};
use atomic_compute::env::Config;
use atomic_data::partitioner::{CustomPartitioner, NamedPartitioner};
use clap::Parser;
use std::any::Any;
use std::net::Ipv4Addr;

#[path = "cli.rs"]
mod cli;
use cli::IntegrationCli;

atomic_compute::register_shuffle_map!(i64, i64);

/// Partitions an `i64` key into `key mod n`.
struct ModPartitioner {
    n: usize,
}

impl CustomPartitioner for ModPartitioner {
    fn num_partitions(&self) -> usize {
        self.n
    }
    fn get_partition_for_key(&self, key: &dyn Any) -> usize {
        let k = key.downcast_ref::<i64>().copied().unwrap_or(0);
        k.rem_euclid(self.n as i64) as usize
    }
}

impl NamedPartitioner for ModPartitioner {
    const NAME: &'static str = "dist_mod";
    fn create(n: usize) -> Self {
        ModPartitioner { n }
    }
}

atomic_compute::register_partitioner!(ModPartitioner);

fn main() {
    let cli = IntegrationCli::parse();
    let _ = env_logger::try_init();

    if cli.worker {
        let port = cli.port.expect("--worker requires --port N");
        let config = Config::worker(Ipv4Addr::LOCALHOST, port);
        start_worker(config);
    } else if cli.driver {
        let config = Config::distributed_driver(Ipv4Addr::LOCALHOST, cli.workers);
        if let Err(e) = run_driver(config) {
            eprintln!("driver error: {e}");
            std::process::exit(1);
        }
    } else {
        eprintln!(
            "usage:\n  integration_named_partitioner --worker --port N\n  \
             integration_named_partitioner --driver --workers host:port[,...]\n"
        );
        std::process::exit(1);
    }
}

fn run_driver(config: Config) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Context::new_with_config(config)?;
    let data: Vec<(i64, i64)> = (0..12).map(|k| (k, k)).collect();

    let parts = ctx
        .parallelize_typed(data, 4)
        .partition_by_named(ModPartitioner { n: 3 })
        .collect_partitions()?;

    println!("{}", serde_json::json!({ "partitions": parts }));
    Ok(())
}
