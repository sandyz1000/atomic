//! Scenario: distributed named partitioner.
//!
//! Validates that a `NamedPartitioner` ships to workers **by name** in
//! distributed mode and is honored exactly (not degraded to hash partitioning,
//! the way an unregistered `CustomPartitioner` closure would be).
//!
//! Expected output: {"partitions":[[[0,0],[3,3],[6,6],[9,9]],[[1,1],[4,4],[7,7],[10,10]],[[2,2],[5,5],[8,8],[11,11]]]}

use atomic_compute::context::Context;
use atomic_data::partitioner::{CustomPartitioner, NamedPartitioner};
use std::any::Any;
use std::error::Error;
use std::sync::Arc;

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

pub fn run_driver(ctx: &Arc<Context>) -> Result<(), Box<dyn Error>> {
    let data: Vec<(i64, i64)> = (0..12).map(|k| (k, k)).collect();

    let parts = ctx
        .parallelize_typed(data, 4)
        .partition_by_named(ModPartitioner { n: 3 })
        .collect_partitions()?;

    println!("{}", serde_json::json!({ "partitions": parts }));
    Ok(())
}
