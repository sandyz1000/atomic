//! Named-partitioner registry, so a distributed `partition_by_named` can ship a
//! user partitioner by **name** (no closure serialization) — the driver puts the
//! name in the shuffle's `PartitionerSchema::Custom`, and the worker rebuilds the
//! partitioner via this registry's factory, mirroring how `#[task]` ships compute
//! by `task_name`.
//!
//! ```rust,ignore
//! atomic_compute::register_partitioner!(ModPartitioner);
//! ```

use atomic_data::partitioner::Partitioner;
use once_cell::sync::Lazy;
use std::collections::HashMap;

/// Reconstructs a registered named partitioner from its partition count.
pub type PartitionerFactoryFn = fn(usize) -> Partitioner;

/// A compile-time named-partitioner factory, registered by
/// `register_partitioner!(P)` for a `P: NamedPartitioner`.
pub struct PartitionerEntry {
    /// The partitioner's stable registry name (`<P as NamedPartitioner>::NAME`).
    pub name: fn() -> &'static str,
    /// Reconstructs the partitioner from its partition count.
    pub factory: PartitionerFactoryFn,
}

impl PartitionerEntry {
    /// `num_partitions` is the number of reduce partitions to build for.
    pub fn build(&self, num_partitions: usize) -> Partitioner {
        (self.factory)(num_partitions)
    }
}

inventory::collect!(PartitionerEntry);

/// Global compile-time named-partitioner registry — built once from all
/// `register_partitioner!(P)` calls linked into the binary. Keyed by `P::NAME`.
pub static PARTITIONER_REGISTRY: Lazy<HashMap<&'static str, &'static PartitionerEntry>> =
    Lazy::new(|| {
        inventory::iter::<PartitionerEntry>
            .into_iter()
            .map(|entry| ((entry.name)(), entry))
            .collect()
    });

/// Reconstruct a registered named partitioner, or `None` if its name was never
/// registered in this binary (the caller then falls back to hash partitioning).
pub fn lookup_partitioner(name: &str, num_partitions: usize) -> Option<Partitioner> {
    PARTITIONER_REGISTRY
        .get(name)
        .map(|entry| entry.build(num_partitions))
}
