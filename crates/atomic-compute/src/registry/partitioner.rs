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

use super::declare_registry;

/// Reconstructs a registered named partitioner from its partition count.
pub type PartitionerFactoryFn = fn(usize) -> Partitioner;

declare_registry!(
    /// A compile-time named-partitioner factory, registered by
    /// `register_partitioner!(P)` for a `P: NamedPartitioner`.
    PartitionerEntry {
        /// The partitioner's stable registry name (`<P as NamedPartitioner>::NAME`).
        name: fn() -> &'static str,
        /// Reconstructs the partitioner from its partition count.
        factory: PartitionerFactoryFn,
    },
    /// Global compile-time named-partitioner registry — built once from all
    /// `register_partitioner!(P)` calls linked into the binary. Keyed by `P::NAME`.
    PARTITIONER_REGISTRY: &'static str => PartitionerFactoryFn,
    |entry: &PartitionerEntry| ((entry.name)(), entry.factory)
);

/// Reconstruct a registered named partitioner, or `None` if its name was never
/// registered in this binary (the caller then falls back to hash partitioning).
pub fn lookup_partitioner(name: &str, num_partitions: usize) -> Option<Partitioner> {
    PARTITIONER_REGISTRY
        .get(name)
        .map(|factory| factory(num_partitions))
}
