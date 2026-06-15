//! `partition_by_named` + `register_partitioner!` — a user partitioner that ships
//! to workers by name instead of degrading to hash.

use std::any::Any;

use atomic_compute::context::Context;
use atomic_data::partitioner::{CustomPartitioner, NamedPartitioner, TypedPartitioner};

// Shuffle needs a registered (K, V) write handler even in local mode.
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
    const NAME: &'static str = "test_mod";
    fn create(n: usize) -> Self {
        ModPartitioner { n }
    }
}

atomic_compute::register_partitioner!(ModPartitioner);

/// Same logic via the `Any`-free `TypedPartitioner` trait: the blanket impl does
/// the single downcast, so this code never mentions `Any`.
struct TypedMod {
    n: usize,
}

impl TypedPartitioner for TypedMod {
    type Key = i64;
    fn num_partitions(&self) -> usize {
        self.n
    }
    fn partition(&self, key: &i64) -> usize {
        key.rem_euclid(self.n as i64) as usize
    }
}

impl NamedPartitioner for TypedMod {
    const NAME: &'static str = "typed_mod";
    fn create(n: usize) -> Self {
        TypedMod { n }
    }
}

atomic_compute::register_partitioner!(TypedMod);

#[test]
fn registry_rebuilds_partitioner() {
    // The factory linked by `register_partitioner!` rebuilds the partitioner by name.
    let p = atomic_compute::task_registry::lookup_partitioner("test_mod", 3)
        .expect("named partitioner not found in registry");
    assert_eq!(p.num_partitions(), 3);
    assert_eq!(p.get_partition(&7i64 as &dyn Any), 1); // 7 mod 3
    assert_eq!(p.get_partition(&9i64 as &dyn Any), 0); // 9 mod 3
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partitions_by_mod() {
    let ctx = Context::local().unwrap();
    let data: Vec<(i64, i64)> = (0..12).map(|k| (k, k)).collect();

    let parts = ctx
        .parallelize_typed(data, 4)
        .partition_by_named(ModPartitioner { n: 3 })
        .collect_partitions()
        .unwrap();

    assert_eq!(parts.len(), 3, "expected 3 output partitions");
    for (p, bucket) in parts.iter().enumerate() {
        for (k, _) in bucket {
            assert_eq!(
                k.rem_euclid(3) as usize,
                p,
                "key {k} landed in partition {p}, not {}",
                k.rem_euclid(3)
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_partitioner_buckets() {
    // The TypedPartitioner path produces identical bucketing with no `Any` in user code.
    let ctx = Context::local().unwrap();
    let data: Vec<(i64, i64)> = (0..12).map(|k| (k, k)).collect();

    let parts = ctx
        .parallelize_typed(data, 4)
        .partition_by_named(TypedMod { n: 3 })
        .collect_partitions()
        .unwrap();

    assert_eq!(parts.len(), 3);
    for (p, bucket) in parts.iter().enumerate() {
        for (k, _) in bucket {
            assert_eq!(k.rem_euclid(3) as usize, p);
        }
    }
}
