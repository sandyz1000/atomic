use rustc_hash::FxHasher;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::data::Data;

pub fn hash<T: Hash>(t: &T) -> u64 {
    let mut s: FxHasher = Default::default();
    t.hash(&mut s);
    s.finish()
}

/// Trait for user-defined partitioners, used with `TypedRdd::partition_by()`.
///
/// # Example
/// ```ignore
/// struct ModPartitioner { buckets: usize }
/// impl CustomPartitioner for ModPartitioner {
///     fn num_partitions(&self) -> usize { self.buckets }
///     fn get_partition_for_key(&self, key: &dyn std::any::Any) -> usize {
///         let k = key.downcast_ref::<i32>().unwrap();
///         (*k as usize) % self.buckets
///     }
/// }
/// ```
pub trait CustomPartitioner: Send + Sync {
    fn num_partitions(&self) -> usize;
    fn get_partition_for_key(&self, key: &dyn Any) -> usize;
}

/// Partitioner enum for creating Rdd partitions
#[derive(Clone)]
pub enum Partitioner {
    Hash {
        num_partitions: usize,
        // Type-erased function for computing partition from key
        get_partition_fn: Arc<dyn Fn(&dyn Any) -> usize + Send + Sync>,
    },
    /// Range partitioner: assigns keys to partitions based on sorted bounds.
    ///
    /// Keys that compare less than `bounds[0]` go to partition 0;
    /// keys in `[bounds[i-1], bounds[i])` go to partition `i`;
    /// keys >= `bounds[last]` go to partition `num_partitions - 1`.
    ///
    /// Bounds are derived by sampling the input RDD (see `TypedRdd::sort_by_key`).
    /// `get_partition_fn` performs a binary search over the pre-computed bounds.
    Range {
        num_partitions: usize,
        get_partition_fn: Arc<dyn Fn(&dyn Any) -> usize + Send + Sync>,
    },
    /// User-defined partitioner supplied via `TypedRdd::partition_by()`.
    Custom {
        num_partitions: usize,
        get_partition_fn: Arc<dyn Fn(&dyn Any) -> usize + Send + Sync>,
    },
}

impl Partitioner {
    /// Create a hash partitioner for keys of type K
    pub fn hash<K: Data + Hash + Eq>(num_partitions: usize) -> Self {
        let get_partition_fn = Arc::new(move |key: &dyn Any| -> usize {
            let key = key.downcast_ref::<K>().unwrap();
            let mut s: FxHasher = Default::default();
            key.hash(&mut s);
            (s.finish() as usize) % num_partitions
        });

        Partitioner::Hash {
            num_partitions,
            get_partition_fn,
        }
    }

    /// Create a range partitioner from a sorted list of split-point bounds.
    ///
    /// `bounds` must be sorted ascending.  There are `bounds.len() + 1` partitions:
    /// - partition 0: keys < bounds[0]
    /// - partition i: bounds[i-1] <= key < bounds[i]
    /// - last partition: key >= bounds[last]
    ///
    /// Pass `ascending = false` to reverse the ordering (largest keys to partition 0).
    pub fn range<K>(bounds: Vec<K>, ascending: bool) -> Self
    where
        K: Data + Ord + Clone,
    {
        let num_partitions = bounds.len() + 1;
        let get_partition_fn = Arc::new(move |key: &dyn Any| -> usize {
            let k = key.downcast_ref::<K>().expect("RangePartitioner: key type mismatch");
            // For ascending bounds [b0, b1, ...]: key < b0 → 0, b0 <= key < b1 → 1, ...
            // For descending bounds [b0, b1, ...] (b0 > b1): key >= b0 → 0, b1 <= key < b0 → 1, ...
            let part = if ascending {
                bounds.partition_point(|b| b <= k)
            } else {
                bounds.partition_point(|b| b >= k)
            };
            part.min(num_partitions.saturating_sub(1))
        });
        Partitioner::Range { num_partitions, get_partition_fn }
    }

    /// Build a `Partitioner` from any type that implements `CustomPartitioner`.
    pub fn from_custom<P: CustomPartitioner + 'static>(p: P) -> Self {
        let p = Arc::new(p);
        let n = p.num_partitions();
        Partitioner::Custom {
            num_partitions: n,
            get_partition_fn: Arc::new(move |key| p.get_partition_for_key(key)),
        }
    }

    /// Check if two partitioners are equal (same type and same num_partitions)
    pub fn equals(&self, other: &Partitioner) -> bool {
        match (self, other) {
            (Partitioner::Hash { num_partitions: n1, .. }, Partitioner::Hash { num_partitions: n2, .. }) => n1 == n2,
            (Partitioner::Range { num_partitions: n1, .. }, Partitioner::Range { num_partitions: n2, .. }) => n1 == n2,
            (Partitioner::Custom { num_partitions: n1, .. }, Partitioner::Custom { num_partitions: n2, .. }) => n1 == n2,
            _ => false,
        }
    }

    pub fn num_partitions(&self) -> usize {
        match self {
            Partitioner::Hash { num_partitions, .. }
            | Partitioner::Range { num_partitions, .. }
            | Partitioner::Custom { num_partitions, .. } => *num_partitions,
        }
    }

    pub fn get_num_of_partitions(&self) -> usize {
        self.num_partitions()
    }

    pub fn get_partition(&self, key: &dyn Any) -> usize {
        match self {
            Partitioner::Hash { get_partition_fn, .. }
            | Partitioner::Range { get_partition_fn, .. }
            | Partitioner::Custom { get_partition_fn, .. } => get_partition_fn(key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_partition() {
        let data = vec![1, 2];
        let num_partition = 3;
        let hash_partitioner = Partitioner::hash::<i32>(num_partition);

        for i in &data {
            println!("value: {:?}-hash: {:?}", i, hash(i));
            println!(
                "value: {:?}-index: {:?}",
                i,
                hash_partitioner.get_partition(i as &dyn Any)
            );
        }

        let mut partition = vec![Vec::new(); num_partition];
        for i in &data {
            let index = hash_partitioner.get_partition(i as &dyn Any);
            partition[index].push(i)
        }
        assert_eq!(partition.len(), 3)
    }

    #[test]
    fn hash_partitioner_eq() {
        let p1 = Partitioner::hash::<i32>(1);
        let p2_1 = Partitioner::hash::<i32>(2);
        let p2_2 = Partitioner::hash::<i32>(2);

        assert!(p1.equals(&p1));
        assert!(p2_1.equals(&p2_1));
        assert!(p2_1.equals(&p2_2));
        assert!(p2_2.equals(&p2_1));
        assert!(!p1.equals(&p2_1));
        assert!(!p1.equals(&p2_2));

        let mut p1 = Some(p1);
        assert!(p1.clone().map(|p| p.equals(&p1.clone().unwrap())) == Some(true));
        assert!(p1.clone().map(|p| p.equals(&p2_1)) == Some(false));
        assert!(p1.clone().map(|p| p.equals(&p2_2)) == Some(false));
        assert!(p1.clone().map(|p| p.equals(&p1.clone().unwrap())) != None);
        assert!(p1.clone().map_or(false, |p| p.equals(&p1.clone().unwrap())));
        assert!(!p1.clone().map_or(false, |p| p.equals(&p2_1)));
        assert!(!p1.clone().map_or(false, |p| p.equals(&p2_2)));

        p1 = None;
        assert!(p1.clone().map(|p| p.equals(&p1.clone().unwrap())) == None);
        assert!(p1.clone().map(|p| p.equals(&p2_1)) == None);
        assert!(p1.clone().map(|p| p.equals(&p2_2)) == None);
        assert!(!p1.clone().map_or(false, |p| p.equals(&p1.clone().unwrap())));
        assert!(!p1.clone().map_or(false, |p| p.equals(&p2_1)));
        assert!(!p1.map_or(false, |p| p.equals(&p2_2)));
    }
}
