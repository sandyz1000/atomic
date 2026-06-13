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
        /// Sort direction, retained so the partitioner can be shipped/reconstructed on workers.
        ascending: bool,
        /// `bincode(Vec<K>)` of the sorted split-point bounds, captured at construction so the
        /// range partitioner can be shipped to and reconstructed on workers.
        bounds_bytes: Vec<u8>,
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
        K: Data + Ord + Clone + bincode::Encode,
    {
        let num_partitions = bounds.len() + 1;
        // Capture the bounds in serializable form so this partitioner can be shipped to workers.
        let bounds_bytes =
            bincode::encode_to_vec(&bounds, bincode::config::standard()).unwrap_or_default();
        let get_partition_fn = Arc::new(move |key: &dyn Any| -> usize {
            let k = key
                .downcast_ref::<K>()
                .expect("RangePartitioner: key type mismatch");
            // For ascending bounds [b0, b1, ...]: key < b0 → 0, b0 <= key < b1 → 1, ...
            // For descending bounds [b0, b1, ...] (b0 > b1): key >= b0 → 0, b1 <= key < b0 → 1, ...
            let part = if ascending {
                bounds.partition_point(|b| b <= k)
            } else {
                bounds.partition_point(|b| b >= k)
            };
            part.min(num_partitions.saturating_sub(1))
        });
        Partitioner::Range {
            num_partitions,
            ascending,
            bounds_bytes,
            get_partition_fn,
        }
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
            (
                Partitioner::Hash {
                    num_partitions: n1, ..
                },
                Partitioner::Hash {
                    num_partitions: n2, ..
                },
            ) => n1 == n2,
            (
                Partitioner::Range {
                    num_partitions: n1, ..
                },
                Partitioner::Range {
                    num_partitions: n2, ..
                },
            ) => n1 == n2,
            (
                Partitioner::Custom {
                    num_partitions: n1, ..
                },
                Partitioner::Custom {
                    num_partitions: n2, ..
                },
            ) => n1 == n2,
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
            Partitioner::Hash {
                get_partition_fn, ..
            }
            | Partitioner::Range {
                get_partition_fn, ..
            }
            | Partitioner::Custom {
                get_partition_fn, ..
            } => get_partition_fn(key),
        }
    }

    /// Build the wire-shippable spec for this partitioner (sent to workers in the shuffle-map task).
    pub fn to_spec(&self) -> PartitionerSchema {
        match self {
            Partitioner::Hash { num_partitions, .. } => PartitionerSchema::Hash {
                num_parts: *num_partitions,
            },
            Partitioner::Range {
                num_partitions,
                ascending,
                bounds_bytes,
                ..
            } => PartitionerSchema::Range {
                num_parts: *num_partitions,
                ascending: *ascending,
                bounds_bytes: bounds_bytes.clone(),
            },
            Partitioner::Custom { num_partitions, .. } => PartitionerSchema::Custom {
                num_parts: *num_partitions,
            },
        }
    }
}

/// Serializable, wire-shippable description of a `Partitioner`. The driver builds this from a
/// `Partitioner` (`Partitioner::to_spec`) and ships it in the shuffle-map task; the worker
/// reconstructs a real `Partitioner` from it, monomorphized for the key type `K`.
///
/// `Custom` partitioners hold a user closure and cannot be serialized, so they degrade to hash
/// partitioning in distributed mode (their `num_partitions` is still honored).
#[derive(Clone, Debug, bincode::Encode, bincode::Decode)]
pub enum PartitionerSchema {
    Hash {
        num_parts: usize,
    },
    Range {
        num_parts: usize,
        ascending: bool,
        bounds_bytes: Vec<u8>,
    },
    Custom {
        num_parts: usize,
    },
}

impl PartitionerSchema {
    pub fn num_partitions(&self) -> usize {
        match self {
            PartitionerSchema::Hash { num_parts: num_partitions }
            | PartitionerSchema::Range { num_parts: num_partitions, .. }
            | PartitionerSchema::Custom { num_parts: num_partitions } => *num_partitions,
        }
    }

    pub fn is_range(&self) -> bool {
        matches!(self, PartitionerSchema::Range { .. })
    }

    /// Reconstruct a hash partitioner (used by the `Hash`-only no-combine worker handler).
    /// `Range`/`Custom` specs fall back to hash here — reconstructing a `Range` needs `K: Ord`
    /// (see [`PartitionerSpec::into_partitioner`]).
    pub fn into_hash<K: Data + Hash + Eq>(&self) -> Partitioner {
        Partitioner::hash::<K>(self.num_partitions())
    }

    /// Reconstruct the real partitioner for an ordered key: `Range` decodes its bounds back into a
    /// `RangePartitioner`; `Hash`/`Custom` produce a hash partitioner.
    pub fn into_partitioner<K>(&self) -> Partitioner
    where
        K: Data + Ord + Clone + Hash + Eq + bincode::Encode + bincode::Decode<()>,
    {
        match self {
            PartitionerSchema::Range {
                ascending,
                bounds_bytes,
                ..
            } => {
                let bounds: Vec<K> =
                    bincode::decode_from_slice(bounds_bytes, bincode::config::standard())
                        .map(|(b, _)| b)
                        .unwrap_or_default();
                Partitioner::range(bounds, *ascending)
            }
            _ => Partitioner::hash::<K>(self.num_partitions()),
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
