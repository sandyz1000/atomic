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

/// A `CustomPartitioner` that can be **shipped to workers** by name instead of by
/// closure. Register it with `atomic_compute::register_partitioner!(MyType)` so
/// the worker can reconstruct it for distributed `partition_by_named`.
///
/// The only state shipped over the wire is `num_partitions`; the worker rebuilds
/// the partitioner with [`create`](NamedPartitioner::create). A named partitioner
/// must therefore be fully determined by its partition count (for richer config,
/// use a `Range` partitioner whose bounds are serialized, or stay local).
///
/// ```ignore
/// struct ModPartitioner { n: usize }
/// impl CustomPartitioner for ModPartitioner {
///     fn num_partitions(&self) -> usize { self.n }
///     fn get_partition_for_key(&self, key: &dyn Any) -> usize {
///         (*key.downcast_ref::<i64>().unwrap() as usize) % self.n
///     }
/// }
/// impl NamedPartitioner for ModPartitioner {
///     const NAME: &'static str = "mod";
///     fn create(n: usize) -> Self { ModPartitioner { n } }
/// }
/// atomic_compute::register_partitioner!(ModPartitioner);
/// ```
pub trait NamedPartitioner: CustomPartitioner + 'static {
    /// Stable, unique name used to dispatch this partitioner on workers.
    const NAME: &'static str;
    /// Reconstruct the partitioner from just its partition count.
    fn create(num_partitions: usize) -> Self
    where
        Self: Sized;
}

/// A statically-typed partitioner — the `Any`-free alternative to implementing
/// [`CustomPartitioner`] directly.
///
/// `Partitioner` itself is stored in the type-erased dependency DAG, so its
/// [`get_partition`](Partitioner::get_partition) seam must accept `&dyn Any`.
/// `TypedPartitioner` keeps that erasure out of *user* code: implement
/// [`partition`](TypedPartitioner::partition) with a concrete `Key` and the
/// blanket impl below performs the single, localized downcast.
///
/// ```
/// use atomic_data::partitioner::TypedPartitioner;
/// struct ModPartitioner { n: usize }
/// impl TypedPartitioner for ModPartitioner {
///     type Key = i64;
///     fn num_partitions(&self) -> usize { self.n }
///     fn partition(&self, key: &i64) -> usize { key.rem_euclid(self.n as i64) as usize }
/// }
/// ```
pub trait TypedPartitioner: Send + Sync + 'static {
    /// The concrete key type this partitioner operates on.
    type Key: 'static;
    /// Number of output partitions.
    fn num_partitions(&self) -> usize;
    /// Map a typed key to a partition index in `0..num_partitions()`.
    fn partition(&self, key: &Self::Key) -> usize;
}

/// The one place the partitioner key is downcast: bridges any [`TypedPartitioner`]
/// into the erased [`CustomPartitioner`] the DAG stores. User partitioners that
/// implement `TypedPartitioner` get `CustomPartitioner` for free and never touch
/// `Any`. (A type may implement `TypedPartitioner` *or* `CustomPartitioner`
/// directly, not both — coherence forbids overlap.)
impl<P: TypedPartitioner> CustomPartitioner for P {
    fn num_partitions(&self) -> usize {
        TypedPartitioner::num_partitions(self)
    }
    fn get_partition_for_key(&self, key: &dyn Any) -> usize {
        let k = key.downcast_ref::<P::Key>().unwrap_or_else(|| {
            panic!(
                "TypedPartitioner: key is not {}",
                std::any::type_name::<P::Key>()
            )
        });
        self.partition(k)
    }
}

/// Type-erased function mapping a key (`&dyn Any`) to a partition index.
pub type PartitionFn = Arc<dyn Fn(&dyn Any) -> usize + Send + Sync>;

/// Partitioner enum for creating Rdd partitions
#[derive(Clone)]
pub enum Partitioner {
    Hash {
        num_partitions: usize,
        // Type-erased function for computing partition from key
        get_partition_fn: PartitionFn,
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
        get_partition_fn: PartitionFn,
    },
    /// User-defined partitioner supplied via `TypedRdd::partition_by()`.
    Custom {
        num_partitions: usize,
        /// Registry name when built from a [`NamedPartitioner`], enabling the worker
        /// to reconstruct it for distributed shuffles. `None` for closure-based
        /// custom partitioners, which degrade to hash partitioning on workers.
        name: Option<String>,
        get_partition_fn: PartitionFn,
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
    ///
    /// The resulting partitioner has no registry name, so it degrades to hash
    /// partitioning in distributed mode. Use [`from_named`](Partitioner::from_named)
    /// for a partitioner that ships correctly to workers.
    pub fn from_custom<P: CustomPartitioner + 'static>(p: P) -> Self {
        let p = Arc::new(p);
        let n = p.num_partitions();
        Partitioner::Custom {
            num_partitions: n,
            name: None,
            get_partition_fn: Arc::new(move |key| p.get_partition_for_key(key)),
        }
    }

    /// Build a `Partitioner` from a [`NamedPartitioner`], tagging it with the
    /// registry name so workers can reconstruct it. `num_partitions` is the only
    /// state shipped; the worker rebuilds via `P::create(num_partitions)`.
    pub fn from_named<P: NamedPartitioner>(num_partitions: usize) -> Self {
        let p = Arc::new(P::create(num_partitions));
        Partitioner::Custom {
            num_partitions,
            name: Some(P::NAME.to_string()),
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
            Partitioner::Custom {
                num_partitions,
                name,
                ..
            } => PartitionerSchema::Custom {
                num_parts: *num_partitions,
                name: name.clone(),
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
        /// Registry name of a [`NamedPartitioner`], if this came from
        /// `partition_by_named`. `None` → degrades to hash on the worker.
        name: Option<String>,
    },
}

impl PartitionerSchema {
    pub fn num_partitions(&self) -> usize {
        match self {
            PartitionerSchema::Hash {
                num_parts: num_partitions,
            }
            | PartitionerSchema::Range {
                num_parts: num_partitions,
                ..
            }
            | PartitionerSchema::Custom {
                num_parts: num_partitions,
                ..
            } => *num_partitions,
        }
    }

    /// The registry name of a named custom partitioner, if any.
    pub fn custom_name(&self) -> Option<&str> {
        match self {
            PartitionerSchema::Custom { name: Some(n), .. } => Some(n.as_str()),
            _ => None,
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
        assert_eq!(
            p1.clone().map(|p| p.equals(p1.as_ref().unwrap())),
            Some(true)
        );
        assert_eq!(p1.clone().map(|p| p.equals(&p2_1)), Some(false));
        assert_eq!(p1.clone().map(|p| p.equals(&p2_2)), Some(false));
        assert!(p1.clone().map(|p| p.equals(p1.as_ref().unwrap())).is_some());
        assert!(p1.clone().is_some_and(|p| p.equals(p1.as_ref().unwrap())));
        assert!(!p1.clone().is_some_and(|p| p.equals(&p2_1)));
        assert!(!p1.clone().is_some_and(|p| p.equals(&p2_2)));

        p1 = None;
        assert!(p1.clone().map(|p| p.equals(p1.as_ref().unwrap())).is_none());
        assert!(p1.clone().map(|p| p.equals(&p2_1)).is_none());
        assert!(p1.clone().map(|p| p.equals(&p2_2)).is_none());
        assert!(!p1.clone().is_some_and(|p| p.equals(p1.as_ref().unwrap())));
        assert!(!p1.clone().is_some_and(|p| p.equals(&p2_1)));
        assert!(!p1.is_some_and(|p| p.equals(&p2_2)));
    }
}
