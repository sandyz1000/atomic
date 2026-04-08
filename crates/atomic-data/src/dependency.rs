use bincode::Encode;

use crate::aggregator::Aggregator;
use crate::data::Data;
use crate::distributed::WireEncode;
// use crate::env;
use crate::partitioner::Partitioner;
use crate::rdd::RddBase;
use crate::split::CoalescedRddSplit;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

/// Type of dependency between RDDs
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DependencyType {
    /// Narrow dependency where each partition depends on a known subset of parent partitions
    Narrow,
    /// Shuffle dependency where data needs to be redistributed across partitions
    Shuffle,
}

/// Dependency between RDDs
#[derive(Clone)]
pub enum Dependency {
    /// One-to-one narrow dependency where each partition depends on exactly one parent partition
    OneToOne { rdd_base: Arc<dyn RddBase> },
    /// Range narrow dependency between ranges of partitions in parent and child RDDs
    Range {
        rdd_base: Arc<dyn RddBase>,
        /// the start of the range in the parent RDD
        in_start: usize,
        /// the start of the range in the child RDD
        out_start: usize,
        /// the length of the range
        length: usize,
    },

    CoalescedSplitDep {
        /// The RDD that this coalesced split depends on.
        /// This is a reference to the base RDD in the dependency graph.
        rdd: Arc<dyn RddBase>,
        /// The previous RDD in the transformation chain.
        /// Used to track the lineage of transformations leading to this coalesced split.
        prev: Arc<dyn RddBase>,
    },
    /// Shuffle dependency representing a shuffle operation
    /// Uses type-erased box to allow heterogeneous shuffle dependencies with different type parameters
    Shuffle(Arc<ShuffleDependencyBox>),
}

impl Dependency {
    /// Create a new one-to-one dependency
    pub fn new_one_to_one(rdd_base: Arc<dyn RddBase>) -> Self {
        Dependency::OneToOne { rdd_base }
    }

    /// Create a new range dependency
    pub fn new_range(
        rdd_base: Arc<dyn RddBase>,
        in_start: usize,
        out_start: usize,
        length: usize,
    ) -> Self {
        Dependency::Range {
            rdd_base,
            in_start,
            out_start,
            length,
        }
    }

    /// Get parent partition IDs for a given partition (for narrow dependencies)
    pub fn get_parents(&self, partition_id: usize) -> Vec<usize> {
        match self {
            Dependency::OneToOne { .. } => vec![partition_id],
            Dependency::Range {
                in_start,
                out_start,
                length,
                ..
            } => {
                if partition_id >= *out_start && partition_id < out_start + length {
                    vec![partition_id - out_start + in_start]
                } else {
                    Vec::new()
                }
            }
            Dependency::Shuffle(_) => Vec::new(),
            Dependency::CoalescedSplitDep { rdd, .. } => rdd
                .splits()
                .into_iter()
                .enumerate()
                .find(|(i, _)| i == &partition_id)
                .and_then(|(_, p)| CoalescedRddSplit::downcasting(p).ok())
                .map(|split| split.parent_indices)
                .unwrap_or_default(),
        }
    }

    /// Get the RDD base for this dependency
    pub fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        match self {
            Dependency::OneToOne { rdd_base } => rdd_base.clone(),
            Dependency::Range { rdd_base, .. } => rdd_base.clone(),
            Dependency::Shuffle(dep) => dep.get_rdd_base(),
            Dependency::CoalescedSplitDep { rdd: _, prev } => prev.clone(),
        }
    }

    /// Check if this is a shuffle dependency
    pub fn is_shuffle(&self) -> bool {
        matches!(self, Dependency::Shuffle(_))
    }

    /// Get the dependency type
    pub fn dependency_type(&self) -> DependencyType {
        match self {
            Dependency::OneToOne { .. }
            | Dependency::Range { .. }
            | Dependency::CoalescedSplitDep { .. } => DependencyType::Narrow,
            Dependency::Shuffle(_) => DependencyType::Shuffle,
        }
    }

    /// Get shuffle ID (only for shuffle dependencies)
    pub fn get_shuffle_id(&self) -> Option<usize> {
        match self {
            Dependency::Shuffle(dep) => Some(dep.get_shuffle_id()),
            _ => None,
        }
    }

    /// Get shuffle dependency box (only for shuffle dependencies)
    pub fn get_shuffle_dep(&self) -> Option<&ShuffleDependencyBox> {
        match self {
            Dependency::Shuffle(dep) => Some(dep),
            _ => None,
        }
    }
}

/// Type-erased wrapper for ShuffleDependency that can be cloned and stored
/// This eliminates the need for ShuffleDependencyTrait and follows the same pattern as ResultTaskBox
#[derive(Clone)]
pub struct ShuffleDependencyBox {
    pub shuffle_id: usize,
    pub rdd_base: Arc<dyn RddBase>,
    pub is_cogroup: bool,
    pub num_output_partitions: usize,
    /// Identifies the registered `SHUFFLE_MAP_REGISTRY` handler for the `(K, V)` type pair.
    ///
    /// Set to `std::any::type_name::<(K, V)>()` when the dependency is created.
    /// Workers look this up to find the correctly-typed shuffle-write function.
    /// Required for distributed shuffle; unused in local mode.
    pub type_id: &'static str,
    /// Encodes all parent RDD partitions as rkyv bytes (one `Vec<u8>` per partition).
    ///
    /// Used by the driver in distributed mode to build the shuffle-map `TaskEnvelope`
    /// sent to workers. Captures the typed parent RDD so encoding is exact.
    pub encode_partitions: Arc<dyn Fn() -> Result<Vec<Vec<u8>>, String> + Send + Sync>,
    // Type-erased executor: stores a closure that runs the actual shuffle task (local mode)
    do_shuffle: Arc<dyn Fn(usize) -> String + Send + Sync>,
}

impl ShuffleDependencyBox {
    /// Get the shuffle ID
    pub fn get_shuffle_id(&self) -> usize {
        self.shuffle_id
    }

    /// Get the RDD base (type-erased)
    pub fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }

    /// Check if this is a cogroup operation
    pub fn is_cogroup(&self) -> bool {
        self.is_cogroup
    }

    /// Execute the shuffle task for a given partition
    /// Returns the server URI where shuffle data is stored
    pub fn do_shuffle_task(&self, partition: usize) -> String {
        (self.do_shuffle)(partition)
    }

    /// Get the number of output partitions
    pub fn get_num_output_partitions(&self) -> usize {
        self.num_output_partitions
    }
}

impl PartialOrd for ShuffleDependencyBox {
    fn partial_cmp(&self, other: &ShuffleDependencyBox) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ShuffleDependencyBox {
    fn eq(&self, other: &ShuffleDependencyBox) -> bool {
        self.shuffle_id == other.shuffle_id
    }
}

impl Eq for ShuffleDependencyBox {}

impl Ord for ShuffleDependencyBox {
    fn cmp(&self, other: &ShuffleDependencyBox) -> Ordering {
        self.shuffle_id.cmp(&other.shuffle_id)
    }
}

/// Generic shuffle dependency with full type information
/// This struct is fully typed and doesn't require iterator_any or unsafe downcasting
pub struct ShuffleDependency<K: Data, V: Data, C: Data> {
    shuffle_id: usize,
    is_cogroup_flag: bool,
    /// Typed RDD - we know it produces (K, V) tuples
    rdd: Arc<dyn crate::rdd::Rdd<Item = (K, V)>>,
    /// Typed aggregator
    aggregator: Arc<Aggregator<K, V, C>>,
    /// Partitioner for determining output partitions
    partitioner: Partitioner,
    _phantom: std::marker::PhantomData<(K, V, C)>,
}

impl<K, V, C> ShuffleDependency<K, V, C>
where
    K: Eq + Hash + Encode + Clone,
    C: Encode + Clone,
    K: Data,
    V: Data + Clone,
    C: Data,
{
    /// Create a new shuffle dependency
    pub fn new(
        shuffle_id: usize,
        is_cogroup: bool,
        rdd: Arc<dyn crate::rdd::Rdd<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        partitioner: Partitioner,
    ) -> Arc<Self> {
        Arc::new(ShuffleDependency {
            shuffle_id,
            is_cogroup_flag: is_cogroup,
            rdd,
            aggregator,
            partitioner,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Execute shuffle task with full type information - NO unsafe code needed!
    fn do_shuffle_task_typed(&self, partition: usize) -> String {
        log::debug!(
            "executing shuffle task #{} for partition #{}",
            self.shuffle_id,
            partition
        );

        let splits = self.rdd.get_rdd_base().splits();
        let split = splits[partition].clone();
        let num_output_splits = self.partitioner.get_num_of_partitions();

        log::debug!("is cogroup rdd: {}", self.is_cogroup_flag);
        log::debug!("number of output splits: {}", num_output_splits);

        let mut buckets: Vec<HashMap<K, C>> =
            (0..num_output_splits).map(|_| HashMap::new()).collect();

        log::debug!(
            "before iterating while executing shuffle map task for partition #{}",
            partition
        );

        // Use typed compute() - no iterator_any, no unsafe downcasting!
        match self.rdd.compute(split) {
            Ok(iter) => {
                for (count, (k, v)) in iter.enumerate() {
                    if count == 0 {
                        log::debug!(
                            "iterating inside dependency map task: key: {:?}, value: {:?}",
                            k,
                            v
                        );
                    }

                    let bucket_id = self.partitioner.get_partition(&k);
                    let bucket = &mut buckets[bucket_id];

                    if let Some(old_v) = bucket.get_mut(&k) {
                        (self.aggregator.merge_value)(old_v, v);
                    } else {
                        bucket.insert(k, (self.aggregator.create_combiner)(v));
                    }
                }
            }
            Err(e) => {
                log::error!("Error computing RDD partition: {:?}", e);
                return "".to_string();
            }
        }

        // Serialize and store shuffle data
        for (i, bucket) in buckets.into_iter().enumerate() {
            let set: Vec<(K, C)> = bucket.into_iter().collect();
            let config = bincode::config::standard();
            match bincode::encode_to_vec(&set, config) {
                Ok(ser_bytes) => {
                    log::debug!(
                        "shuffle dependency map task set from bucket #{} in shuffle id #{}, partition #{}: {:?}",
                        i,
                        self.shuffle_id,
                        partition,
                        set.first()
                    );
                    if let Some(cache) = crate::env::SHUFFLE_CACHE.get() {
                        cache.insert((self.shuffle_id, partition, i), ser_bytes);
                    } else {
                        log::warn!(
                            "SHUFFLE_CACHE not initialized — shuffle data for ({},{},{}) dropped",
                            self.shuffle_id, partition, i
                        );
                    }
                }
                Err(e) => {
                    log::error!("Error serializing shuffle data: {:?}", e);
                }
            }
        }

        log::debug!(
            "returning shuffle address for shuffle task #{}",
            self.shuffle_id
        );

        crate::env::SHUFFLE_SERVER_URI
            .get()
            .cloned()
            .unwrap_or_default()
    }
}

/// Convert typed ShuffleDependency to type-erased ShuffleDependencyBox
impl<K, V, C> From<Arc<ShuffleDependency<K, V, C>>> for ShuffleDependencyBox
where
    K: Data + Eq + Hash + Encode + Clone,
    V: Data + Clone,
    C: Data + Encode + Clone,
    Vec<(K, V)>: WireEncode,
{
    fn from(dep: Arc<ShuffleDependency<K, V, C>>) -> Self {
        let cloned = Arc::clone(&dep);

        // Capture the typed RDD so partitions can be rkyv-encoded for workers.
        let enc_rdd = dep.rdd.clone();
        let encode_partitions: Arc<dyn Fn() -> Result<Vec<Vec<u8>>, String> + Send + Sync> =
            Arc::new(move || {
                enc_rdd
                    .splits()
                    .iter()
                    .map(|split| {
                        let items: Vec<(K, V)> = enc_rdd
                            .compute(split.clone())
                            .map_err(|e| format!("encode partition: {e}"))?
                            .collect();
                        items.encode_wire().map_err(|e| format!("encode wire: {e}"))
                    })
                    .collect()
            });

        ShuffleDependencyBox {
            shuffle_id: dep.shuffle_id,
            rdd_base: dep.rdd.get_rdd_base(),
            is_cogroup: dep.is_cogroup_flag,
            num_output_partitions: dep.partitioner.get_num_of_partitions(),
            type_id: std::any::type_name::<(K, V)>(),
            encode_partitions,
            do_shuffle: Arc::new(move |partition| cloned.do_shuffle_task_typed(partition)),
        }
    }
}
