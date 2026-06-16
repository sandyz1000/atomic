use bincode::Encode;

use crate::aggregator::Aggregator;
use crate::data::Data;
use crate::distributed::{PipelineOp, WireEncode};
use crate::error::BaseResult;
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
    OneToOne {
        rdd_base: Arc<dyn RddBase>,
    },
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
    Shuffle(Arc<ShuffleDependencyBox>),
}

impl Dependency {
    pub fn new_one_to_one(rdd_base: Arc<dyn RddBase>) -> Self {
        Dependency::OneToOne { rdd_base }
    }

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

    pub fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        match self {
            Dependency::OneToOne { rdd_base } => rdd_base.clone(),
            Dependency::Range { rdd_base, .. } => rdd_base.clone(),
            Dependency::Shuffle(dep) => dep.get_rdd_base(),
            Dependency::CoalescedSplitDep { rdd: _, prev } => prev.clone(),
        }
    }

    pub fn is_shuffle(&self) -> bool {
        matches!(self, Dependency::Shuffle(_))
    }

    pub fn dependency_type(&self) -> DependencyType {
        match self {
            Dependency::OneToOne { .. }
            | Dependency::Range { .. }
            | Dependency::CoalescedSplitDep { .. } => DependencyType::Narrow,
            Dependency::Shuffle(_) => DependencyType::Shuffle,
        }
    }

    pub fn get_shuffle_id(&self) -> Option<usize> {
        match self {
            Dependency::Shuffle(dep) => Some(dep.get_shuffle_id()),
            _ => None,
        }
    }

    pub fn get_shuffle_dep(&self) -> Option<&ShuffleDependencyBox> {
        match self {
            Dependency::Shuffle(dep) => Some(dep),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct ShuffleDependencyBox {
    pub shuffle_id: usize,
    pub rdd_base: Arc<dyn RddBase>,
    pub is_cogroup: bool,
    pub num_output_partitions: usize,
    /// Serializable partitioner spec, shipped to workers so they partition shuffle output using
    /// the RDD's real partitioner (e.g. range for `sort_by_key`) instead of plain hash.
    pub partitioner_spec: crate::partitioner::PartitionerSchema,
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
    pub encode_partitions: Arc<dyn Fn() -> BaseResult<Vec<Vec<u8>>> + Send + Sync>,
    /// Pipeline ops that must run on workers *before* the ShuffleMap op.
    ///
    /// Non-empty when a staged pipeline (from `map_task`/`flat_map_task`) precedes
    /// the shuffle stage. `encode_partitions` then returns the staged source partitions
    /// whose element type matches the first op's input type.
    pub preceding_ops: Vec<PipelineOp>,
    // Type-erased executor: stores a closure that runs the actual shuffle task (local mode)
    do_shuffle: Arc<dyn Fn(usize) -> String + Send + Sync>,
}

impl ShuffleDependencyBox {
    pub fn get_shuffle_id(&self) -> usize {
        self.shuffle_id
    }

    pub fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }

    pub fn is_cogroup(&self) -> bool {
        self.is_cogroup
    }

    pub fn do_shuffle_task(&self, partition: usize) -> String {
        (self.do_shuffle)(partition)
    }

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

/// Type-erased key comparator for the sort-shuffle path. Built by the constructing op
/// where `K: Ord` is statically available (e.g. `sort_by_key`); `None` means the legacy
/// unsorted layout. Stored erased so `ShuffleDependency`'s `K` bound stays `Hash`-only.
pub type KeyComparator<K> = Arc<dyn Fn(&K, &K) -> Ordering + Send + Sync>;

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
    /// When `Some`, each reduce-partition bucket is sorted by key before it is written,
    /// producing sorted runs the reduce side can k-way merge (sort-shuffle).
    comparator: Option<KeyComparator<K>>,
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
    /// Create a new shuffle dependency (legacy unsorted layout).
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
            comparator: None,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Create a sort-shuffle dependency: each reduce-partition bucket is sorted by `comparator`
    /// before it is written, so the reduce side can k-way merge sorted runs. The caller supplies
    /// the comparator from a site where `K: Ord` (e.g. `Arc::new(|a, b| a.cmp(b))`).
    pub fn new_sorted(
        shuffle_id: usize,
        is_cogroup: bool,
        rdd: Arc<dyn crate::rdd::Rdd<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        partitioner: Partitioner,
        comparator: KeyComparator<K>,
    ) -> Arc<Self> {
        Arc::new(ShuffleDependency {
            shuffle_id,
            is_cogroup_flag: is_cogroup,
            rdd,
            aggregator,
            partitioner,
            comparator: Some(comparator),
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

        // Serialize each reduce-partition bucket, then store via the consolidated
        // (sort-shuffle) layout for wide shuffles or the legacy per-bucket layout otherwise.
        let cache = match crate::env::get_shuffle_cache() {
            Some(c) => c,
            None => {
                log::warn!(
                    "SHUFFLE_CACHE not initialized — shuffle data for shuffle #{} partition #{} dropped",
                    self.shuffle_id,
                    partition
                );
                return crate::env::get_shuffle_server_uri().unwrap_or_default();
            }
        };

        let config = bincode::config::standard();
        let encoded: Vec<Vec<u8>> = buckets
            .into_iter()
            .map(|bucket| {
                let mut set: Vec<(K, C)> = bucket.into_iter().collect();
                // Sort-shuffle: emit each bucket as a sorted run so the reduce side can k-way merge.
                if let Some(cmp) = &self.comparator {
                    set.sort_by(|a, b| cmp(&a.0, &b.0));
                }
                bincode::encode_to_vec(&set, config).unwrap_or_else(|e| {
                    log::error!("Error serializing shuffle bucket: {:?}", e);
                    bincode::encode_to_vec(Vec::<(K, C)>::new(), config).unwrap_or_default()
                })
            })
            .collect();

        if num_output_splits >= crate::env::sort_shuffle_threshold() {
            if let Err(e) = crate::shuffle::cache::write_consolidated(
                cache.as_ref(),
                self.shuffle_id,
                partition,
                &encoded,
            ) {
                log::error!("write_consolidated failed: {e}");
            }
        } else {
            for (i, ser_bytes) in encoded.into_iter().enumerate() {
                cache.insert((self.shuffle_id, partition, i), ser_bytes);
            }
        }

        log::debug!(
            "returning shuffle address for shuffle task #{}",
            self.shuffle_id
        );

        crate::env::get_shuffle_server_uri().unwrap_or_default()
    }
}

/// Convert typed ShuffleDependency to type-erased ShuffleDependencyBox.
///
/// Sets `type_id` to `std::any::type_name::<(K, V)>()`. This is a fallback path;
/// prefer [`ShuffleDependencyBox::from_typed_with_key`] when a stable key is
/// available from the compile-time `SHUFFLE_KEY_REGISTRY`.
impl<K, V, C> From<Arc<ShuffleDependency<K, V, C>>> for ShuffleDependencyBox
where
    K: Data + Eq + Hash + Encode + Clone,
    V: Data + Clone,
    C: Data + Encode + Clone,
    Vec<(K, V)>: WireEncode,
{
    fn from(dep: Arc<ShuffleDependency<K, V, C>>) -> Self {
        ShuffleDependencyBox::from_typed_with_key(dep, std::any::type_name::<(K, V)>())
    }
}

impl ShuffleDependencyBox {
    /// Build a type-erased [`ShuffleDependencyBox`] from a typed [`ShuffleDependency`],
    /// using an explicit, stable `shuffle_key` string as the dispatch key.
    ///
    /// The `shuffle_key` is embedded in the `ShuffleMap` pipeline-op payload sent to
    /// workers, which look it up in their `SHUFFLE_MAP_REGISTRY`. Using a
    /// `stringify!`-based key (from `register_shuffle_map!`) instead of `type_name`
    /// avoids instability across Rust toolchain versions.
    ///
    /// Call sites in `atomic-compute` obtain the key from `SHUFFLE_KEY_REGISTRY`
    /// (keyed by `TypeId::of::<(K, V)>()`) before calling this constructor.
    pub fn from_typed_with_key<K, V, C>(
        dep: Arc<ShuffleDependency<K, V, C>>,
        shuffle_key: &'static str,
    ) -> Self
    where
        K: Data + Eq + Hash + Encode + Clone,
        V: Data + Clone,
        C: Data + Encode + Clone,
        Vec<(K, V)>: WireEncode,
    {
        let cloned = Arc::clone(&dep);

        let enc_rdd = dep.rdd.clone();
        let encode_partitions: Arc<dyn Fn() -> BaseResult<Vec<Vec<u8>>> + Send + Sync> =
            Arc::new(move || {
                enc_rdd
                    .splits()
                    .iter()
                    .map(|split| {
                        let items: Vec<(K, V)> = enc_rdd.compute(split.clone())?.collect();
                        items.encode_wire()
                    })
                    .collect()
            });

        ShuffleDependencyBox {
            shuffle_id: dep.shuffle_id,
            rdd_base: dep.rdd.get_rdd_base(),
            is_cogroup: dep.is_cogroup_flag,
            num_output_partitions: dep.partitioner.get_num_of_partitions(),
            partitioner_spec: dep.partitioner.to_spec(),
            type_id: shuffle_key,
            encode_partitions,
            preceding_ops: vec![],
            do_shuffle: Arc::new(move |partition| cloned.do_shuffle_task_typed(partition)),
        }
    }

    /// Override `encode_partitions` and set `preceding_ops` for staged-pipeline shuffles.
    ///
    /// Used when a `_task`-based pipeline precedes a shuffle: the staged source partitions
    /// (already rkyv-encoded) become the worker input, and the staged ops run on workers
    /// before the ShuffleMap op.
    pub fn with_staged_pipeline(
        mut self,
        source_partitions: Vec<Vec<u8>>,
        preceding_ops: Vec<PipelineOp>,
    ) -> Self {
        self.encode_partitions = Arc::new(move || Ok(source_partitions.clone()));
        self.preceding_ops = preceding_ops;
        self
    }
}
