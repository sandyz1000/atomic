use bincode::Encode;

use crate::aggregator::Aggregator;
use crate::data::Data;
use crate::distributed::{Step, WireEncode};
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
    Shuffle(Arc<ErasedShuffleDependency>),
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
                    vec![(partition_id - out_start) + in_start]
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

    pub fn get_shuffle_dep(&self) -> Option<&ErasedShuffleDependency> {
        match self {
            Dependency::Shuffle(dep) => Some(dep),
            _ => None,
        }
    }
}

/// Plain, cloneable description of a shuffle boundary — everything the scheduler and the
/// wire layer need to know about a shuffle without its `K`/`V`/`C` generics. The behaviour
/// (encoding parent partitions, running the map task) lives behind [`ShuffleExecutor`].
#[derive(Clone)]
pub struct ShuffleSpec {
    pub shuffle_id: usize,
    pub rdd_base: Arc<dyn RddBase>,
    pub is_cogroup: bool,
    pub num_output_partitions: usize,
    /// Serializable partitioner spec, shipped to workers so they partition shuffle output using
    /// the RDD's real partitioner (e.g. range for `sort_by_key`) instead of plain hash.
    pub partitioner_spec: crate::partitioner::PartitionerSchema,
    /// Identifies the registered `SHUFFLE_MAP_REGISTRY` handler for the `(K, V)` type pair —
    /// the `register_shuffle_map!` key the worker looks up. Required for distributed shuffle.
    pub type_id: &'static str,
    /// Ops that run on workers *before* the ShuffleMap op, non-empty when a `_task` pipeline
    /// precedes the shuffle. Paired with [`staged_partitions`](Self::staged_partitions).
    pub preceding_steps: Vec<Step>,
    /// Pre-encoded worker input for a staged-pipeline shuffle. When `Some`, these bytes are the
    /// shuffle input instead of encoding the parent RDD through the executor.
    pub staged_partitions: Option<Vec<Vec<u8>>>,
}

/// Type-erased behaviour of a shuffle: the two operations that need the concrete `K`/`V`/`C`,
/// implemented by [`ShuffleDependency`] and stored as a trait object so the generics drop away.
pub trait ShuffleExecutor: Send + Sync {
    /// Encode every parent RDD partition as rkyv bytes (one `Vec<u8>` per partition) — the
    /// non-staged distributed input.
    fn encode_parent_partitions(&self) -> BaseResult<Vec<Vec<u8>>>;
    /// Run the shuffle-map task for one partition in-process (local mode), returning the
    /// shuffle server URI that now holds its buckets.
    fn run_local(&self, partition_id: usize) -> String;
}

/// The type-erased form of a [`ShuffleDependency`] stored in the RDD DAG: its [`ShuffleSpec`]
/// (data) plus a [`ShuffleExecutor`] (behaviour).
#[derive(Clone)]
pub struct ErasedShuffleDependency {
    pub spec: ShuffleSpec,
    exec: Arc<dyn ShuffleExecutor>,
}

impl ErasedShuffleDependency {
    pub fn get_shuffle_id(&self) -> usize {
        self.spec.shuffle_id
    }

    pub fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.spec.rdd_base.clone()
    }

    pub fn is_cogroup(&self) -> bool {
        self.spec.is_cogroup
    }

    pub fn get_num_output_partitions(&self) -> usize {
        self.spec.num_output_partitions
    }

    /// Run the shuffle-map task for `partition` in-process (local mode).
    pub fn do_shuffle_task(&self, partition: usize) -> String {
        self.exec.run_local(partition)
    }

    /// Worker input for the distributed shuffle-map stage: the staged pipeline's pre-encoded
    /// partitions when present, otherwise the parent RDD encoded on demand.
    pub fn encode_partitions(&self) -> BaseResult<Vec<Vec<u8>>> {
        match &self.spec.staged_partitions {
            Some(parts) => Ok(parts.clone()),
            None => self.exec.encode_parent_partitions(),
        }
    }
}

impl PartialOrd for ErasedShuffleDependency {
    fn partial_cmp(&self, other: &ErasedShuffleDependency) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ErasedShuffleDependency {
    fn eq(&self, other: &ErasedShuffleDependency) -> bool {
        self.spec.shuffle_id == other.spec.shuffle_id
    }
}

impl Eq for ErasedShuffleDependency {}

impl Ord for ErasedShuffleDependency {
    fn cmp(&self, other: &ErasedShuffleDependency) -> Ordering {
        self.spec.shuffle_id.cmp(&other.spec.shuffle_id)
    }
}

/// Type-erased key comparator for the sort-shuffle path. Built by the constructing op
/// where `K: Ord` is statically available (e.g. `sort_by_key`); `None` means the legacy
/// unsorted layout. Stored erased so `ShuffleDependency`'s `K` bound stays `Hash`-only.
pub type KeyComparator<K> = Arc<dyn Fn(&K, &K) -> Ordering + Send + Sync>;

/// Generic shuffle dependency with full type information
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

    /// Execute shuffle task with full type information
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

/// Convert typed ShuffleDependency to type-erased ErasedShuffleDependency.
///
/// Sets `type_id` to `std::any::type_name::<(K, V)>()`. This is a fallback path;
/// prefer [`ErasedShuffleDependency::from_typed_with_key`] when a stable key is
/// available from the compile-time `SHUFFLE_KEY_REGISTRY`.
impl<K, V, C> From<Arc<ShuffleDependency<K, V, C>>> for ErasedShuffleDependency
where
    K: Data + Eq + Hash + Encode + Clone,
    V: Data + Clone,
    C: Data + Encode + Clone,
    Vec<(K, V)>: WireEncode,
{
    fn from(dep: Arc<ShuffleDependency<K, V, C>>) -> Self {
        ErasedShuffleDependency::from_typed_with_key(dep, std::any::type_name::<(K, V)>())
    }
}

impl ErasedShuffleDependency {
    /// Build a type-erased [`ErasedShuffleDependency`] from a typed [`ShuffleDependency`],
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
        let spec = ShuffleSpec {
            shuffle_id: dep.shuffle_id,
            rdd_base: dep.rdd.get_rdd_base(),
            is_cogroup: dep.is_cogroup_flag,
            num_output_partitions: dep.partitioner.get_num_of_partitions(),
            partitioner_spec: dep.partitioner.to_spec(),
            type_id: shuffle_key,
            preceding_steps: vec![],
            staged_partitions: None,
        };
        ErasedShuffleDependency { spec, exec: dep }
    }

    /// Attach a staged `_task` pipeline that precedes the shuffle: `source_partitions`
    /// (already rkyv-encoded) become the worker input and `preceding_steps` run before the
    /// ShuffleMap op, in place of encoding the parent RDD.
    pub fn with_staged_pipeline(
        mut self,
        source_partitions: Vec<Vec<u8>>,
        preceding_steps: Vec<Step>,
    ) -> Self {
        self.spec.staged_partitions = Some(source_partitions);
        self.spec.preceding_steps = preceding_steps;
        self
    }
}

impl<K, V, C> ShuffleExecutor for ShuffleDependency<K, V, C>
where
    K: Data + Eq + Hash + Encode + Clone,
    V: Data + Clone,
    C: Data + Encode + Clone,
    Vec<(K, V)>: WireEncode,
{
    fn encode_parent_partitions(&self) -> BaseResult<Vec<Vec<u8>>> {
        self.rdd
            .splits()
            .iter()
            .map(|split| {
                let items: Vec<(K, V)> = self.rdd.compute(split.clone())?.collect();
                items.encode_wire()
            })
            .collect()
    }

    fn run_local(&self, partition_id: usize) -> String {
        self.do_shuffle_task_typed(partition_id)
    }
}
