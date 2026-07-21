//! The shuffle-dependency subsystem.
//!
//! A shuffle has a **typed source** ([`TypedShuffle<K,V,C>`]) the RDD layer builds. It
//! projects into an erased **[`ShuffleDependency`]** — the value the DAG holds — which reads all
//! its metadata and behaviour through the erased [`ShuffleExecutor`] (impl'd by the typed
//! source); it also carries the few bits the source can't provide (the worker dispatch key and
//! any staged pipeline). See `notes/shuffle-dependency-lineage.md`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use bincode::Encode;

use crate::aggregator::Aggregator;
use crate::data::Data;
use crate::distributed::{Step, WireEncode};
use crate::error::BaseResult;
use crate::partitioner::{Partitioner, PartitionerSchema};
use crate::rdd::RddBase;

/// Type-erased behaviour **and** metadata of a shuffle: everything the DAG/scheduler need
/// without the concrete `K`/`V`/`C`. Implemented by [`TypedShuffle`] and stored as a trait
/// object so the generics drop away. Reading through this keeps a single source of truth (the
/// typed source) instead of snapshotting its fields.
pub trait ShuffleExecutor: Send + Sync {
    /// Stable id of this shuffle, assigned by the context.
    fn shuffle_id(&self) -> usize;
    /// Whether this shuffle backs a cogroup.
    fn is_cogroup(&self) -> bool;
    /// The parent RDD, type-erased.
    fn rdd_base(&self) -> Arc<dyn RddBase>;
    /// Number of reduce-side output partitions.
    fn num_output_partitions(&self) -> usize;
    /// Serializable partitioner descriptor shipped to workers (see [`PartitionerSchema`]).
    fn partitioner_spec(&self) -> PartitionerSchema;
    /// Encode every parent RDD partition as rkyv bytes (one `Vec<u8>` per partition) — the
    /// non-staged distributed input.
    fn encode_parent_partitions(&self) -> BaseResult<Vec<Vec<u8>>>;
    /// Run the shuffle-map task for one partition in-process (local mode), returning the
    /// shuffle server URI that now holds its buckets.
    fn run_local(&self, partition_id: usize) -> String;
}

/// The shuffle dependency the RDD DAG holds: an erased [`ShuffleExecutor`] (behaviour +
/// metadata of the typed source) plus the few bits the DAG attaches at erasure that the source
/// can't provide. Built from a [`TypedShuffle`] whose `K`/`V`/`C` are erased behind the `dyn`.
#[derive(Clone)]
pub struct ShuffleDependency {
    /// Identifies the registered `SHUFFLE_MAP_REGISTRY` handler for the `(K, V)` type pair —
    /// the `register_shuffle_map!` key the worker looks up. Required for distributed shuffle.
    pub type_id: &'static str,
    /// Steps that run on workers *before* the ShuffleMap op, non-empty when a `_task` pipeline
    /// precedes the shuffle. Paired with [`staged_partitions`](Self::staged_partitions).
    pub preceding_steps: Vec<Step>,
    /// Pre-encoded worker input for a staged-pipeline shuffle. When `Some`, these bytes are the
    /// shuffle input instead of encoding the parent RDD through the executor.
    pub staged_partitions: Option<Vec<Vec<u8>>>,
    pub exec: Arc<dyn ShuffleExecutor>,
}

impl ShuffleDependency {
    pub fn get_shuffle_id(&self) -> usize {
        self.exec.shuffle_id()
    }

    pub fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.exec.rdd_base()
    }

    pub fn is_cogroup(&self) -> bool {
        self.exec.is_cogroup()
    }

    pub fn get_num_output_partitions(&self) -> usize {
        self.exec.num_output_partitions()
    }

    /// Serializable partitioner descriptor for the workers.
    pub fn partitioner_spec(&self) -> PartitionerSchema {
        self.exec.partitioner_spec()
    }

    /// Run the shuffle-map task for `partition` in-process (local mode).
    pub fn do_shuffle_task(&self, partition: usize) -> String {
        self.exec.run_local(partition)
    }

    /// Worker input for the distributed shuffle-map stage: the staged pipeline's pre-encoded
    /// partitions when present, otherwise the parent RDD encoded on demand.
    pub fn encode_partitions(&self) -> BaseResult<Vec<Vec<u8>>> {
        match &self.staged_partitions {
            Some(parts) => Ok(parts.clone()),
            None => self.exec.encode_parent_partitions(),
        }
    }
}

impl PartialOrd for ShuffleDependency {
    fn partial_cmp(&self, other: &ShuffleDependency) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ShuffleDependency {
    fn eq(&self, other: &ShuffleDependency) -> bool {
        self.exec.shuffle_id() == other.exec.shuffle_id()
    }
}

impl Eq for ShuffleDependency {}

impl Ord for ShuffleDependency {
    fn cmp(&self, other: &ShuffleDependency) -> Ordering {
        self.exec.shuffle_id().cmp(&other.exec.shuffle_id())
    }
}

/// Type-erased key comparator for the sort-shuffle path. Built by the constructing op
/// where `K: Ord` is statically available (e.g. `sort_by_key`); `None` means the legacy
/// unsorted layout. Stored erased so `TypedShuffle`'s `K` bound stays `Hash`-only.
pub type KeyComparator<K> = Arc<dyn Fn(&K, &K) -> Ordering + Send + Sync>;

/// Generic shuffle dependency with full type information
pub struct TypedShuffle<K: Data, V: Data, C: Data> {
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

impl<K, V, C> TypedShuffle<K, V, C>
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
        Arc::new(TypedShuffle {
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
        Arc::new(TypedShuffle {
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

/// Erase a [`TypedShuffle`] into the DAG-facing [`ShuffleDependency`].
///
/// Sets `type_id` to `std::any::type_name::<(K, V)>()`. This is a fallback path;
/// prefer [`ShuffleDependency::from_typed_with_key`] when a stable key is
/// available from the compile-time `SHUFFLE_KEY_REGISTRY`.
impl<K, V, C> From<Arc<TypedShuffle<K, V, C>>> for ShuffleDependency
where
    K: Data + Eq + Hash + Encode + Clone,
    V: Data + Clone,
    C: Data + Encode + Clone,
    Vec<(K, V)>: WireEncode,
{
    fn from(dep: Arc<TypedShuffle<K, V, C>>) -> Self {
        ShuffleDependency::from_typed_with_key(dep, std::any::type_name::<(K, V)>())
    }
}

impl ShuffleDependency {
    /// Build a type-erased [`ShuffleDependency`] from a typed [`TypedShuffle`],
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
        dep: Arc<TypedShuffle<K, V, C>>,
        shuffle_key: &'static str,
    ) -> Self
    where
        K: Data + Eq + Hash + Encode + Clone,
        V: Data + Clone,
        C: Data + Encode + Clone,
        Vec<(K, V)>: WireEncode,
    {
        ShuffleDependency {
            type_id: shuffle_key,
            preceding_steps: vec![],
            staged_partitions: None,
            exec: dep,
        }
    }

    /// Attach a staged `_task` pipeline that precedes the shuffle: `source_partitions`
    /// (already rkyv-encoded) become the worker input and `preceding_steps` run before the
    /// ShuffleMap op, in place of encoding the parent RDD.
    pub fn with_staged_pipeline(
        mut self,
        source_partitions: Vec<Vec<u8>>,
        preceding_steps: Vec<Step>,
    ) -> Self {
        self.staged_partitions = Some(source_partitions);
        self.preceding_steps = preceding_steps;
        self
    }
}

impl<K, V, C> ShuffleExecutor for TypedShuffle<K, V, C>
where
    K: Data + Eq + Hash + Encode + Clone,
    V: Data + Clone,
    C: Data + Encode + Clone,
    Vec<(K, V)>: WireEncode,
{
    fn shuffle_id(&self) -> usize {
        self.shuffle_id
    }

    fn is_cogroup(&self) -> bool {
        self.is_cogroup_flag
    }

    fn rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd.get_rdd_base()
    }

    fn num_output_partitions(&self) -> usize {
        self.partitioner.get_num_of_partitions()
    }

    fn partitioner_spec(&self) -> PartitionerSchema {
        self.partitioner.to_spec()
    }

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
