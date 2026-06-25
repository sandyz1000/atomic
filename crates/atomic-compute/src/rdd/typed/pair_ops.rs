use super::*;

impl<K, V> TypedRdd<(K, V)>
where
    K: Data + Eq + std::hash::Hash + Clone,
    V: Data + Clone,
{
    pub fn keys(self) -> TypedRdd<K> {
        self.map_rdd(|id, rdd| MapperRdd::new(id, rdd, |(k, _v)| k))
    }

    pub fn values(self) -> TypedRdd<V> {
        self.map_rdd(|id, rdd| MapperRdd::new(id, rdd, |(_k, v)| v))
    }

    /// Combine values for each key using three aggregation functions.
    ///
    /// - `create_combiner(V) -> C`: starts a combiner for the first value of a key.
    /// - `merge_value(C, V) -> C`: merges a new value into an existing combiner.
    /// - `merge_combiners(C, C) -> C`: merges two combiners (for cross-partition merging).
    ///
    /// This is the generalisation of `reduce_by_key` (`C = V`) and `group_by_key` (`C = Vec<V>`).
    pub fn combine_by_key<C, CC, MV, MC>(
        self,
        create_combiner: CC,
        merge_value: MV,
        merge_combiners: MC,
        num_partitions: usize,
    ) -> TypedRdd<(K, C)>
    where
        C: Data + Clone + bincode::Encode + bincode::Decode<()>,
        CC: Fn(V) -> C + Clone + Send + Sync + 'static,
        MV: Fn(C, V) -> C + Clone + Send + Sync + 'static,
        MC: Fn(C, C) -> C + Clone + Send + Sync + 'static,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        use crate::rdd::shuffled::ShuffledRdd;
        use atomic_data::aggregator::Aggregator;
        use atomic_data::shuffle::fetcher::ShuffleFetcher;

        let mv2 = merge_value.clone();
        let mc2 = merge_combiners.clone();
        let aggregator = Arc::new(Aggregator::<K, V, C>::new(
            Arc::new(move |v: V| create_combiner(v)),
            Arc::new(move |c: &mut C, v: V| *c = mv2(c.clone(), v)),
            Arc::new(move |c1: &mut C, c2: C| *c1 = mc2(c1.clone(), c2)),
        ));

        let partitioner = Partitioner::hash::<K>(num_partitions.max(1));
        let shuffle_id = self.context.new_shuffle_id();
        let rdd_id = self.context.new_rdd_id();
        let tracker = atomic_data::env::get_map_output_tracker()
            .unwrap_or_else(|| Arc::new(atomic_data::shuffle::MapOutputTracker::default()));
        let fetcher = Arc::new(ShuffleFetcher::new(tracker));

        let staged_info = if self.context.is_distributed() {
            self.staged
                .as_ref()
                .map(|s| (s.source_partitions.clone(), s.ops.clone()))
        } else {
            None
        };

        let shuffled = ShuffledRdd::<K, V, C>::new_with_staged(
            rdd_id,
            shuffle_id,
            self.rdd,
            aggregator,
            partitioner,
            fetcher,
            staged_info,
            None,
        );
        TypedRdd::new(Arc::new(shuffled), self.context)
    }

    /// Re-partition this pair RDD using a user-defined `CustomPartitioner`.
    ///
    /// All existing `(K, V)` pairs are preserved; only the partition assignment changes.
    /// Triggers a shuffle.
    pub fn partition_by<P>(self, partitioner: P) -> TypedRdd<(K, V)>
    where
        P: atomic_data::partitioner::CustomPartitioner + 'static,
        V: bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        let p = Partitioner::from_custom(partitioner);
        self.combine_by_key_with_partitioner(|v| v, |_, v| v, |c, _| c, p, None)
    }

    /// Re-partition using a registered [`NamedPartitioner`] — the distributed-capable
    /// counterpart to [`partition_by`](Self::partition_by).
    ///
    /// Because the partitioner is shipped to workers by name (it must be registered
    /// with `atomic_compute::register_partitioner!(P)`), the custom partitioning is
    /// applied on the workers rather than degrading to hash. `partitioner`'s only
    /// shipped state is its partition count; the worker rebuilds it via `P::create`.
    pub fn partition_by_named<P>(self, partitioner: P) -> TypedRdd<(K, V)>
    where
        P: atomic_data::partitioner::NamedPartitioner,
        V: bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        let p = Partitioner::from_named::<P>(partitioner.num_partitions());
        self.combine_by_key_with_partitioner(|v| v, |_, v| v, |c, _| c, p, None)
    }

    /// Internal: `combine_by_key` with an explicit `Partitioner` instead of hash.
    pub(crate) fn combine_by_key_with_partitioner<C, CC, MV, MC>(
        self,
        create_combiner: CC,
        merge_value: MV,
        merge_combiners: MC,
        partitioner: Partitioner,
        comparator: Option<atomic_data::dependency::KeyComparator<K>>,
    ) -> TypedRdd<(K, C)>
    where
        C: Data + Clone + bincode::Encode + bincode::Decode<()>,
        CC: Fn(V) -> C + Clone + Send + Sync + 'static,
        MV: Fn(C, V) -> C + Clone + Send + Sync + 'static,
        MC: Fn(C, C) -> C + Clone + Send + Sync + 'static,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        use crate::rdd::shuffled::ShuffledRdd;
        use atomic_data::aggregator::Aggregator;
        use atomic_data::shuffle::fetcher::ShuffleFetcher;

        let mv2 = merge_value.clone();
        let mc2 = merge_combiners.clone();
        let aggregator = Arc::new(Aggregator::<K, V, C>::new(
            Arc::new(move |v: V| create_combiner(v)),
            Arc::new(move |c: &mut C, v: V| *c = mv2(c.clone(), v)),
            Arc::new(move |c1: &mut C, c2: C| *c1 = mc2(c1.clone(), c2)),
        ));

        let shuffle_id = self.context.new_shuffle_id();
        let rdd_id = self.context.new_rdd_id();
        let tracker = atomic_data::env::get_map_output_tracker()
            .unwrap_or_else(|| Arc::new(atomic_data::shuffle::MapOutputTracker::default()));
        let fetcher = Arc::new(ShuffleFetcher::new(tracker));

        let staged_info = if self.context.is_distributed() {
            self.staged
                .as_ref()
                .map(|s| (s.source_partitions.clone(), s.ops.clone()))
        } else {
            None
        };

        let shuffled = ShuffledRdd::<K, V, C>::new_with_staged(
            rdd_id,
            shuffle_id,
            self.rdd,
            aggregator,
            partitioner,
            fetcher,
            staged_info,
            comparator,
        );
        TypedRdd::new(Arc::new(shuffled), self.context)
    }

    /// Fold values for each key with an initial zero value.
    ///
    /// Equivalent to `combine_by_key` where the combiner type equals the value type.
    /// `zero` must be a neutral element: `f(zero.clone(), v) == v`.
    pub(crate) fn fold_by_key<F>(self, zero: V, f: F, num_partitions: usize) -> TypedRdd<(K, V)>
    where
        F: Fn(V, V) -> V + Clone + Send + Sync + 'static,
        V: bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        let f1 = f.clone();
        let f2 = f.clone();
        let f3 = f;
        let z1 = zero.clone();
        self.combine_by_key(move |v| f1(z1.clone(), v), f2, f3, num_partitions)
    }

    /// Aggregate values for each key with a different accumulator type.
    ///
    /// `zero` is the initial accumulator value per partition.
    /// `seq_fn(acc, value)` merges a value into the partition accumulator.
    /// `comb_fn(acc1, acc2)` merges two partition accumulators on the driver.
    pub fn aggregate_by_key<C, SF, CF>(
        self,
        zero: C,
        seq_fn: SF,
        comb_fn: CF,
        num_partitions: usize,
    ) -> TypedRdd<(K, C)>
    where
        C: Data + Clone + bincode::Encode + bincode::Decode<()>,
        SF: Fn(C, V) -> C + Clone + Send + Sync + 'static,
        CF: Fn(C, C) -> C + Clone + Send + Sync + 'static,
        V: bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        let z = zero;
        self.combine_by_key(move |_v| z.clone(), seq_fn, comb_fn, num_partitions)
    }

    /// Reduce values per key using a registered binary task — the content-addressed
    /// form of [`reduce_by_key`](Self::reduce_by_key).
    ///
    /// Runs the same shuffle; the merge is a `#[task]`, so it is part of the registry
    /// fingerprint and the job uses one task-based API for local and distributed runs.
    ///
    /// # Example
    /// ```ignore
    /// #[task] fn add(a: u32, b: u32) -> u32 { a + b }
    /// let sums = pair_rdd.reduce_by_key_task(Add);
    /// ```
    pub fn reduce_by_key_task<B>(self, _task: B) -> TypedRdd<(K, V)>
    where
        B: BinaryTask<V>,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        self.reduce_by_key(|a, b| B::call(a, b))
    }

    /// Fold values per key from `zero` using a registered binary task — the
    /// content-addressed form of [`fold_by_key`](Self::fold_by_key).
    pub fn fold_by_key_task<B>(self, zero: V, _task: B, num_partitions: usize) -> TypedRdd<(K, V)>
    where
        B: BinaryTask<V>,
        V: bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        self.fold_by_key(zero, |a, b| B::call(a, b), num_partitions)
    }

    /// Return elements whose key is NOT present in `other`.
    ///
    /// Collects all keys from `other` to the driver, then filters `self` to exclude them.
    pub fn subtract_by_key<U>(self, other: TypedRdd<(K, U)>) -> TypedRdd<(K, V)>
    where
        U: Data + Clone,
        K: std::hash::Hash + Eq,
        Vec<(K, U)>: Data + Clone,
    {
        use std::collections::HashSet;
        let ctx = self.context.clone();
        let id = ctx.new_rdd_id();

        let other_parts = other
            .context
            .run_job(other.rdd, |iter| iter.map(|(k, _)| k).collect::<Vec<K>>())
            .unwrap_or_default();
        let excluded: Arc<HashSet<K>> = Arc::new(other_parts.into_iter().flatten().collect());

        let rdd = Arc::new(MapPartitionsRdd::new(id, self.rdd, move |_idx, iter| {
            let excl = excluded.clone();
            Box::new(iter.filter(move |(k, _)| !excl.contains(k)))
                as Box<dyn Iterator<Item = (K, V)>>
        }));
        TypedRdd::new(rdd, ctx)
    }

    /// Reduce values for each key using an associative function.
    ///
    /// Produces a globally correct result by creating a shuffle dependency.
    /// The shuffle stage repartitions data by key; each output partition is independently
    /// reduced. `collect()` triggers the full map → shuffle → reduce pipeline.
    ///
    /// Internal substrate for [`reduce_by_key_task`](Self::reduce_by_key_task) — the
    /// public API takes a registered binary task, not a closure.
    pub(crate) fn reduce_by_key<F>(self, f: F) -> TypedRdd<(K, V)>
    where
        F: Fn(V, V) -> V + Clone + Send + Sync + 'static,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        use crate::rdd::shuffled::ShuffledRdd;
        use atomic_data::aggregator::Aggregator;
        use atomic_data::shuffle::fetcher::ShuffleFetcher;

        let f2 = f.clone();
        let _f3 = f.clone();
        let aggregator = Arc::new(Aggregator::<K, V, V>::new(
            Arc::new(|v: V| v),
            Arc::new(move |c: &mut V, v: V| *c = f(c.clone(), v)),
            Arc::new(move |c1: &mut V, c2: V| *c1 = f2(c1.clone(), c2)),
        ));

        let num_output_partitions = self.context.default_parallelism().max(1);
        let partitioner = Partitioner::hash::<K>(num_output_partitions);
        let shuffle_id = self.context.new_shuffle_id();
        let rdd_id = self.context.new_rdd_id();

        let tracker = atomic_data::env::get_map_output_tracker()
            .unwrap_or_else(|| Arc::new(atomic_data::shuffle::MapOutputTracker::default()));
        let fetcher = Arc::new(ShuffleFetcher::new(tracker));

        // In distributed mode, if a staged pipeline precedes the shuffle, carry its
        // source partitions + ops into the ShuffleDependencyBox so the workers receive
        // real data and the correct preceding ops (instead of the placeholder RDD).
        let staged_info = if self.context.is_distributed() {
            self.staged
                .as_ref()
                .map(|s| (s.source_partitions.clone(), s.ops.clone()))
        } else {
            None
        };

        let shuffled = ShuffledRdd::<K, V, V>::new_with_staged(
            rdd_id,
            shuffle_id,
            self.rdd,
            aggregator,
            partitioner,
            fetcher,
            staged_info,
            None,
        );
        TypedRdd::new(Arc::new(shuffled), self.context)
    }

    /// Group values for each key.
    ///
    /// Produces a globally correct result by creating a shuffle dependency.
    /// All values for a key are gathered from across partitions into a single `Vec<V>`
    /// per key after the shuffle stage completes.
    ///
    /// # Example
    /// ```ignore
    /// let grouped = pair_rdd.group_by_key();
    /// ```
    pub fn group_by_key(self) -> TypedRdd<(K, Vec<V>)>
    where
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        let n = self.context.default_parallelism().max(1);
        self.group_by_key_n(n)
    }

    /// Like `group_by_key` but shuffles into exactly `num_partitions` output partitions.
    /// Used internally by `cogroup_shuffle` to guarantee both sides use the same partitioner.
    pub(crate) fn group_by_key_n(self, num_partitions: usize) -> TypedRdd<(K, Vec<V>)>
    where
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        use crate::rdd::shuffled::ShuffledRdd;
        use atomic_data::aggregator::Aggregator;
        use atomic_data::shuffle::fetcher::ShuffleFetcher;

        let aggregator = Arc::new(Aggregator::<K, V, Vec<V>>::default());
        let partitioner = Partitioner::hash::<K>(num_partitions);
        let shuffle_id = self.context.new_shuffle_id();
        let rdd_id = self.context.new_rdd_id();
        let tracker = atomic_data::env::get_map_output_tracker()
            .unwrap_or_else(|| Arc::new(atomic_data::shuffle::MapOutputTracker::default()));
        let fetcher = Arc::new(ShuffleFetcher::new(tracker));
        let staged_info = if self.context.is_distributed() {
            self.staged
                .as_ref()
                .map(|s| (s.source_partitions.clone(), s.ops.clone()))
        } else {
            None
        };
        let shuffled = ShuffledRdd::<K, V, Vec<V>>::new_with_staged(
            rdd_id,
            shuffle_id,
            self.rdd,
            aggregator,
            partitioner,
            fetcher,
            staged_info,
            None,
        );
        TypedRdd::new(Arc::new(shuffled), self.context)
    }

    pub fn count_by_key(&self) -> Result<std::collections::HashMap<K, u64>, BaseError> {
        use std::collections::HashMap;

        let count_partition = move |iter: Box<dyn Iterator<Item = (K, V)>>| {
            let mut counts: HashMap<K, u64> = HashMap::new();
            for (k, _v) in iter {
                *counts.entry(k).or_insert(0) += 1;
            }
            counts
        };

        let partition_counts = self.context.run_job(self.rdd.clone(), count_partition)?;

        let mut final_counts = HashMap::new();
        for counts in partition_counts {
            for (k, v) in counts {
                *final_counts.entry(k).or_insert(0) += v;
            }
        }

        Ok(final_counts)
    }

    pub fn lookup(&self, key: &K) -> Result<Vec<V>, BaseError>
    where
        K: Clone,
    {
        let key_clone = key.clone();
        let lookup_partition = move |iter: Box<dyn Iterator<Item = (K, V)>>| {
            iter.filter(|(k, _)| k == &key_clone)
                .map(|(_, v)| v)
                .collect::<Vec<V>>()
        };

        let partition_values = self.context.run_job(self.rdd.clone(), lookup_partition)?;
        Ok(partition_values.into_iter().flatten().collect())
    }

    /// Collect a pair RDD into a `HashMap<K, V>`.
    ///
    /// When a key appears multiple times, the last value encountered wins.
    /// Collects every pair into a single map on the driver.
    pub fn collect_as_map(&self) -> Result<std::collections::HashMap<K, V>, BaseError>
    where
        K: std::hash::Hash + Eq,
    {
        let partitions = self
            .context
            .run_job(self.rdd.clone(), |iter| iter.collect::<Vec<(K, V)>>())?;
        let mut map = std::collections::HashMap::new();
        for pairs in partitions {
            for (k, v) in pairs {
                map.insert(k, v);
            }
        }
        Ok(map)
    }
}
