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
                .map(|s| (s.source_partitions.clone(), s.steps.clone()))
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
                .map(|s| (s.source_partitions.clone(), s.steps.clone()))
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
    pub fn reduce_by_key_task<B>(self, task: B) -> TypedRdd<(K, V)>
    where
        B: BinaryTask<V>,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        self.reduce_by_key(move |a, b| task.call(a, b))
    }

    /// Fold values per key from `zero` using a registered binary task — the
    /// content-addressed form of [`fold_by_key`](Self::fold_by_key).
    pub fn fold_by_key_task<B>(self, zero: V, task: B, num_partitions: usize) -> TypedRdd<(K, V)>
    where
        B: BinaryTask<V>,
        V: bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        self.fold_by_key(zero, move |a, b| task.call(a, b), num_partitions)
    }

    /// Aggregate values per key into a different accumulator type `C`, using two
    /// registered tasks — the `C != V` generalisation of
    /// [`reduce_by_key_task`](Self::reduce_by_key_task).
    ///
    /// - `lift: UnaryTask<V, C>` turns one value into a single-element accumulator.
    /// - `merge: BinaryTask<C>` combines two accumulators. It must be associative and
    ///   commutative: it runs both per-partition (map side) and across partitions
    ///   (reduce side) after the shuffle.
    ///
    /// This expresses the canonical "lift each value into a monoid, then sum in the
    /// monoid" shape — averages, min/max-by, set union, histograms, top-k — the cases
    /// where the accumulator is not the value type and so `reduce_by_key_task` cannot
    /// apply. Both functions are `#[task]`s, so the merge is part of the registry
    /// fingerprint and the job runs identically in local and distributed mode.
    ///
    /// Register the shuffle handler for the *value* wire type, as with any keyed
    /// reduction: `register_shuffle_map!(K, V)`.
    ///
    /// # Example
    /// ```ignore
    /// // Mean rating per movie: lift each rating into (sum, count), merge component-wise.
    /// #[task] fn to_sum_count(r: f64) -> (f64, u64) { (r, 1) }
    /// #[task] fn add_sum_count(a: (f64, u64), b: (f64, u64)) -> (f64, u64) {
    ///     (a.0 + b.0, a.1 + b.1)
    /// }
    /// let means = ratings // TypedRdd<(MovieId, f64)>
    ///     .aggregate_by_key_task(ToSumCount, AddSumCount, 8)
    ///     .map_task(task_fn!(|kv: (MovieId, (f64, u64))| -> (MovieId, f64) {
    ///         (kv.0, kv.1.0 / kv.1.1 as f64)
    ///     }));
    /// ```
    pub fn aggregate_by_key_task<C, L, M>(
        self,
        lift: L,
        merge: M,
        num_partitions: usize,
    ) -> TypedRdd<(K, C)>
    where
        C: Data + Clone + bincode::Encode + bincode::Decode<()>,
        L: UnaryTask<V, C>,
        M: BinaryTask<C>,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        let lift_mv = lift.clone();
        let merge_mv = merge.clone();
        self.combine_by_key(
            move |v| lift.call(v),
            move |c, v| merge_mv.call(c, lift_mv.call(v)),
            move |c1, c2| merge.call(c1, c2),
            num_partitions,
        )
    }

    /// Return elements whose key is NOT present in `other`.
    ///
    /// Collects all keys from `other` to the driver, then filters `self` to exclude them.
    pub fn subtract_by_key<U>(self, other: TypedRdd<(K, U)>) -> TypedRdd<(K, V)>
    where
        U: Data + Clone,
        K: std::hash::Hash + Eq,
        Vec<(K, U)>: Data + Clone + WireEncode + WireDecode,
        (K, U): WireEncode,
    {
        use std::collections::HashSet;
        let ctx = self.context.clone();
        let id = ctx.new_rdd_id();

        // Materialise the other side's keys. Distributed uses the op path; the closure
        // `run_job` would run over the empty driver placeholder RDD.
        let excluded: Arc<HashSet<K>> = Arc::new(if other.context.is_distributed() {
            other
                .collect_distributed()
                .unwrap_or_default()
                .into_iter()
                .map(|(k, _)| k)
                .collect()
        } else {
            other
                .context
                .run_job(other.rdd, |iter| iter.map(|(k, _)| k).collect::<Vec<K>>())
                .unwrap_or_default()
                .into_iter()
                .flatten()
                .collect()
        });

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
        // source partitions + ops into the ErasedShuffleDependency so the workers receive
        // real data and the correct preceding ops (instead of the placeholder RDD).
        let staged_info = if self.context.is_distributed() {
            self.staged
                .as_ref()
                .map(|s| (s.source_partitions.clone(), s.steps.clone()))
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
                .map(|s| (s.source_partitions.clone(), s.steps.clone()))
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

    pub fn count_by_key(&self) -> Result<std::collections::HashMap<K, u64>, BaseError>
    where
        (K, V): WireEncode,
        Vec<(K, V)>: WireEncode + WireDecode,
    {
        use std::collections::HashMap;

        let mut final_counts = HashMap::new();
        // Distributed: count on the driver from op-path collected pairs — the closure
        // `run_job` path below would run over the empty driver placeholder RDD.
        if self.context.is_distributed() {
            for (k, _v) in self.collect_distributed()? {
                *final_counts.entry(k).or_insert(0) += 1;
            }
            return Ok(final_counts);
        }

        let count_partition = move |iter: Box<dyn Iterator<Item = (K, V)>>| {
            let mut counts: HashMap<K, u64> = HashMap::new();
            for (k, _v) in iter {
                *counts.entry(k).or_insert(0) += 1;
            }
            counts
        };
        let partition_counts = self.context.run_job(self.rdd.clone(), count_partition)?;
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
        (K, V): WireEncode,
        Vec<(K, V)>: WireEncode + WireDecode,
    {
        if self.context.is_distributed() {
            return Ok(self
                .collect_distributed()?
                .into_iter()
                .filter(|(k, _)| k == key)
                .map(|(_, v)| v)
                .collect());
        }
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
        (K, V): WireEncode,
        Vec<(K, V)>: WireEncode + WireDecode,
    {
        let mut map = std::collections::HashMap::new();
        if self.context.is_distributed() {
            for (k, v) in self.collect_distributed()? {
                map.insert(k, v);
            }
            return Ok(map);
        }
        let partitions = self
            .context
            .run_job(self.rdd.clone(), |iter| iter.collect::<Vec<(K, V)>>())?;
        for pairs in partitions {
            for (k, v) in pairs {
                map.insert(k, v);
            }
        }
        Ok(map)
    }

    /// Reduce values per key with a registered binary task and return the result
    /// as a `HashMap` on the driver — **no shuffle**.
    ///
    /// Each partition combines its own keys first (map-side combine), then the
    /// per-partition maps are merged on the driver. This is the cheap counterpart
    /// to [`reduce_by_key_task`](Self::reduce_by_key_task) when the reduced key set
    /// is small enough to hold on the driver: it avoids the shuffle entirely. For a
    /// distributed result that stays partitioned, use `reduce_by_key_task`.
    ///
    /// `merge` must be associative and commutative — it runs both per-partition and
    /// across partitions.
    ///
    /// # Example
    /// ```ignore
    /// #[task] fn add(a: i32, b: i32) -> i32 { a + b }
    /// let totals: HashMap<String, i32> = pairs.reduce_by_key_locally_task(Add)?;
    /// ```
    pub fn reduce_by_key_locally_task<B>(
        &self,
        merge: B,
    ) -> Result<std::collections::HashMap<K, V>, BaseError>
    where
        B: BinaryTask<V>,
        K: std::hash::Hash + Eq,
        (K, V): WireEncode,
        Vec<(K, V)>: WireEncode + WireDecode,
    {
        use std::collections::HashMap;

        // Distributed: merge on the driver from op-path collected pairs — the closure
        // `run_job` path below would run over the empty driver placeholder RDD.
        if self.context.is_distributed() {
            let mut result: HashMap<K, V> = HashMap::new();
            for (k, v) in self.collect_distributed()? {
                let merged = match result.remove(&k) {
                    Some(existing) => merge.call(existing, v),
                    None => v,
                };
                result.insert(k, merged);
            }
            return Ok(result);
        }

        // Map-side combine: each partition reduces its own keys independently.
        let merge_c = merge.clone();
        let combine_partition = move |iter: Box<dyn Iterator<Item = (K, V)>>| {
            let mut acc: HashMap<K, V> = HashMap::new();
            for (k, v) in iter {
                let merged = match acc.remove(&k) {
                    Some(existing) => merge_c.call(existing, v),
                    None => v,
                };
                acc.insert(k, merged);
            }
            acc
        };

        let partials = self.context.run_job(self.rdd.clone(), combine_partition)?;

        // Driver-side merge of the per-partition maps.
        let mut result: HashMap<K, V> = HashMap::new();
        for partial in partials {
            for (k, v) in partial {
                let merged = match result.remove(&k) {
                    Some(existing) => merge.call(existing, v),
                    None => v,
                };
                result.insert(k, merged);
            }
        }
        Ok(result)
    }
}
