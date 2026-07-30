use std::any::TypeId;

use atomic_data::partitioner::{CustomPartitioner, NamedPartitioner};

use crate::builtin_tasks::{max::MaxTask, min::MinTask, sum::SumTask};

use super::*;

/// Wiring for a map-side `CombineByKey` pre-combine step: the step to append to the staged
/// pipeline, plus whether it pre-combines the shuffle input into `(K, C)` pairs (`C != V`).
struct CombineWiring {
    step: Step,
    map_side_combined: bool,
}

/// Build the `CombineByKey` engine step. `task_name` is empty — the op is framework-handled
/// and requires no worker capability lookup; `combine_key` selects the worker handler.
fn combine_step(combine_key: &str, lift_task_name: Option<&str>, merge_task_name: &str) -> Step {
    Step {
        task_name: String::new(),
        kind: StepKind::Engine(EngineAction::CombineByKey {
            combine_key: combine_key.to_string(),
            lift_task_name: lift_task_name.map(str::to_string),
            merge_task_name: merge_task_name.to_string(),
        }),
        runtime: TaskRuntime::Native,
        payload: vec![],
    }
}

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
        P: CustomPartitioner + 'static,
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
        P: NamedPartitioner,
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

    /// Wiring for an opt-in map-side `CombineByKey` step: the step appended to the staged
    /// pipeline, plus whether it pre-combines into `(K, C)` (`map_side_combined`, `C != V`).
    fn combine_wiring_cv(&self, merge_task_name: &str) -> Option<CombineWiring>
    where
        K: 'static,
        V: 'static,
    {
        if !self.context.is_distributed() || self.staged.is_none() {
            return None;
        }
        let key = crate::registry::combine_handler_registered(TypeId::of::<(K, V)>())?;
        Some(CombineWiring {
            step: combine_step(key, None, merge_task_name),
            map_side_combined: false,
        })
    }

    /// Wiring for the `C != V` lift case (`aggregate_by_key_task`): the pre-combine ships
    /// `(K, C)` pairs, so `map_side_combined` is set and the reduce side fetches `(K, C)`.
    fn combine_wiring_lift<C>(
        &self,
        lift_task_name: &str,
        merge_task_name: &str,
    ) -> Option<CombineWiring>
    where
        K: 'static,
        V: 'static,
        C: 'static,
    {
        if !self.context.is_distributed() || self.staged.is_none() {
            return None;
        }
        let key = crate::registry::combine_handler_registered(TypeId::of::<(K, V, C)>())?;
        Some(CombineWiring {
            step: combine_step(key, Some(lift_task_name), merge_task_name),
            map_side_combined: true,
        })
    }

    /// Shared combine-shuffle builder for the `_task` reductions, used only when the gate
    /// held (distributed, a staged pipeline precedes the shuffle, and a combine handler is
    /// registered). Builds the same `Aggregator`/`ShuffledRdd` the closure substrate does,
    /// then appends `combine.step` to the staged pipeline so the workers pre-combine before
    /// the shuffle write.
    fn build_shuffle_combine<C, CC, MV, MC>(
        self,
        create_combiner: CC,
        merge_value: MV,
        merge_combiners: MC,
        partitioner: Partitioner,
        combine: CombineWiring,
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

        let map_side_combined = combine.map_side_combined;
        // The gate guarantees a staged pipeline exists; append the combine step to it.
        let staged_info = self.staged.as_ref().map(|s| {
            let mut steps = s.steps.clone();
            steps.push(combine.step.clone());
            (s.source_partitions.clone(), steps)
        });

        let shuffled = if map_side_combined {
            ShuffledRdd::<K, V, C>::new_with_staged_combined(
                rdd_id,
                shuffle_id,
                self.rdd,
                aggregator,
                partitioner,
                fetcher,
                staged_info,
            )
        } else {
            ShuffledRdd::<K, V, C>::new_with_staged(
                rdd_id,
                shuffle_id,
                self.rdd,
                aggregator,
                partitioner,
                fetcher,
                staged_info,
                None,
            )
        };
        TypedRdd::new(Arc::new(shuffled), self.context)
    }

    /// Reduce values per key using a registered binary task — the content-addressed
    /// form of [`reduce_by_key`](Self::reduce_by_key).
    ///
    /// Runs the same shuffle; the merge is a `#[task]`, so it is part of the registry
    /// fingerprint and the job uses one task-based API for local and distributed runs.
    /// When `register_combine!(K, V)` is present and a `_task` pipeline precedes
    /// this op in distributed mode, values are pre-combined per key on the map side.
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
        // No combine handler registered (or no staged pipeline / local mode): fall back to the
        // unchanged closure substrate — byte-identical to today's behaviour.
        match self.combine_wiring_cv(B::NAME) {
            None => self.reduce_by_key(move |a, b| task.call(a, b)),
            Some(combine) => {
                let partitioner = Partitioner::hash::<K>(self.context.default_parallelism().max(1));
                let f = move |a: V, b: V| task.call(a, b);
                let f2 = f.clone();
                self.build_shuffle_combine(
                    |v| v,
                    move |c: V, v: V| f(c, v),
                    move |c1: V, c2: V| f2(c1, c2),
                    partitioner,
                    combine,
                )
            }
        }
    }

    /// Fold values per key from `zero` using a registered binary task — the
    /// content-addressed form of [`fold_by_key`](Self::fold_by_key). Supports the same
    /// opt-in map-side pre-combine as [`reduce_by_key_task`](Self::reduce_by_key_task).
    pub fn fold_by_key_task<B>(self, zero: V, task: B, num_partitions: usize) -> TypedRdd<(K, V)>
    where
        B: BinaryTask<V>,
        V: bincode::Encode + bincode::Decode<()>,
        K: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        match self.combine_wiring_cv(B::NAME) {
            None => self.fold_by_key(zero, move |a, b| task.call(a, b), num_partitions),
            Some(combine) => {
                let partitioner = Partitioner::hash::<K>(num_partitions.max(1));
                let f = move |a: V, b: V| task.call(a, b);
                let f1 = f.clone();
                let f2 = f.clone();
                let z1 = zero.clone();
                self.build_shuffle_combine(
                    move |v: V| f1(z1.clone(), v),
                    move |c: V, v: V| f(c, v),
                    move |c1: V, c2: V| f2(c1, c2),
                    partitioner,
                    combine,
                )
            }
        }
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
        match self.combine_wiring_lift::<C>(L::NAME, M::NAME) {
            None => {
                // No lift-combine handler registered: unchanged closure substrate.
                let lift_mv = lift.clone();
                let merge_mv = merge.clone();
                self.combine_by_key(
                    move |v| lift.call(v),
                    move |c, v| merge_mv.call(c, lift_mv.call(v)),
                    move |c1, c2| merge.call(c1, c2),
                    num_partitions,
                )
            }
            Some(combine) => {
                let partitioner = Partitioner::hash::<K>(num_partitions.max(1));
                let lift_cc = lift.clone();
                let lift_mv = lift;
                let merge_mv = merge.clone();
                let merge_mc = merge;
                self.build_shuffle_combine(
                    move |v| lift_cc.call(v),
                    move |c, v| merge_mv.call(c, lift_mv.call(v)),
                    move |c1, c2| merge_mc.call(c1, c2),
                    partitioner,
                    combine,
                )
            }
        }
    }

    /// Transform each value with a registered unary task, keeping the key — the
    /// content-addressed, key-preserving form of a value map.
    pub fn map_values_task<W, B>(self, task: B) -> TypedRdd<(K, W)>
    where
        W: Data + Clone,
        B: UnaryTask<V, W>,
    {
        self.map_rdd(move |id, rdd| MapperRdd::new(id, rdd, move |(k, v)| (k, task.call(v))))
    }

    /// Flat-map each value with a registered task returning `Vec<W>`, pairing every produced
    /// element with the original key.
    pub fn flat_map_values_task<W, B>(self, task: B) -> TypedRdd<(K, W)>
    where
        W: Data + Clone,
        B: UnaryTask<V, Vec<W>>,
    {
        self.map_rdd(move |id, rdd| {
            FlatMapperRdd::new(id, rdd, move |(k, v)| {
                Box::new(task.call(v).into_iter().map(move |w| (k.clone(), w)))
                    as Box<dyn Iterator<Item = (K, W)>>
            })
        })
    }

    /// Sum the values for each key using the built-in `SumTask<V>` (numeric `V`).
    ///
    /// Shorthand for [`reduce_by_key_task`](Self::reduce_by_key_task) with the sum monoid.
    pub fn sum_values(self) -> TypedRdd<(K, V)>
    where
        SumTask<V>: BinaryTask<V> + Default,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        self.reduce_by_key_task(SumTask::<V>::default())
    }

    /// Maximum value for each key using the built-in `MaxTask<V>` (`V: Ord`).
    pub fn max_values(self) -> TypedRdd<(K, V)>
    where
        MaxTask<V>: BinaryTask<V> + Default,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        self.reduce_by_key_task(MaxTask::<V>::default())
    }

    /// Minimum value for each key using the built-in `MinTask<V>` (`V: Ord`).
    pub fn min_values(self) -> TypedRdd<(K, V)>
    where
        MinTask<V>: BinaryTask<V> + Default,
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        self.reduce_by_key_task(MinTask::<V>::default())
    }

    /// Count the values for each key, producing `(K, u64)`.
    ///
    /// A `combine_by_key` with `C = u64` — value-agnostic, so it works for any `V`.
    pub fn count_values(self, num_partitions: usize) -> TypedRdd<(K, u64)>
    where
        K: bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
        Vec<(K, V)>: WireEncode,
    {
        self.combine_by_key(|_v| 1u64, |c, _v| c + 1, |a, b| a + b, num_partitions)
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

        // Materialise the other side's keys via the pipeline dispatch path.
        let excluded: Arc<HashSet<K>> = Arc::new(
            other
                .collect()
                .unwrap_or_default()
                .into_iter()
                .map(|(k, _)| k)
                .collect(),
        );

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
        // source partitions + steps into the ShuffleDependency so the workers receive
        // real data and the correct preceding steps (instead of the placeholder RDD).
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

    pub fn count_by_key(&self) -> Result<std::collections::HashMap<K, u64>, DataError>
    where
        (K, V): WireEncode + WireDecode,
        Vec<(K, V)>: WireEncode + WireDecode,
    {
        use std::collections::HashMap;

        self.reduce_partitions(HashMap::new(), |mut acc: HashMap<K, u64>, part| {
            for (k, _v) in part {
                *acc.entry(k).or_insert(0) += 1;
            }
            acc
        })
    }

    pub fn lookup(&self, key: &K) -> Result<Vec<V>, DataError>
    where
        K: Clone,
        (K, V): WireEncode,
        Vec<(K, V)>: WireEncode + WireDecode,
    {
        Ok(self
            .collect()?
            .into_iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v)
            .collect())
    }

    /// Collect a pair RDD into a `HashMap<K, V>`.
    ///
    /// When a key appears multiple times, the last value encountered wins.
    /// Collects every pair into a single map on the driver.
    pub fn collect_as_map(&self) -> Result<std::collections::HashMap<K, V>, DataError>
    where
        K: std::hash::Hash + Eq,
        (K, V): WireEncode,
        Vec<(K, V)>: WireEncode + WireDecode,
    {
        let mut map = std::collections::HashMap::new();
        for (k, v) in self.collect()? {
            map.insert(k, v);
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
    ) -> Result<std::collections::HashMap<K, V>, DataError>
    where
        B: BinaryTask<V>,
        K: std::hash::Hash + Eq,
        (K, V): WireEncode + WireDecode,
        Vec<(K, V)>: WireEncode + WireDecode,
    {
        use std::collections::HashMap;

        // Map-side combine: each partition reduces its own keys independently, then
        // the per-partition maps are merged into the accumulator one at a time.
        self.reduce_partitions(HashMap::<K, V>::new(), move |mut result, part| {
            let mut acc: HashMap<K, V> = HashMap::new();
            for (k, v) in part {
                let merged = match acc.remove(&k) {
                    Some(existing) => merge.call(existing, v),
                    None => v,
                };
                acc.insert(k, merged);
            }
            for (k, v) in acc {
                let merged = match result.remove(&k) {
                    Some(existing) => merge.call(existing, v),
                    None => v,
                };
                result.insert(k, merged);
            }
            result
        })
    }
}
