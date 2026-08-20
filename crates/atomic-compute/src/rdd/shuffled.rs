use crate::rdd::rdd_val::RddVals;
use crate::rdd::{Rdd, RddBase};
use crate::registry::SHUFFLE_KEY_REGISTRY;
use atomic_data::aggregator::{Aggregator, CreateCombinerFn, MergeValueFn};
use atomic_data::data::Data;
use atomic_data::dependency::{Dependency, KeyComparator, ShuffleDependency, TypedShuffle};
use atomic_data::distributed::WireSerde;
use atomic_data::error::DataError;
use atomic_data::partitioner::Partitioner;
use atomic_data::shuffle::fetcher::{ShuffleFetcher, SpilledRunIter};
use atomic_data::split::{ShuffledRddSplit, Split};
use itertools::Itertools;
use std::any::TypeId;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

/// Reduce-side fetch switches from in-memory runs to disk-spilled lazy runs once
/// a reduce partition draws from more than this many map outputs (wide shuffles).
/// Override with `ATOMIC_REDUCE_SPILL_THRESHOLD_RUNS`.
static REDUCE_SPILL_THRESHOLD_RUNS: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("ATOMIC_REDUCE_SPILL_THRESHOLD_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64)
});

/// Folds a `kmerge_by`'d stream of raw `(K, V)` pairs into `(K, C)` by key, one key's full
/// neighbourhood at a time: the first value under a key seeds `C` via `create_combiner`, each
/// later value under the same key folds in via `merge_value`. Peeks at most one element ahead
/// (like the `coalesce` it replaces), so it stays as lazy as the merge it wraps.
struct KeyFold<K, M: Iterator<Item = (K, V)>, V, C> {
    merged: std::iter::Peekable<M>,
    cmp: KeyComparator<K>,
    create_combiner: CreateCombinerFn<V, C>,
    merge_value: MergeValueFn<V, C>,
}

impl<K, M: Iterator<Item = (K, V)>, V, C> Iterator for KeyFold<K, M, V, C> {
    type Item = (K, C);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, first) = self.merged.next()?;
        let mut acc = (self.create_combiner)(first);
        while let Some((next_key, _)) = self.merged.peek()
            && (self.cmp)(&key, next_key) == Ordering::Equal
        {
            let (_, next_val) = self.merged.next().expect("peeked Some");
            (self.merge_value)(&mut acc, next_val);
        }
        Some((key, acc))
    }
}

/// Lazy k-way sort-merge of pre-sorted `runs` of raw `(K, V)` pairs (each run internally
/// sorted by key — the map side writes raw pairs regardless of executor, see
/// `registry::shuffle`/`ShuffleDependency::do_shuffle_task`), aggregating same-key
/// neighbours into `C` via [`KeyFold`] as the merge streams past them. The merged output is
/// never materialized: `kmerge_by` keeps an O(#runs) heap and `KeyFold` looks one element
/// ahead, so a streaming consumer holds only that working set.
fn lazy_sort_merge<K, V, C, I>(
    runs: Vec<I>,
    cmp: KeyComparator<K>,
    create_combiner: CreateCombinerFn<V, C>,
    merge_value: MergeValueFn<V, C>,
) -> Box<dyn Iterator<Item = (K, C)>>
where
    K: 'static,
    V: 'static,
    C: 'static,
    I: Iterator<Item = (K, V)> + 'static,
{
    let cmp_merge = cmp.clone();
    let merged = runs
        .into_iter()
        .kmerge_by(move |a: &(K, V), b: &(K, V)| cmp_merge(&a.0, &b.0) == Ordering::Less);
    Box::new(KeyFold {
        merged: merged.peekable(),
        cmp,
        create_combiner,
        merge_value,
    })
}

pub struct ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash + Clone + WireSerde,
    V: Data + Clone,
    C: Data + Clone + WireSerde,
{
    parent: Arc<dyn Rdd<Item = (K, V)>>,
    aggregator: Arc<Aggregator<K, V, C>>,
    vals: Arc<RddVals>,
    part: Partitioner,
    shuffle_id: usize,
    fetcher: Arc<ShuffleFetcher>,
    /// When `Some`, the map side wrote sorted runs and `compute` k-way merges them
    /// instead of building a `HashMap` (sort-shuffle). `None` keeps the HashMap reduce.
    comparator: Option<atomic_data::dependency::KeyComparator<K>>,
    /// Set when an opt-in map-side `CombineByKey`-lift step pre-combined the shuffle input
    /// into `(K, C)` pairs (`aggregate_by_key_task`, `C != V`). The reduce side then fetches
    /// `(K, C)` and merges same-key entries via `merge_combiners` alone, skipping
    /// `create_combiner`/`merge_value` (which expect a raw `V`). Always `false` for the
    /// `C == V` path, where the wire type is unchanged and the reduce side is untouched.
    map_side_combined: bool,
}

impl<K, V, C> Clone for ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash + Clone + WireSerde,
    V: Data + Clone,
    C: Data + Clone + WireSerde,
{
    fn clone(&self) -> Self {
        ShuffledRdd {
            parent: self.parent.clone(),
            aggregator: self.aggregator.clone(),
            vals: self.vals.clone(),
            part: self.part.clone(),
            shuffle_id: self.shuffle_id,
            fetcher: self.fetcher.clone(),
            comparator: self.comparator.clone(),
            map_side_combined: self.map_side_combined,
        }
    }
}

impl<K, V, C> ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash + Clone + WireSerde,
    V: Data + Clone + WireSerde,
    C: Data + Clone + WireSerde,
{
    pub fn new(
        id: usize,
        shuffle_id: usize,
        parent: Arc<dyn Rdd<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Partitioner,
        fetcher: Arc<ShuffleFetcher>,
    ) -> Self {
        Self::new_with_staged(
            id, shuffle_id, parent, aggregator, part, fetcher, None, None,
        )
    }

    /// Variant used when a staged pipeline (from `_task` steps) precedes the shuffle.
    /// `staged` carries `(source_partitions, preceding_steps)` so workers run the steps
    /// before writing shuffle buckets.
    ///
    /// The shuffle wire type is `(K, V)` and the dispatch key is looked up for `(K, V)`.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_staged(
        id: usize,
        shuffle_id: usize,
        parent: Arc<dyn Rdd<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Partitioner,
        fetcher: Arc<ShuffleFetcher>,
        staged: Option<(Vec<Vec<u8>>, Vec<atomic_data::distributed::Step>)>,
        comparator: Option<atomic_data::dependency::KeyComparator<K>>,
    ) -> Self {
        Self::build(
            id,
            shuffle_id,
            parent,
            aggregator,
            part,
            fetcher,
            staged,
            comparator,
            TypeId::of::<(K, V)>(),
            false,
        )
    }

    /// Variant for an opt-in map-side `CombineByKey`-lift shuffle (`aggregate_by_key_task`,
    /// `C != V`): the staged pipeline's trailing `CombineByKey` step ships `(K, C)` pairs, so
    /// the dispatch key is looked up for `(K, C)` and `map_side_combined` is set so the reduce
    /// side fetches `(K, C)` and merges via `merge_combiners`.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_staged_combined(
        id: usize,
        shuffle_id: usize,
        parent: Arc<dyn Rdd<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Partitioner,
        fetcher: Arc<ShuffleFetcher>,
        staged: Option<(Vec<Vec<u8>>, Vec<atomic_data::distributed::Step>)>,
    ) -> Self {
        Self::build(
            id,
            shuffle_id,
            parent,
            aggregator,
            part,
            fetcher,
            staged,
            None,
            TypeId::of::<(K, C)>(),
            true,
        )
    }

    /// Shared constructor: builds the `ShuffleDependency` (embedding the dispatch key resolved
    /// for `shuffle_value_type_id`), attaches any staged pipeline, and records whether the map
    /// side pre-combined into `(K, C)` (`map_side_combined`).
    #[allow(clippy::too_many_arguments)]
    fn build(
        id: usize,
        shuffle_id: usize,
        parent: Arc<dyn Rdd<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Partitioner,
        fetcher: Arc<ShuffleFetcher>,
        staged: Option<(Vec<Vec<u8>>, Vec<atomic_data::distributed::Step>)>,
        comparator: Option<atomic_data::dependency::KeyComparator<K>>,
        shuffle_value_type_id: TypeId,
        map_side_combined: bool,
    ) -> Self {
        let mut vals = RddVals::new(id);

        // Sort-shuffle when a comparator is supplied: the dependency sorts each bucket so the
        // reduce side can k-way merge sorted runs. Otherwise the legacy unsorted layout.
        let shuffle_dep = match &comparator {
            Some(cmp) => TypedShuffle::<K, V, C>::new_sorted(
                shuffle_id,
                false,
                parent.clone(),
                part.clone(),
                cmp.clone(),
            ),
            None => TypedShuffle::<K, V, C>::new(shuffle_id, false, parent.clone(), part.clone()),
        };
        let shuffle_key = SHUFFLE_KEY_REGISTRY
            .get(&shuffle_value_type_id)
            .copied()
            .unwrap_or_else(|| {
                panic!(
                    "register_shuffle_map! not called for the shuffle value type of ({}, {}) \
                     [combined={map_side_combined}]; add the matching \
                     `atomic_compute::register_shuffle_map!` / `register_combine_lift!` \
                     to your binary before triggering the shuffle",
                    std::any::type_name::<K>(),
                    std::any::type_name::<V>(),
                )
            });
        let dep_box = ShuffleDependency::from_typed_with_key(shuffle_dep, shuffle_key);
        let dep_box = if let Some((src_parts, steps)) = staged {
            dep_box.with_staged_pipeline(src_parts, steps)
        } else {
            dep_box
        };
        vals.dependencies
            .push(Dependency::Shuffle(Arc::new(dep_box)));
        let vals = Arc::new(vals);
        ShuffledRdd {
            parent,
            aggregator,
            vals,
            part,
            shuffle_id,
            fetcher,
            comparator,
            map_side_combined,
        }
    }
}

impl<K, V, C> RddBase for ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash + Clone + WireSerde,
    V: Data + Clone + WireSerde,
    C: Data + Clone + WireSerde,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        (0..self.part.num_partitions())
            .map(|x| Box::new(ShuffledRddSplit::new(x)) as Box<dyn Split>)
            .collect()
    }

    fn number_of_splits(&self) -> usize {
        // If adaptive coalescing ran for this shuffle, return the coalesced count.
        if let Some(tracker) = atomic_data::env::get_map_output_tracker()
            && let Some(entry) = tracker.coalesced_parts.get(&self.shuffle_id)
        {
            return *entry;
        }
        self.part.num_partitions()
    }

    fn partitioner(&self) -> Option<Partitioner> {
        Some(self.part.clone())
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, DataError> {
        log::debug!("inside iterator_any shuffledrdd",);
        let rdd_iter = self
            .iterator(split)?
            .map(|(k, v)| Box::new((k, v)) as Box<dyn Data>);
        Ok(Box::new(rdd_iter))
    }

    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, DataError> {
        log::debug!("inside cogroup iterator_any shuffledrdd",);
        let rdd_iter = self
            .iterator(split)?
            .map(|(k, v)| Box::new((k, Box::new(v))) as Box<dyn Data>);
        Ok(Box::new(rdd_iter))
    }
}

impl<K, V, C> Rdd for ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash + Clone + WireSerde,
    V: Data + Clone + WireSerde,
    C: Data + Clone + WireSerde,
{
    type Item = (K, C);

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, DataError> {
        log::debug!("compute inside shuffled rdd");
        let start = Instant::now();

        let coalesced_id = split.get_index();
        let original_num_partitions = self.part.num_partitions();

        // Determine which original reduce-partition IDs this coalesced split covers.
        let mut original_ids: Vec<usize> = vec![coalesced_id];

        if let Some(tracker) = atomic_data::env::get_map_output_tracker()
            && let Some(entry) = tracker.coalesced_parts.get(&self.shuffle_id)
        {
            let coalesced_n = *entry;
            // Map coalesced_id → original reduce partition range.
            // Simple even-split mapping: coalesced partition i covers
            // [i * (original / coalesced), (i+1) * (original / coalesced)).
            let ratio = original_num_partitions.max(1);
            let per_coalesced = ratio.div_ceil(coalesced_n); // ceil
            let start_id = coalesced_id * per_coalesced;
            let end_id = ((coalesced_id + 1) * per_coalesced).min(original_num_partitions);
            original_ids = (start_id..end_id).collect();
        }

        // Map-side pre-combined reduce (`aggregate_by_key_task`, `C != V`): the shuffle input
        // was lifted `V -> C` and combined per key on the map side, so the fetched pairs are
        // already `(K, C)`. Merge same-key combiners via `merge_combiners` alone —
        // `create_combiner`/`merge_value` expect a raw `V` and must not run here. This path is
        // never sort-shuffled (`aggregate_by_key_task` always builds `comparator: None`), so it
        // short-circuits ahead of the comparator branch below.
        if self.map_side_combined {
            let mut combiners: HashMap<K, C> = HashMap::new();
            for orig_id in original_ids {
                let fetcher = self.fetcher.clone();
                let shuffle_id = self.shuffle_id;
                let result = crate::env::Env::block_on_shuffle_rt(async move {
                    fetcher.fetch::<K, C>(shuffle_id, orig_id).await
                })
                .map_err(DataError::from)?;
                for (k, c) in result {
                    match combiners.entry(k) {
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            (self.aggregator.merge_combiners)(e.get_mut(), c);
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert(c);
                        }
                    }
                }
            }
            log::debug!(
                "map-side-combined reduce fetched in {}",
                start.elapsed().as_millis()
            );
            return Ok(Box::new(combiners.into_iter()));
        }

        // Sort-shuffle reduce: the map side wrote sorted runs (in `comparator` order),
        // and range partitions are themselves key-ordered, so a single **lazy** k-way
        // merge over all runs yields a globally-ordered stream — no full re-sort.
        //
        // The merged output is never materialized: `kmerge_by` keeps only an
        // O(#runs) heap and `coalesce` looks one element ahead, so a streaming
        // consumer (`fold`, `count`, `save_as_text_file`, …) holds just the fetched
        // input runs plus that small working set, instead of the previous
        // "concatenate every run → re-sort → build full output Vec" (which peaked at
        // input + a second sorted copy + the sort's scratch). (K is only `Hash`-bound,
        // so ordering is driven by the stored comparator rather than `Ord`.)
        if let Some(cmp) = &self.comparator {
            let create_combiner = self.aggregator.create_combiner.clone();
            let merge_value = self.aggregator.merge_value.clone();

            // Wide shuffle: stream each run from a temp file so the full input is
            // never resident — only the k-way-merge working set. The run count is
            // the number of map outputs registered for this shuffle.
            let run_count = atomic_data::env::get_map_output_tracker()
                .and_then(|t| {
                    t.map_output_uris
                        .get(&self.shuffle_id)
                        .map(|e| e.value().len())
                })
                .unwrap_or(0);
            if run_count > *REDUCE_SPILL_THRESHOLD_RUNS {
                let mut runs: Vec<SpilledRunIter<K, V>> = Vec::new();
                for orig_id in original_ids {
                    let fetcher = self.fetcher.clone();
                    let shuffle_id = self.shuffle_id;
                    let fetched = crate::env::Env::block_on_shuffle_rt(async move {
                        fetcher
                            .fetch_runs_spilled::<K, V>(shuffle_id, orig_id)
                            .await
                    })
                    .map_err(DataError::from)?;
                    runs.extend(fetched);
                }
                log::debug!(
                    "sort-merge (lazy k-way, {} disk-spilled runs) prepared in {}",
                    runs.len(),
                    start.elapsed().as_millis()
                );
                return Ok(lazy_sort_merge(
                    runs,
                    cmp.clone(),
                    create_combiner,
                    merge_value,
                ));
            }

            let mut runs: Vec<std::vec::IntoIter<(K, V)>> = Vec::new();
            for orig_id in original_ids {
                let fetcher = self.fetcher.clone();
                let shuffle_id = self.shuffle_id;
                let fetched = crate::env::Env::block_on_shuffle_rt(async move {
                    fetcher.fetch_runs::<K, V>(shuffle_id, orig_id).await
                })
                .map_err(DataError::from)?;
                runs.extend(fetched.into_iter().map(IntoIterator::into_iter));
            }
            log::debug!(
                "sort-merge (lazy k-way) prepared in {}",
                start.elapsed().as_millis()
            );
            return Ok(lazy_sort_merge(
                runs,
                cmp.clone(),
                create_combiner,
                merge_value,
            ));
        }

        let mut combiners: HashMap<K, C> = HashMap::new();
        for orig_id in original_ids {
            let fetcher = self.fetcher.clone();
            let shuffle_id = self.shuffle_id;
            let result = crate::env::Env::block_on_shuffle_rt(async move {
                fetcher.fetch::<K, V>(shuffle_id, orig_id).await
            })
            .map_err(DataError::from)?;
            for (k, v) in result {
                match combiners.entry(k) {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        (self.aggregator.merge_value)(e.get_mut(), v);
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert((self.aggregator.create_combiner)(v));
                    }
                }
            }
        }

        log::debug!("time taken for fetching {}", start.elapsed().as_millis());
        Ok(Box::new(combiners.into_iter()))
    }
}
