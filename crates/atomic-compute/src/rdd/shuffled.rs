use crate::rdd::rdd_val::RddVals;
use crate::rdd::{Rdd, RddBase};
use crate::task_registry::SHUFFLE_KEY_REGISTRY;
use atomic_data::aggregator::{Aggregator, MergeCombinersFn};
use atomic_data::data::Data;
use atomic_data::dependency::{Dependency, KeyComparator, ShuffleDependency, TypedShuffle};
use atomic_data::distributed::WireEncode;
use atomic_data::error::DataError;
use atomic_data::partitioner::Partitioner;
use atomic_data::shuffle::fetcher::{ShuffleFetcher, SpilledRunIter};
use atomic_data::split::{ShuffledRddSplit, Split};
use bincode::{Decode, Encode};
use itertools::Itertools;
use std::any::TypeId;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, LazyLock};
use std::time::Instant;
use tokio::runtime::Handle;

/// Reduce-side fetch switches from in-memory runs to disk-spilled lazy runs once
/// a reduce partition draws from more than this many map outputs (wide shuffles).
/// Override with `ATOMIC_REDUCE_SPILL_THRESHOLD_RUNS`.
static REDUCE_SPILL_THRESHOLD_RUNS: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("ATOMIC_REDUCE_SPILL_THRESHOLD_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64)
});

/// Lazy k-way sort-merge of pre-sorted `runs` (any run iterator type), combining
/// equal-key neighbours with `merge_combiners`. The merged output is never
/// materialized: `kmerge_by` keeps an O(#runs) heap and `coalesce` looks one
/// element ahead, so a streaming consumer holds only that working set.
fn lazy_sort_merge<K, C, I>(
    runs: Vec<I>,
    cmp: KeyComparator<K>,
    merge_combiners: MergeCombinersFn<C>,
) -> Box<dyn Iterator<Item = (K, C)>>
where
    K: 'static,
    C: 'static,
    I: Iterator<Item = (K, C)> + 'static,
{
    let cmp_merge = cmp.clone();
    let merged = runs
        .into_iter()
        .kmerge_by(move |a: &(K, C), b: &(K, C)| cmp_merge(&a.0, &b.0) == Ordering::Less)
        .coalesce(move |mut acc: (K, C), next: (K, C)| {
            if cmp(&acc.0, &next.0) == Ordering::Equal {
                merge_combiners(&mut acc.1, next.1);
                Ok(acc)
            } else {
                Err((acc, next))
            }
        });
    Box::new(merged)
}

pub struct ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash + Clone + Encode + Decode<()>,
    V: Data + Clone,
    C: Data + Clone + Encode + Decode<()>,
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
}

impl<K, V, C> Clone for ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash + Clone + Encode + Decode<()>,
    V: Data + Clone,
    C: Data + Clone + Encode + Decode<()>,
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
        }
    }
}

impl<K, V, C> ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash + Clone + Encode + Decode<()>,
    V: Data + Clone,
    C: Data + Clone + Encode + Decode<()>,
    Vec<(K, V)>: WireEncode,
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
        let mut vals = RddVals::new(id);

        // Sort-shuffle when a comparator is supplied: the dependency sorts each bucket so the
        // reduce side can k-way merge sorted runs. Otherwise the legacy unsorted layout.
        let shuffle_dep = match &comparator {
            Some(cmp) => TypedShuffle::new_sorted(
                shuffle_id,
                false,
                parent.clone(),
                aggregator.clone(),
                part.clone(),
                cmp.clone(),
            ),
            None => TypedShuffle::new(
                shuffle_id,
                false,
                parent.clone(),
                aggregator.clone(),
                part.clone(),
            ),
        };
        let shuffle_key = SHUFFLE_KEY_REGISTRY
            .get(&TypeId::of::<(K, V)>())
            .copied()
            .unwrap_or_else(|| {
                panic!(
                    "register_shuffle_map! not called for ({}, {}); \
                     add `atomic_compute::register_shuffle_map!(K, V)` to your binary before \
                     calling reduce_by_key or group_by_key",
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
        }
    }
}

impl<K, V, C> RddBase for ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash + Clone + Encode + Decode<()>,
    V: Data + Clone,
    C: Data + Clone + Encode + Decode<()>,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        (0..self.part.get_num_of_partitions())
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
        self.part.get_num_of_partitions()
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
    K: Data + Eq + Hash + Clone + Encode + Decode<()>,
    V: Data + Clone,
    C: Data + Clone + Encode + Decode<()>,
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
        let original_num_partitions = self.part.get_num_of_partitions();

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
            let merge_combiners = self.aggregator.merge_combiners.clone();

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
                let mut runs: Vec<SpilledRunIter<K, C>> = Vec::new();
                for orig_id in original_ids {
                    let fetched = Handle::current()
                        .block_on(
                            self.fetcher
                                .fetch_runs_spilled::<K, C>(self.shuffle_id, orig_id),
                        )
                        .map_err(DataError::from)?;
                    runs.extend(fetched);
                }
                log::debug!(
                    "sort-merge (lazy k-way, {} disk-spilled runs) prepared in {}",
                    runs.len(),
                    start.elapsed().as_millis()
                );
                return Ok(lazy_sort_merge(runs, cmp.clone(), merge_combiners));
            }

            let mut runs: Vec<std::vec::IntoIter<(K, C)>> = Vec::new();
            for orig_id in original_ids {
                let fetched = Handle::current()
                    .block_on(self.fetcher.fetch_runs::<K, C>(self.shuffle_id, orig_id))
                    .map_err(DataError::from)?;
                runs.extend(fetched.into_iter().map(IntoIterator::into_iter));
            }
            log::debug!(
                "sort-merge (lazy k-way) prepared in {}",
                start.elapsed().as_millis()
            );
            return Ok(lazy_sort_merge(runs, cmp.clone(), merge_combiners));
        }

        let mut combiners: HashMap<K, C> = HashMap::new();
        for orig_id in original_ids {
            let fut = self.fetcher.fetch::<K, C>(self.shuffle_id, orig_id);
            let result = Handle::current().block_on(fut).map_err(DataError::from)?;
            for (k, c) in result {
                combiners
                    .entry(k)
                    .and_modify(|old| (self.aggregator.merge_combiners)(old, c.clone()))
                    .or_insert(c);
            }
        }

        log::debug!("time taken for fetching {}", start.elapsed().as_millis());
        Ok(Box::new(combiners.into_iter()))
    }
}
