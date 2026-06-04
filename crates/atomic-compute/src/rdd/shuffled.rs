use crate::rdd::rdd_val::RddVals;
use crate::rdd::{Rdd, RddBase};
use crate::task_registry::SHUFFLE_KEY_REGISTRY;
use atomic_data::aggregator::Aggregator;
use atomic_data::data::Data;
use atomic_data::dependency::{Dependency, ShuffleDependency, ShuffleDependencyBox};
use atomic_data::distributed::WireEncode;
use atomic_data::error::BaseError;
use atomic_data::partitioner::Partitioner;
use atomic_data::shuffle::fetcher::ShuffleFetcher;
use atomic_data::split::{ShuffledRddSplit, Split};
use bincode::{Decode, Encode};
use std::any::TypeId;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Handle;

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
        Self::new_with_staged(id, shuffle_id, parent, aggregator, part, fetcher, None)
    }

    /// Variant used when a staged pipeline (from `_task` ops) precedes the shuffle.
    /// `staged` carries `(source_partitions, preceding_ops)` so workers run the ops
    /// before writing shuffle buckets.
    pub fn new_with_staged(
        id: usize,
        shuffle_id: usize,
        parent: Arc<dyn Rdd<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Partitioner,
        fetcher: Arc<ShuffleFetcher>,
        staged: Option<(Vec<Vec<u8>>, Vec<atomic_data::distributed::PipelineOp>)>,
    ) -> Self {
        let mut vals = RddVals::new(id);

        let shuffle_dep = ShuffleDependency::new(
            shuffle_id,
            false,
            parent.clone(),
            aggregator.clone(),
            part.clone(),
        );
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
        let dep_box = ShuffleDependencyBox::from_typed_with_key(shuffle_dep, shuffle_key);
        let dep_box = if let Some((src_parts, ops)) = staged {
            dep_box.with_staged_pipeline(src_parts, ops)
        } else {
            dep_box
        };
        vals.dependencies.push(Dependency::Shuffle(Arc::new(dep_box)));
        let vals = Arc::new(vals);
        ShuffledRdd {
            parent,
            aggregator,
            vals,
            part,
            shuffle_id,
            fetcher,
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
        if let Some(tracker) = atomic_data::env::get_map_output_tracker() {
            if let Some(n) = tracker.get_coalesced_partitions(self.shuffle_id) {
                return n;
            }
        }
        self.part.get_num_of_partitions()
    }

    fn partitioner(&self) -> Option<Partitioner> {
        Some(self.part.clone())
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside iterator_any shuffledrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn Data>),
        ))
    }

    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside cogroup iterator_any shuffledrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, Box::new(v))) as Box<dyn Data>),
        ))
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
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        log::debug!("compute inside shuffled rdd");
        let start = Instant::now();

        let coalesced_id = split.get_index();
        let original_num_partitions = self.part.get_num_of_partitions();

        // Determine which original reduce-partition IDs this coalesced split covers.
        let original_ids: Vec<usize> = if let Some(tracker) =
            atomic_data::env::get_map_output_tracker()
        {
            if let Some(coalesced_n) = tracker.get_coalesced_partitions(self.shuffle_id) {
                // Map coalesced_id → original reduce partition range.
                // Simple even-split mapping: coalesced partition i covers
                // [i * (original / coalesced), (i+1) * (original / coalesced)).
                let ratio = original_num_partitions.max(1);
                let per_coalesced = (ratio + coalesced_n - 1) / coalesced_n; // ceil
                let start_id = coalesced_id * per_coalesced;
                let end_id = ((coalesced_id + 1) * per_coalesced).min(original_num_partitions);
                (start_id..end_id).collect()
            } else {
                vec![coalesced_id]
            }
        } else {
            vec![coalesced_id]
        };

        let mut combiners: HashMap<K, C> = HashMap::new();
        for orig_id in original_ids {
            let fut = self.fetcher.fetch::<K, C>(self.shuffle_id, orig_id);
            let result = Handle::current()
                .block_on(fut)
                .map_err(|e| BaseError::Other(format!("Shuffle fetch error: {}", e)))?;
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
