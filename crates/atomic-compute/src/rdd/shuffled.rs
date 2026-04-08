use crate::rdd::rdd_val::RddVals;
use crate::rdd::{Rdd, RddBase};
use atomic_data::aggregator::Aggregator;
use atomic_data::data::Data;
use atomic_data::dependency::{Dependency, ShuffleDependency};
use atomic_data::distributed::WireEncode;
use atomic_data::error::BaseError;
use atomic_data::partitioner::Partitioner;
use atomic_data::shuffle::fetcher::ShuffleFetcher;
use atomic_data::split::{ShuffledRddSplit, Split};
use bincode::{Decode, Encode};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;

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
        let mut vals = RddVals::new(id);

        vals.dependencies.push(Dependency::Shuffle(Arc::new(
            ShuffleDependency::new(
                shuffle_id,
                false,
                parent.clone(),
                aggregator.clone(),
                part.clone(),
            )
            .into(),
        )));
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

        let fut = self
            .fetcher
            .fetch::<K, C>(self.shuffle_id, split.get_index());
        let mut combiners: HashMap<K, C> = HashMap::new();
        let result = futures::executor::block_on(fut)
            .map_err(|e| BaseError::Other(format!("Shuffle fetch error: {}", e)))?;
        for (k, c) in result.into_iter() {
            combiners
                .entry(k)
                .and_modify(|old| (self.aggregator.merge_combiners)(old, c.clone()))
                .or_insert(c);
        }

        log::debug!("time taken for fetching {}", start.elapsed().as_millis());
        Ok(Box::new(combiners.into_iter()))
    }
}
