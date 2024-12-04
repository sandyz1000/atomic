use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{Dependency, NarrowDependencyTrait, ShuffleDependency, ShuffleDependencyTrait};
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::rdd::{Rdd, RddBase, RddVals};
use crate::ser_data::{AnyData, Data, SerFunc};
use crate::shuffle::ShuffleFetcher;
use crate::split::Split;
use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct ShuffledRddSplit {
    index: usize,
}

impl ShuffledRddSplit {
    fn new(index: usize) -> Self {
        ShuffledRddSplit { index }
    }
}

impl Split for ShuffledRddSplit {
    fn get_index(&self) -> usize {
        self.index
    }
}

// #[derive(Serialize, Deserialize)]
#[derive(Serialize)]
pub struct ShuffledRdd<K: Data + Eq + Hash, V: Data, C: Data, F1, F2, F3, RDD, Pa, N, S> {
    parent: Arc<RDD>,
    aggregator: Arc<Aggregator<K, V, C, F1, F2, F3>>,
    vals: Arc<RddVals<N, S>>,
    part: Box<Pa>,
    shuffle_id: usize,
}

impl<K, V, C, F1, F2, F3, RDD, Pa, N, S> Clone for ShuffledRdd<K, V, C, F1, F2, F3, RDD, Pa, N, S> 
where 
    K: Data + Eq + Hash,
    V: Data,
    C: Data,
    RDD: Rdd<Item = (K, V)>,
    F1: SerFunc<V, Output = C>,
    F2: SerFunc<(C, V), Output = C>,
    F3: SerFunc<(C, C), Output = C>,
    Pa: Partitioner,
    N: NarrowDependencyTrait,
    S: ShuffleDependencyTrait
{
    fn clone(&self) -> Self {
        ShuffledRdd {
            parent: self.parent.clone(),
            aggregator: self.aggregator.clone(),
            vals: self.vals.clone(),
            part: self.part.clone(),
            shuffle_id: self.shuffle_id,
        }
    }
}

impl<K, V, C, F1, F2, F3, RDD, Pa, N, S> ShuffledRdd<K, V, C, F1, F2, F3, RDD, Pa, N, S> 
where
    K: Data + Eq + Hash,
    RDD: Rdd<Item = (K, V)>,
    V: Data,
    C: Data,
    Pa: Partitioner ,
    F1: SerFunc<V, Output = C>,
    F2: SerFunc<(C, V), Output = C>,
    F3: SerFunc<(C, C), Output = C>,
{
    pub(crate) fn new(
        parent: Arc<RDD>,
        aggregator: Arc<Aggregator<K, V, C, F1, F2, F3>>,
        part: Box<Pa>,
    ) -> Self {
        let ctx = parent.get_context();
        let shuffle_id = ctx.new_shuffle_id();
        let mut vals = RddVals::new(ctx);

        vals.dependencies
            .push(Dependency::ShuffleDependency(Arc::new(
                ShuffleDependency::new(
                    shuffle_id,
                    false,
                    parent.get_rdd_base(),
                    aggregator.clone(),
                    part.clone(),
                ),
            )));
        let vals = Arc::new(vals);
        ShuffledRdd {
            parent,
            aggregator,
            vals,
            part,
            shuffle_id,
        }
    }
}

impl<K, V, C, F1, F2, F3, RDD, P, N, S> RddBase for ShuffledRdd<K, V, C, F1, F2, F3, RDD, P, N, S> 
where
    K: Data + Eq + Hash,
    RDD: Rdd<Item = (K, V)>,
    V: Data,
    C: Data,
    P: Partitioner,
    F1: SerFunc<V, Output = C>,
    F2: SerFunc<(C, V), Output = C>,
    F3: SerFunc<(C, C), Output = C>,
    N: NarrowDependencyTrait + 'static,
    S: ShuffleDependencyTrait + 'static
{
    type Split = RDD::Split;
    type Partitioner = P;
    type ShuffleDeps = S;
    type NarrowDeps = N;

    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency<Self::NarrowDeps, Self::ShuffleDeps>> {
        self.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<Self::Split>> {
        (0..self.part.get_num_of_partitions())
            .map(|x| Box::new(ShuffledRddSplit::new(x)))
            .collect()
    }

    fn number_of_splits(&self) -> usize {
        self.part.get_num_of_partitions()
    }

    fn partitioner(&self) -> Option<Box<Self::Partitioner>> {
        Some(self.part.clone())
    }

    fn iterator_any(
        &self,
        split: Box<Self::Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        log::debug!("inside iterator_any shuffledrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v))),
        ))
    }

    fn cogroup_iterator_any(
        &self,
        split: Box<Self::Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        log::debug!("inside cogroup iterator_any shuffledrdd",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v)))
        })))
    }
    
}

impl<K, V, C, F1, F2, F3, RDD, Pa, N, S> Rdd for ShuffledRdd<K, V, C, F1, F2, F3, RDD, Pa, N, S> 
where
    K: Data + Eq + Hash,
    RDD: Rdd<Item = (K, V)>,
    V: Data,
    C: Data,
    Pa: Partitioner,
    F1: SerFunc<V, Output = C>,
    F2: SerFunc<(C, V), Output = C>,
    F3: SerFunc<(C, C), Output = C>,
    N: NarrowDependencyTrait + 'static,
    S: ShuffleDependencyTrait + 'static

{
    type Item = (K, C);
    type RddBase = RDD;

    fn get_rdd_base(&self) -> Arc<Self::RddBase> {
        Arc::new(self.clone())
    }

    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<Self::Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        log::debug!("compute inside shuffled rdd");
        let start = Instant::now();

        let fut = ShuffleFetcher::fetch::<K, C>(
            self.shuffle_id, 
            split.get_index()
        );
        
        let mut combiners: HashMap<K, Option<C>> = HashMap::new();
        for (k, c) in futures::executor::block_on(fut)?.into_iter() {
            if let Some(old_c) = combiners.get_mut(&k) {
                let old = old_c.take().unwrap();
                let input = ((old, c),);
                let output = self.aggregator.merge_combiners.call(input);
                *old_c = Some(output);
            } else {
                combiners.insert(k, Some(c));
            }
        }

        log::debug!("time taken for fetching {}", start.elapsed().as_millis());
        Ok(Box::new(
            combiners.into_iter().map(|(k, v)| (k, v.unwrap())),
        ))
    }
    
}
