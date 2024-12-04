use crate::context::Context;
use crate::dependency::{Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependencyTrait};
use crate::error::Result;
use crate::rdd::rdd::{Rdd, RddBase, RddVals};
use crate::ser_data::{AnyData, Data};
use crate::utils::random::RandomSampler;
use serde_derive::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct PartitionwiseSampledRdd<T: Data, RDD, Nd, Sd, Rs> {
    prev: Arc<RDD>,
    vals: Arc<RddVals<Nd, Sd>>,
    sampler: Arc<Rs>,
    preserves_partitioning: bool,
    _marker_t: PhantomData<T>,
}

impl<T: Data, RDD, N, S, R> PartitionwiseSampledRdd<T, RDD, N, S, R> 
where 
    RDD: Rdd<Item = T>,
    R: RandomSampler<T>,
    N: NarrowDependencyTrait,
    S: ShuffleDependencyTrait,
{
    pub(crate) fn new(
        prev: Arc<RDD>,
        sampler: Arc<R>,
        preserves_partitioning: bool,
    ) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);

        PartitionwiseSampledRdd {
            prev,
            vals,
            sampler,
            preserves_partitioning,
            _marker_t: PhantomData,
        }
    }
}

impl<T, RDD, Nd, Sd, Rs> Clone for PartitionwiseSampledRdd<T, RDD, Nd, Sd, Rs> 
where 
    T: Data,
    RDD: Rdd<Item = T>,
    Rs: RandomSampler<T>,
    Nd: NarrowDependencyTrait,
    Sd: ShuffleDependencyTrait,
{
    fn clone(&self) -> Self {
        PartitionwiseSampledRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            sampler: self.sampler.clone(),
            preserves_partitioning: self.preserves_partitioning,
            _marker_t: PhantomData,
        }
    }
}

impl<T, RDD, Nd, Sd, Rs> RddBase for PartitionwiseSampledRdd<T, RDD, Nd, Sd, Rs> 
where 
    T: Data,
    RDD: Rdd<Item = T>,
    Rs: RandomSampler<T>,
    Nd: NarrowDependencyTrait,
    Sd: ShuffleDependencyTrait,
{
    type Split = RDD::Split;
    type Partitioner = RDD::Partitioner;
    type ShuffleDeps = Sd;
    type NarrowDeps = Nd;

    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency<Nd, Sd>> {
        self.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<Self::Split>> {
        self.prev.splits()
    }

    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    fn partitioner(&self) -> Option<Box<Self::Partitioner>> {
        if self.preserves_partitioning {
            self.prev.partitioner()
        } else {
            None
        }
    }

    fn cogroup_iterator_any(
        &self,
        split: Box<Self::Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        self.iterator_any(split)
    }

    fn iterator_any(
        &self,
        split: Box<Self::Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        log::debug!("inside PartitionwiseSampledRdd iterator_any");
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x)),
        ))
    }
    
}

// impl<T: Data, V: Data> RddBase for PartitionwiseSampledRdd<(T, V)> {
//     fn cogroup_iterator_any(
//         &self,
//         split: Box<Self::Split>,
//     ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
//         log::debug!("inside PartitionwiseSampledRdd cogroup_iterator_any",);
//         Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
//             Box::new((k, Box::new(v)))
//         })))
//     }
// }

impl<T: Data, RDD, Nd, Sd, Rs> Rdd for PartitionwiseSampledRdd<T, RDD, Nd, Sd, Rs> 
where 
    RDD: Rdd<Item = T>,
    Rs: RandomSampler<T> + 'static,
    Nd: NarrowDependencyTrait + 'static,
    Sd: ShuffleDependencyTrait + 'static,
{
    type Item = T;
    type RddBase = RDD;

    fn get_rdd_base(&self) -> Arc<Self::RddBase> {
        Arc::new(self.clone())
    }

    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<Self::Split>) -> Result<Box<impl Iterator<Item = Self::Item>>> {
        let sampler_func = self.sampler.get_sampler(None);
        let iter = self.prev.iterator(split)?;
        Ok(Box::new(sampler_func(iter).into_iter()))
    }
    
}
