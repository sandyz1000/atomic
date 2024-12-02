use crate::context::Context;
use crate::dependency::{Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependencyTrait};
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::rdd::{Rdd, RddBase, RddVals};
use crate::ser_data::{AnyData, Data};
use crate::split::Split;
use crate::utils::random::RandomSampler;
use serde_derive::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct PartitionwiseSampledRdd<T: Data, RDD, ND, SD, RS> {
    prev: Arc<RDD>,
    vals: Arc<RddVals<ND, SD>>,
    sampler: Arc<RS>,
    preserves_partitioning: bool,
    _marker_t: PhantomData<T>,
}

impl<T: Data, RDD, ND, SD, RS> PartitionwiseSampledRdd<T, RDD, ND, SD, RS> 
where 
    RDD: Rdd<Item = T>,
    RS: RandomSampler<T>,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait,
{
    pub(crate) fn new(
        prev: Arc<RDD>,
        sampler: Arc<RS>,
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

impl<T, RDD, ND, SD, RS> Clone for PartitionwiseSampledRdd<T, RDD, ND, SD, RS> 
where 
    T: Data,
    RDD: Rdd<Item = T>,
    RS: RandomSampler<T>,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait,
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

impl<T, RDD, ND, SD, RS> RddBase for PartitionwiseSampledRdd<T, RDD, ND, SD, RS> 
where 
    T: Data,
    RDD: Rdd<Item = T>,
    RS: RandomSampler<T>,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency<ND, SD>> {
        self.vals.dependencies.clone()
    }

    fn splits<S: Split + ?Sized>(&self) -> Vec<Box<S>> {
        self.prev.splits()
    }

    fn number_of_splits<S: Split + ?Sized>(&self) -> usize {
        self.prev.number_of_splits()
    }

    fn partitioner<P: Partitioner + ?Sized>(&self) -> Option<Box<P>> {
        if self.preserves_partitioning {
            self.prev.partitioner()
        } else {
            None
        }
    }

    fn cogroup_iterator_any<S: Split + ?Sized>(
        &self,
        split: Box<S>,
    ) -> Result<Box<impl Iterator<Item = Box<impl AnyData>>>> {
        self.iterator_any(split)
    }

    fn iterator_any<S: Split + ?Sized>(
        &self,
        split: Box<S>,
    ) -> Result<Box<impl Iterator<Item = Box<impl AnyData>>>> {
        log::debug!("inside PartitionwiseSampledRdd iterator_any");
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x)),
        ))
    }
}

// impl<T: Data, V: Data> RddBase for PartitionwiseSampledRdd<(T, V)> {
//     fn cogroup_iterator_any(
//         &self,
//         split: Box<dyn Split>,
//     ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
//         log::debug!("inside PartitionwiseSampledRdd cogroup_iterator_any",);
//         Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
//             Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
//         })))
//     }
// }

impl<T: Data, RDD, ND, SD, RS> Rdd for PartitionwiseSampledRdd<T, RDD, ND, SD, RS> 
where 
    RDD: Rdd<Item = T>,
    RS: RandomSampler<T>,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait,
{
    type Item = T;
    fn get_rdd_base(&self) -> Arc<impl RddBase> {
        Arc::new(self.clone())
    }

    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute<S: Split + ?Sized>(&self, split: Box<S>) -> Result<Box<impl Iterator<Item = Self::Item>>> {
        let sampler_func = self.sampler.get_sampler(None);
        let iter = self.prev.iterator(split)?;
        Ok(Box::new(sampler_func(iter).into_iter()))
    }
}
