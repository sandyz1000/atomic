use crate::context::{Context, RddContext};
use crate::rdd::rdd_val::RddVals;
use crate::rdd::{Rdd, RddBase};
use ember_data::data::Data;
use ember_data::dependency::Dependency;
use ember_data::error::BaseError;
use ember_data::partitioner::Partitioner;
use ember_data::split::Split;
use ember_utils::random::RandomSampler;
use std::marker::PhantomData;
use std::sync::Arc;

// #[derive(Serialize, Deserialize)]
pub struct PartitionwiseSampledRdd<T: Data> {
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    sampler: Arc<dyn RandomSampler<T>>,
    preserves_partitioning: bool,
    _marker_t: PhantomData<T>,
}

impl<T: Data> PartitionwiseSampledRdd<T> {
    pub(crate) fn new(
        prev: Arc<dyn Rdd<Item = T>>,
        sampler: Arc<dyn RandomSampler<T>>,
        preserves_partitioning: bool,
    ) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies.push(Dependency::OneToOne {
            rdd_base: prev.get_rdd_base(),
        });
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

impl<T: Data> Clone for PartitionwiseSampledRdd<T> {
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

impl<T: Data + Clone> RddContext for PartitionwiseSampledRdd<T> {
    fn get_context(&self) -> Arc<Context> {
        self.vals.get_context()
    }
}

impl<T: Data> RddBase for PartitionwiseSampledRdd<T> {
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }

    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    fn partitioner(&self) -> Option<Partitioner> {
        if self.preserves_partitioning {
            self.prev.partitioner()
        } else {
            None
        }
    }

    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        self.iterator_any(split)
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside PartitionwiseSampledRdd iterator_any");
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }
}

impl<T: Data> Rdd for PartitionwiseSampledRdd<T> {
    type Item = T;
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        let sampler_func = self.sampler.get_sampler(None);
        let iter = self.prev.iterator(split)?;
        Ok(Box::new(sampler_func(iter).into_iter()) as Box<dyn Iterator<Item = T>>)
    }
}
