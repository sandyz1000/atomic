use crate::{context::RddContext, rdd::rdd_val::RddVals};
use ember_data::{
    data::Data,
    dependency::Dependency,
    error::BaseError,
    rdd::{Rdd, RddBase},
    split::CartesianSplit,
};

use crate::context::Context;
use ember_data::split::Split;
use itertools::{Itertools, iproduct};
use std::{marker::PhantomData, sync::Arc};

pub struct CartesianRdd<T: Data, U: Data> {
    vals: Arc<RddVals>,
    rdd1: Arc<dyn Rdd<Item = T>>,
    rdd2: Arc<dyn Rdd<Item = U>>,
    num_partitions_in_rdd2: usize,
    _marker_t: PhantomData<T>,
    _market_u: PhantomData<U>,
}

impl<T: Data, U: Data> CartesianRdd<T, U> {
    pub(crate) fn new(
        context: Arc<Context>,
        rdd1: Arc<dyn Rdd<Item = T>>,
        rdd2: Arc<dyn Rdd<Item = U>>,
    ) -> CartesianRdd<T, U> {
        let vals = Arc::new(RddVals::new(context));
        let num_partitions_in_rdd2 = rdd2.number_of_splits();
        CartesianRdd {
            vals,
            rdd1,
            rdd2,
            num_partitions_in_rdd2,
            _marker_t: PhantomData,
            _market_u: PhantomData,
        }
    }
}

impl<T: Data, U: Data> Clone for CartesianRdd<T, U> {
    fn clone(&self) -> Self {
        CartesianRdd {
            vals: self.vals.clone(),
            rdd1: self.rdd1.clone(),
            rdd2: self.rdd2.clone(),
            num_partitions_in_rdd2: self.num_partitions_in_rdd2,
            _marker_t: PhantomData,
            _market_u: PhantomData,
        }
    }
}

impl<T, U> RddContext for CartesianRdd<T, U>
where
    T: Data + Clone,
    U: Data + Clone,
{
    fn get_context(&self) -> Arc<Context> {
        self.vals.get_context()
    }
}

impl<T: Data + Clone, U: Data + Clone> RddBase for CartesianRdd<T, U> {
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        // create the cross product split
        let mut array =
            Vec::with_capacity(self.rdd1.number_of_splits() + self.rdd2.number_of_splits());
        for (s1, s2) in iproduct!(self.rdd1.splits().iter(), self.rdd2.splits().iter()) {
            let s1_idx = s1.get_index();
            let s2_idx = s2.get_index();
            let idx = s1_idx * self.num_partitions_in_rdd2 + s2_idx;
            array.push(Box::new(CartesianSplit {
                idx,
                s1_idx,
                s2_idx,
                s1: s1.clone(),
                s2: s2.clone(),
            }) as Box<dyn Split>);
        }
        array
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }
}

impl<T: Data + Clone, U: Data + Clone> Rdd for CartesianRdd<T, U> {
    type Item = (T, U);

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone())
    }

    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        let current_split = split
            .as_any()
            .downcast_ref::<CartesianSplit>()
            .ok_or_else(|| BaseError::DowncastFailure("CartesianSplit".to_string()))?;

        let iter1 = self.rdd1.iterator(current_split.s1)?;
        // required because iter2 must be clonable:
        let iter2: Vec<_> = self.rdd2.iterator(current_split.s2)?.collect();
        Ok(Box::new(iter1.cartesian_product(iter2.into_iter())))
    }
}
