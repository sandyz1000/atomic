use itertools::{iproduct, Itertools};

use crate::context::Context;
use crate::dependency::{Dependency, NarrowDependencyTrait, ShuffleDependencyTrait};
use crate::error::{Error, Result};
use crate::rdd::rdd::{Rdd, RddBase, RddVals};
use crate::ser_data::{AnyData, Data};
use crate::split::Split;
use serde_derive::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
pub struct CartesianSplit<S: Split> {
    idx: usize,
    s1_idx: usize,
    s2_idx: usize,
    s1: Box<S>,
    s2: Box<S>,
}

impl<S> Split for CartesianSplit<S> 
where
    S: Split + Clone
{
    fn get_index(&self) -> usize {
        self.idx
    }
}

#[derive(Serialize, Deserialize)]
pub struct CartesianRdd<T: Data, U: Data, R1, R2, ND, SD> {
    vals: Arc<RddVals<ND, SD>>,
    rdd1: Arc<R1>,
    rdd2: Arc<R2>,
    num_partitions_in_rdd2: usize,
    _marker_t: PhantomData<T>,
    _market_u: PhantomData<U>,
}

impl<T, U, R1, R2, ND, SD> CartesianRdd<T, U, R1, R2, ND, SD> 
where
    ND:NarrowDependencyTrait, 
    SD: ShuffleDependencyTrait,
    T: Data, 
    U: Data,
    R1: Rdd<Item = T>,
    R2: Rdd<Item = U>
{
    pub(crate) fn new(
        rdd1: Arc<R1>,
        rdd2: Arc<R2>,
    ) -> Self {
        let vals = Arc::new(RddVals::new(rdd1.get_context()));
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

impl<T, U, R1, R2, ND, SD> Clone for CartesianRdd<T, U, R1, R2, ND, SD> 
where
    ND:NarrowDependencyTrait, 
    SD: ShuffleDependencyTrait,
    T: Data, 
    U: Data,
    R1: Rdd<Item = T>,
    R2: Rdd<Item = U>
{
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

impl<T: Data, U: Data, R1, R2, ND, SD> RddBase for CartesianRdd<T, U, R1, R2, ND, SD> 
where
    R1: Rdd<Item = T>,
    R2: Rdd<Item = U>,
    ND: NarrowDependencyTrait + 'static,
    SD: ShuffleDependencyTrait + 'static
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
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        ))
    }
}

impl<T: Data, U: Data, R1, R2, ND, SD> Rdd for CartesianRdd<T, U, R1, R2, ND, SD> 
where
    R1: Rdd<Item = T>,
    R2: Rdd<Item = U>,
    ND: NarrowDependencyTrait + 'static,
    SD: ShuffleDependencyTrait + 'static
{
    type Item = (T, U);
    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>>
    where
        Self: Sized,
    {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<impl RddBase> {
        Arc::new(self.clone())
    }

    fn compute<S: Split + ?Sized>(&self, split: Box<S>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let current_split: Box<CartesianSplit> = split
            .downcast::<CartesianSplit>()
            .or(Err(Error::DowncastFailure("CartesianSplit")))?;

        let iter1 = self.rdd1.iterator(current_split.s1)?;
        // required because iter2 must be clonable:
        let iter2: Vec<_> = self.rdd2.iterator(current_split.s2)?.collect();
        Ok(Box::new(iter1.cartesian_product(iter2.into_iter())))
    }
}
