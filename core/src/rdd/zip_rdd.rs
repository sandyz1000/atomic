use std::cmp::min;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::context::Context;
use crate::dependency::{
    Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependencyTrait,
};
use crate::error::{Error, Result};
use crate::rdd::rdd::{Rdd, RddBase, RddVals};
use crate::ser_data::{AnyData, Data};
use crate::split::Split;
use dyn_clone::DynClone;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct ZippedPartitionsSplit<S> {
    fst_idx: usize,
    sec_idx: usize,
    idx: usize,
    fst_split: Box<S>,
    sec_split: Box<S>,
}

impl<S: Split + Clone + DynClone> Split for ZippedPartitionsSplit<S> {
    fn get_index(&self) -> usize {
        self.idx
    }
}

#[derive(Serialize, Deserialize)]
pub struct ZippedPartitionsRdd<F: Data, S: Data, R1, R2, ND, SD> {
    first: Arc<R1>,
    second: Arc<R2>,
    vals: Arc<RddVals<ND, SD>>,
    _marker_t: PhantomData<(F, S)>,
}

impl<F, S, R1, R2, ND, SD> Clone for ZippedPartitionsRdd<F, S, R1, R2, ND, SD>
where
    F: Data,
    S: Data,
    R1: Rdd<Item = F>,
    R2: Rdd<Item = S>,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait,
{
    fn clone(&self) -> Self {
        ZippedPartitionsRdd {
            first: self.first.clone(),
            second: self.second.clone(),
            vals: self.vals.clone(),
            _marker_t: PhantomData,
        }
    }
}

impl<F, S, R1, R2, ND, SD> RddBase for ZippedPartitionsRdd<F, S, R1, R2, ND, SD>
where
    F: Data,
    S: Data,
    R1: Rdd<Item = F>,
    R2: Rdd<Item = S>,
    ND: NarrowDependencyTrait + 'static,
    SD: ShuffleDependencyTrait + 'static,
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

    fn splits(&self) -> Vec<Box<impl Split>> {
        let mut arr = Vec::with_capacity(min(
            self.first.number_of_splits(),
            self.second.number_of_splits(),
        ));

        for (fst, sec) in self.first.splits().iter().zip(self.second.splits().iter()) {
            let fst_idx = fst.get_index();
            let sec_idx = sec.get_index();

            arr.push(Box::new(ZippedPartitionsSplit {
                fst_idx,
                sec_idx,
                idx: fst_idx,
                fst_split: fst.clone(),
                sec_split: sec.clone(),
            }))
        }
        arr
    }

    fn number_of_splits(&self) -> usize {
        self.splits().len()
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

    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        self.iterator_any(split)
    }
}

impl<F, S, R1, R2, ND, SD> Rdd for ZippedPartitionsRdd<F, S, R1, R2, ND, SD>
where
    F: Data,
    S: Data,
    R1: Rdd<Item = F>,
    R2: Rdd<Item = S>,
    ND: NarrowDependencyTrait + 'static,
    SD: ShuffleDependencyTrait + 'static,
{
    type Item = (F, S);

    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<impl RddBase> {
        Arc::new(self.clone())
    }

    fn compute<SC: Split + Clone + dyn_clone::DynClone>(
        &self,
        split: Box<SC>,
    ) -> Result<Box<impl Iterator<Item = Self::Item>>> {
        let current_split = split
            .downcast::<ZippedPartitionsSplit>()
            .or(Err(Error::DowncastFailure("ZippedPartitionsSplit")))?;

        let fst_iter = self.first.iterator(current_split.fst_split.clone())?;
        let sec_iter = self.second.iterator(current_split.sec_split.clone())?;
        Ok(Box::new(fst_iter.zip(sec_iter)))
    }

    fn iterator<SC: Split + Clone + dyn_clone::DynClone>(
        &self,
        split: Box<SC>,
    ) -> Result<Box<impl Iterator<Item = Self::Item>>> {
        self.compute(split.clone())
    }
}

impl<F, S, R1, R2, ND, SD> ZippedPartitionsRdd<F, S, R1, R2, ND, SD>
where
    F: Data,
    S: Data,
    R1: Rdd<Item = F>,
    R2: Rdd<Item = S>,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait,
{
    pub fn new(first: Arc<R1>, second: Arc<R2>) -> Self {
        let mut vals = RddVals::new(first.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(first.get_rdd_base()),
            )));
        let vals = Arc::new(vals);

        ZippedPartitionsRdd {
            first,
            second,
            vals,
            _marker_t: PhantomData,
        }
    }
}
