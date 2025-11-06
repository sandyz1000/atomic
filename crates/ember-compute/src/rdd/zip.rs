use crate::error::Error;
use crate::rdd::rdd_val::RddVals;
use crate::rdd::{Rdd, RddBase};
use ember_data::data::Data;
use ember_data::dependency::Dependency;
use ember_data::error::BaseError;
use ember_data::split::{Split, ZippedPartitionsSplit};
use std::cmp::min;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct ZippedPartitionsRdd<F, S>
where
    F: Data + Clone,
    S: Data + Clone,
{
    first: Arc<dyn Rdd<Item = F>>,
    second: Arc<dyn Rdd<Item = S>>,
    vals: Arc<RddVals>,
    _marker_t: PhantomData<(F, S)>,
}

impl<F, S> ZippedPartitionsRdd<F, S>
where
    F: Data + Clone,
    S: Data + Clone,
{
    pub fn new(id: usize, first: Arc<dyn Rdd<Item = F>>, second: Arc<dyn Rdd<Item = S>>) -> Self {
        let dependencies = vec![
            Dependency::new_one_to_one(first.get_rdd_base()),
            Dependency::new_one_to_one(second.get_rdd_base()),
        ];
        let mut vals = RddVals::new(id);
        vals.dependencies = dependencies;
        ZippedPartitionsRdd {
            first,
            second,
            vals: Arc::new(vals),
            _marker_t: PhantomData,
        }
    }
}

impl<F, S> Clone for ZippedPartitionsRdd<F, S>
where
    F: Data + Clone,
    S: Data + Clone,
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

impl<F, S> RddBase for ZippedPartitionsRdd<F, S>
where
    F: Data + Clone,
    S: Data + Clone,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
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
            }) as Box<dyn Split>)
        }
        arr
    }

    fn number_of_splits(&self) -> usize {
        self.splits().len()
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }

    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        self.iterator_any(split)
    }
}

impl<F, S> Rdd for ZippedPartitionsRdd<F, S>
where
    F: Data + Clone,
    S: Data + Clone,
{
    type Item = (F, S);

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        let current_split = split
            .as_any()
            .downcast_ref::<ZippedPartitionsSplit>()
            .ok_or(Error::DowncastFailure("ZippedPartitionsSplit"))?;

        let fst_iter = self.first.iterator(current_split.fst_split.clone())?;
        let sec_iter = self.second.iterator(current_split.sec_split.clone())?;
        Ok(Box::new(fst_iter.zip(sec_iter)))
    }

    fn iterator(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        self.compute(split.clone())
    }
}
