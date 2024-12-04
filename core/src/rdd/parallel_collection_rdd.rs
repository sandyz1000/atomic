//! This module implements parallel collection RDD for dividing the input collection for parallel processing.
use std::sync::{Arc, Weak};

use crate::context::Context;
use crate::dependency::{Dependency, NarrowDependencyTrait, ShuffleDependencyTrait};
use crate::error::Result;
use crate::rdd::rdd::{Rdd, RddBase, RddVals};
use crate::ser_data::{AnyData, Data};
use crate::split::Split;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};

/// A collection of objects which can be sliced into partitions with a partitioning function.
pub trait Chunkable<D>
where
    D: Data,
{
    fn slice_with_set_parts(self, parts: usize) -> Vec<Arc<Vec<D>>>;

    fn slice(self) -> Vec<Arc<Vec<D>>>
    where
        Self: Sized,
    {
        let as_many_parts_as_cpus = num_cpus::get();
        self.slice_with_set_parts(as_many_parts_as_cpus)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ParallelCollectionSplit<T> {
    rdd_id: i64,
    index: usize,
    values: Arc<Vec<T>>,
}

impl<T: Data> Split for ParallelCollectionSplit<T> {
    fn get_index(&self) -> usize {
        self.index
    }
}

impl<T: Data> ParallelCollectionSplit<T> {
    fn new(rdd_id: i64, index: usize, values: Arc<Vec<T>>) -> Self {
        ParallelCollectionSplit {
            rdd_id,
            index,
            values,
        }
    }
    // Lot of unnecessary cloning is there. Have to refactor for better performance
    fn iterator(&self) -> Box<impl Iterator<Item = T>> {
        let data = self.values.clone();
        let len = data.len();
        Box::new((0..len).map(move |i| data[i].clone()))
    }
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollectionVals<T, ND, SD> {
    vals: Arc<RddVals<ND, SD>>,
    #[serde(skip_serializing, skip_deserializing)]
    context: Weak<Context>,
    splits_: Vec<Arc<Vec<T>>>,
    num_slices: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollection<T, ND, SD> {
    #[serde(skip_serializing, skip_deserializing)]
    name: Mutex<String>,
    rdd_vals: Arc<ParallelCollectionVals<T, ND, SD>>,
}

impl<T: Data, ND, SD> Clone for ParallelCollection<T, ND, SD> 
where
    ND: NarrowDependencyTrait, 
    SD: ShuffleDependencyTrait
{
    fn clone(&self) -> Self {
        ParallelCollection {
            name: Mutex::new(self.name.lock().clone()),
            rdd_vals: self.rdd_vals.clone(),
        }
    }
}

impl<T: Data, ND, SD> ParallelCollection<T, ND, SD> 
where
    ND: NarrowDependencyTrait, 
    SD: ShuffleDependencyTrait
{
    pub fn new<I>(context: Arc<Context>, data: I, num_slices: usize) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        // TODO: Verify and remove Arc, this may add more memory in the heap
        ParallelCollection {
            name: Mutex::new("parallel_collection".to_owned()),
            rdd_vals: Arc::new(ParallelCollectionVals {
                context: Arc::downgrade(&context),
                vals: Arc::new(RddVals::new(context.clone())),
                splits_: ParallelCollection::<T, ND, SD>::slice(data, num_slices),
                num_slices,
            }),
        }
    }

    pub fn from_chunkable<C>(context: Arc<Context>, data: C) -> Self
    where
        C: Chunkable<T>,
    {
        let splits_ = data.slice();
        let rdd_vals = ParallelCollectionVals {
            context: Arc::downgrade(&context),
            vals: Arc::new(RddVals::new(context.clone())),
            num_slices: splits_.len(),
            splits_,
        };
        ParallelCollection {
            name: Mutex::new("parallel_collection".to_owned()),
            rdd_vals: Arc::new(rdd_vals),
        }
    }

    /// TODO: Add docs
    /// Explore the logic and add documentation
    fn slice<I>(data: I, num_slices: usize) -> Vec<Arc<Vec<T>>>
    where
        I: IntoIterator<Item = T>,
    {
        if num_slices < 1 {
            panic!("Number of slices should be greater than or equal to 1");
        } else {
            let mut slice_count = 0;
            let data: Vec<_> = data.into_iter().collect();
            let data_len = data.len();
            let mut end: usize = ((slice_count + 1) * data_len) / num_slices;
            let mut output = Vec::new();
            let mut tmp = Vec::new();
            let mut iter_count = 0;
            for i in data {
                if iter_count < end {
                    tmp.push(i);
                    iter_count += 1;
                } else {
                    slice_count += 1;
                    end = ((slice_count + 1) * data_len) / num_slices;
                    output.push(Arc::new(tmp.drain(..).collect::<Vec<_>>()));
                    tmp.push(i);
                    iter_count += 1;
                }
            }
            output.push(Arc::new(tmp.drain(..).collect::<Vec<_>>()));
            output
        }
    }
}

// impl<K: Data, V: Data> RddBase for ParallelCollection<(K, V)> {
//     fn cogroup_iterator_any(
//         &self,
//         split: Box<Self::Split>,
//     ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
//         log::debug!("inside iterator_any parallel collection",);
//         Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
//             Box::new((k, Box::new(v)))
//         })))
//     }
// }

impl<T: Data, Nd, Sd> RddBase for ParallelCollection<T, Nd, Sd> 
where
    Nd: NarrowDependencyTrait + 'static, 
    Sd: ShuffleDependencyTrait + 'static
{
    type Split = Self::Split;
    type Partitioner = Self::Partitioner;
    type ShuffleDeps = Sd;
    type NarrowDeps = Nd;

    fn get_rdd_id(&self) -> usize {
        self.rdd_vals.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.rdd_vals.vals.context.upgrade().unwrap()
    }

    fn get_op_name(&self) -> String {
        self.name.lock().to_owned()
    }

    fn register_op_name(&self, name: &str) {
        let own_name = &mut *self.name.lock();
        *own_name = name.to_owned();
    }

    fn get_dependencies(&self) -> Vec<Dependency<Nd, Sd>> {
        self.rdd_vals.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<Self::Split>> {
        (0..self.rdd_vals.splits_.len())
            .map(|i| {
                Box::new(ParallelCollectionSplit::new(
                    self.rdd_vals.vals.id as i64,
                    i,
                    self.rdd_vals.splits_[i as usize].clone(),
                ))
            })
            .collect()
    }

    fn number_of_splits(&self) -> usize {
        self.rdd_vals.splits_.len()
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
        log::debug!("inside iterator_any parallel collection",);
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x)),
        ))
    }
    
}

impl<T: Data, ND, SD> Rdd for ParallelCollection<T, ND, SD> 
where
    ND: NarrowDependencyTrait + 'static, 
    SD: ShuffleDependencyTrait + 'static
{
    type Item = T;
    type RddBase = Self;

    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>> {
        let par = ParallelCollection {
            name: Mutex::new(self.name.lock().clone()),
            rdd_vals: self.rdd_vals.clone(),
        };

        Arc::new(par)
    }

    fn get_rdd_base(&self) -> Arc<Self::RddBase> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<Self::Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        if let Some(s) = split.downcast_ref::<ParallelCollectionSplit<T>>() {
            Ok(s.iterator())
        } else {
            panic!(
                "Got split object from different concrete type other than ParallelCollectionSplit"
            )
        }
    }
    
}
