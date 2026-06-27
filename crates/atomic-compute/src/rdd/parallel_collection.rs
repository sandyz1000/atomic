//! This module implements parallel collection RDD for dividing the input collection for parallel processing.
use crate::rdd::rdd_val::RddVals;
use crate::rdd::{Rdd, RddBase};
use atomic_data::data::Data;
use atomic_data::dependency::Dependency;
use atomic_data::error::BaseError;
use atomic_data::split::{ParallelCollectionSplit, Split};
use parking_lot::Mutex;
use std::sync::Arc;

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

pub struct ParallelCollectionVals<T> {
    vals: Arc<RddVals>,
    splits_: Vec<Arc<Vec<T>>>,
}

pub struct ParallelCollection<T> {
    name: Mutex<String>,
    rdd_vals: Arc<ParallelCollectionVals<T>>,
}

impl<T: Data> Clone for ParallelCollection<T> {
    fn clone(&self) -> Self {
        ParallelCollection {
            name: Mutex::new(self.name.lock().clone()),
            rdd_vals: self.rdd_vals.clone(),
        }
    }
}

impl<T: Data> ParallelCollection<T> {
    pub fn new<I>(id: usize, data: I, num_slices: usize) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        ParallelCollection {
            name: Mutex::new("parallel_collection".to_owned()),
            rdd_vals: Arc::new(ParallelCollectionVals {
                vals: Arc::new(RddVals::new(id)),
                splits_: ParallelCollection::slice(data, num_slices),
            }),
        }
    }

    pub fn from_chunkable<C>(id: usize, data: C) -> Self
    where
        C: Chunkable<T>,
    {
        let splits_ = data.slice();
        let rdd_vals = ParallelCollectionVals {
            vals: Arc::new(RddVals::new(id)),
            splits_,
        };
        ParallelCollection {
            name: Mutex::new("parallel_collection".to_owned()),
            rdd_vals: Arc::new(rdd_vals),
        }
    }

    /// Split `data` into exactly `num_slices` partitions, mirroring Spark's
    /// `ParallelCollectionRDD.slice`: partition `i` holds the half-open index range
    /// `[i * len / num_slices, (i + 1) * len / num_slices)`. Sizes differ by at most one,
    /// and when `num_slices > len` the surplus partitions are empty — the requested
    /// partition count is always honored (it drives task parallelism, e.g. Monte-Carlo
    /// jobs over a tiny input).
    fn slice<I>(data: I, num_slices: usize) -> Vec<Arc<Vec<T>>>
    where
        I: IntoIterator<Item = T>,
    {
        assert!(
            num_slices >= 1,
            "num_slices must be at least 1, got {num_slices}"
        );
        let mut data: Vec<T> = data.into_iter().collect();
        let len = data.len();
        let mut output = Vec::with_capacity(num_slices);
        let mut prev_end = 0;
        for i in 0..num_slices {
            let end = ((i + 1) * len) / num_slices;
            // Each slice is the contiguous front chunk `[prev_end, end)`; draining it moves
            // the elements out (no `Clone` bound) and leaves the next slice at the front.
            let chunk: Vec<T> = data.drain(0..end - prev_end).collect();
            output.push(Arc::new(chunk));
            prev_end = end;
        }
        output
    }
}

impl<T: Data + Clone> RddBase for ParallelCollection<T> {
    fn get_rdd_id(&self) -> usize {
        self.rdd_vals.vals.id
    }

    fn get_op_name(&self) -> String {
        self.name.lock().to_owned()
    }

    fn register_op_name(&self, name: &str) {
        let own_name = &mut *self.name.lock();
        *own_name = name.to_owned();
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.rdd_vals.vals.dependencies.clone()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        (0..self.rdd_vals.splits_.len())
            .map(|i| {
                Box::new(ParallelCollectionSplit::new(
                    self.rdd_vals.vals.id as i64,
                    i,
                    self.rdd_vals.splits_[i].clone(),
                )) as Box<dyn Split>
            })
            .collect::<Vec<Box<dyn Split>>>()
    }

    fn number_of_splits(&self) -> usize {
        self.rdd_vals.splits_.len()
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
        log::debug!("inside iterator_any parallel collection");
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }
}

impl<T: Data + Clone> Rdd for ParallelCollection<T> {
    type Item = T;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(ParallelCollection {
            name: Mutex::new(self.name.lock().clone()),
            rdd_vals: self.rdd_vals.clone(),
        })
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        if let Some(s) = split.as_any().downcast_ref::<ParallelCollectionSplit<T>>() {
            Ok(s.iterator())
        } else {
            panic!(
                "Got split object from different concrete type other than ParallelCollectionSplit"
            )
        }
    }
}
