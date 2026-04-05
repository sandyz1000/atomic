use std::marker::PhantomData;
use std::sync::Arc;

use crate::rdd::rdd_val::RddVals;
use crate::rdd::{Rdd, RddBase};
use atomic_data::data::Data;
use atomic_data::dependency::Dependency;
use atomic_data::error::BaseError;
use atomic_data::fn_traits::RddFlatMapFn;
use atomic_data::split::Split;

pub struct FlatMapperRdd<T: Data, U: Data, F>
where
    F: RddFlatMapFn<T, U>,
{
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: Arc<F>,
    _marker: PhantomData<(T, U)>,
}

pub struct FlatMapperPairRdd<T: Data, K: Data + Clone, V: Data + Clone, F>
where
    F: RddFlatMapFn<T, (K, V)>,
{
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: Arc<F>,
    _marker: PhantomData<(T, K, V)>,
}

impl<T: Data, U: Data, F> Clone for FlatMapperRdd<T, U, F>
where
    F: RddFlatMapFn<T, U>,
{
    fn clone(&self) -> Self {
        FlatMapperRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> FlatMapperRdd<T, U, F>
where
    F: RddFlatMapFn<T, U>,
{
    pub fn new(id: usize, prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        let mut vals = RddVals::new(id);
        vals.dependencies.push(Dependency::OneToOne {
            rdd_base: prev.get_rdd_base(),
        });
        let vals = Arc::new(vals);
        FlatMapperRdd {
            prev,
            vals,
            f: Arc::new(f),
            _marker: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> RddBase for FlatMapperRdd<T, U, F>
where
    F: RddFlatMapFn<T, U>,
{
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
        log::debug!("inside iterator_any flatmaprdd",);
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }
}

impl<T: Data, U: Data, F: 'static> Rdd for FlatMapperRdd<T, U, F>
where
    F: RddFlatMapFn<T, U>,
{
    type Item = U;
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        let f = self.f.clone();
        Ok(Box::new(self.prev.iterator(split)?.flat_map(move |x| f(x))))
    }
}

// ============================================================================
// FlatMapperPairRdd - Specialized implementation for tuple outputs (K, V)
// ============================================================================

impl<T: Data, K: Data + Clone, V: Data + Clone, F> Clone for FlatMapperPairRdd<T, K, V, F>
where
    F: RddFlatMapFn<T, (K, V)>,
{
    fn clone(&self) -> Self {
        FlatMapperPairRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: Data, K: Data + Clone, V: Data + Clone, F> FlatMapperPairRdd<T, K, V, F>
where
    F: RddFlatMapFn<T, (K, V)>,
{
    pub fn new(id: usize, prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        let mut vals = RddVals::new(id);
        vals.dependencies.push(Dependency::OneToOne {
            rdd_base: prev.get_rdd_base(),
        });
        let vals = Arc::new(vals);
        FlatMapperPairRdd {
            prev,
            vals,
            f: Arc::new(f),
            _marker: PhantomData,
        }
    }
}

impl<T: Data, K: Data + Clone, V: Data + Clone, F> RddBase for FlatMapperPairRdd<T, K, V, F>
where
    F: RddFlatMapFn<T, (K, V)>,
{
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

    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside cogroup_iterator_any flatmapperpairrdd");
        Ok(Box::new(
            self.iterator(split)?
                .map(|pair| Box::new(pair) as Box<dyn Data>),
        ))
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside iterator_any flatmapperpairrdd");
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }
}

impl<T: Data, K: Data + Clone, V: Data + Clone, F: 'static> Rdd for FlatMapperPairRdd<T, K, V, F>
where
    F: RddFlatMapFn<T, (K, V)>,
{
    type Item = (K, V);

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        let f = self.f.clone();
        Ok(Box::new(self.prev.iterator(split)?.flat_map(move |x| f(x))))
    }
}
