use crate::rdd::rdd_val::RddVals;
use crate::rdd::*;
use atomic_data::dependency::Dependency;
use atomic_data::error::BaseError;
use atomic_data::fn_traits::{RddFlatMapFn, RddFn};
use atomic_data::split::Split;
use std::marker::PhantomData;
use std::sync::Arc;

// Implementing the PairRdd trait for all types which implements Rdd
// impl<K: Data + Eq + Hash, V: Data, T> PairRdd<K, V> for Arc<T> where T: Rdd<Item = (K, V)> {}

pub struct MappedValuesRdd<K: Data, V: Data, U: Data, F>
where
    F: RddFn<V, U>,
{
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
    f: Arc<F>,
    _marker: PhantomData<(K, V, U)>,
}

impl<K: Data, V: Data, U: Data, F> Clone for MappedValuesRdd<K, V, U, F>
where
    F: RddFn<V, U>,
{
    fn clone(&self) -> Self {
        MappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Clone, V: Data + Clone, U: Data, F> MappedValuesRdd<K, V, U, F>
where
    F: RddFn<V, U>,
{
    pub fn new(id: usize, prev: Arc<dyn Rdd<Item = (K, V)>>, f: F) -> Self {
        let mut vals = RddVals::new(id);
        vals.dependencies
            .push(Dependency::new_one_to_one(prev.get_rdd_base()));
        let vals = Arc::new(vals);
        MappedValuesRdd {
            prev,
            vals,
            f: Arc::new(f),
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Clone, V: Data + Clone, U: Data + Clone, F> RddBase for MappedValuesRdd<K, V, U, F>
where
    F: RddFn<V, U>,
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
    // TODO: Analyze the possible error in invariance here
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside iterator_any mapvaluesrdd");
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn Data>),
        ))
    }
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside cogroup_iterator_any mapvaluesrdd");
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, Box::new(v))) as Box<dyn Data>),
        ))
    }
}

impl<K: Data + Clone, V: Data + Clone, U: Data + Clone, F: 'static> Rdd
    for MappedValuesRdd<K, V, U, F>
where
    F: RddFn<V, U>,
{
    type Item = (K, U);
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
        Ok(Box::new(
            self.prev.iterator(split)?.map(move |(k, v)| (k, f(v))),
        ))
    }
}

pub struct FlatMappedValuesRdd<K: Data, V: Data, U: Data, F>
where
    F: RddFlatMapFn<V, U>,
{
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
    f: Arc<F>,
    _marker: PhantomData<(K, V, U)>,
}

impl<K: Data, V: Data, U: Data, F> Clone for FlatMappedValuesRdd<K, V, U, F>
where
    F: RddFlatMapFn<V, U>,
{
    fn clone(&self) -> Self {
        FlatMappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Clone, V: Data + Clone, U: Data, F> FlatMappedValuesRdd<K, V, U, F>
where
    F: RddFlatMapFn<V, U>,
{
    pub fn new(id: usize, prev: Arc<dyn Rdd<Item = (K, V)>>, f: F) -> Self {
        let mut vals = RddVals::new(id);
        vals.dependencies
            .push(Dependency::new_one_to_one(prev.get_rdd_base()));
        let vals = Arc::new(vals);
        FlatMappedValuesRdd {
            prev,
            vals,
            f: Arc::new(f),
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Clone, V: Data + Clone, U: Data + Clone, F> RddBase
    for FlatMappedValuesRdd<K, V, U, F>
where
    F: RddFlatMapFn<V, U>,
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
    // TODO: Analyze the possible error in invariance here
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn Data>),
        ))
    }
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, Box::new(v))) as Box<dyn Data>),
        ))
    }
}

impl<K: Data + Clone, V: Data + Clone, U: Data + Clone, F> Rdd for FlatMappedValuesRdd<K, V, U, F>
where
    F: RddFlatMapFn<V, U>,
{
    type Item = (K, U);

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
        Ok(Box::new(
            self.prev
                .iterator(split)?
                .flat_map(move |(k, v)| f(v).map(move |x| (k.clone(), x))),
        ))
    }
}
