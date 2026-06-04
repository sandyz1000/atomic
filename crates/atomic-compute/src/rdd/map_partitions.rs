use parking_lot::Mutex;
use std::{
    marker::PhantomData,
    net::Ipv4Addr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::rdd::{Data, Rdd, rdd_val::RddVals};
use atomic_data::{
    dependency::Dependency, error::BaseError, fn_traits::RddPartitionFn, rdd::RddBase, split::Split,
};

/// An RDD that applies the provided function to every partition of the parent RDD.
pub struct MapPartitionsRdd<T: Data, U: Data, F>
where
    F: RddPartitionFn<T, U>,
{
    name: Mutex<String>,
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: Arc<F>,
    pinned: AtomicBool,
    _marker: PhantomData<(T, U)>,
}

impl<T: Data, U: Data, F> Clone for MapPartitionsRdd<T, U, F>
where
    F: RddPartitionFn<T, U>,
{
    fn clone(&self) -> Self {
        MapPartitionsRdd {
            name: Mutex::new(self.name.lock().clone()),
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            pinned: AtomicBool::new(self.pinned.load(Ordering::SeqCst)),
            _marker: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> MapPartitionsRdd<T, U, F>
where
    F: RddPartitionFn<T, U>,
{
    pub fn new(id: usize, prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        let mut vals = RddVals::new(id);
        vals.dependencies
            .push(Dependency::new_one_to_one(prev.get_rdd_base()));
        let vals = Arc::new(vals);
        MapPartitionsRdd {
            name: Mutex::new("map_partitions".to_owned()),
            prev,
            vals,
            f: Arc::new(f),
            pinned: AtomicBool::new(false),
            _marker: PhantomData,
        }
    }

    pub fn pin(self) -> Self {
        self.pinned.store(true, Ordering::SeqCst);
        self
    }
}

impl<T: Data, U: Data, F> RddBase for MapPartitionsRdd<T, U, F>
where
    F: RddPartitionFn<T, U>,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_op_name(&self) -> String {
        self.name.lock().to_owned()
    }

    fn register_op_name(&self, name: &str) {
        let own_name = &mut *self.name.lock();
        *own_name = name.to_owned();
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        self.prev.preferred_locations(split)
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
        log::debug!("inside iterator_any map_partitions_rdd",);
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }

    fn is_pinned(&self) -> bool {
        self.pinned.load(Ordering::SeqCst)
    }

}

/// An RDD that applies a function to every partition of the parent RDD, producing pair `(K, V)` items.
///
/// Unlike `MapPartitionsRdd`, the output type is a pair and the RDD correctly implements the
/// `cogroup_iterator_any` protocol (key and value boxed separately) so the result can participate
/// in `reduce_by_key`, `join`, and `cogroup` operations.
pub struct MapPartitionsPairRdd<T: Data, V: Data, K: Data, F>
where
    F: RddPartitionFn<T, (K, V)>,
{
    name: Mutex<String>,
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: Arc<F>,
    pinned: AtomicBool,
    _marker: PhantomData<(T, K, V)>,
}

impl<T: Data, V: Data, K: Data, F> Clone for MapPartitionsPairRdd<T, V, K, F>
where
    F: RddPartitionFn<T, (K, V)>,
{
    fn clone(&self) -> Self {
        MapPartitionsPairRdd {
            name: Mutex::new(self.name.lock().clone()),
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            pinned: AtomicBool::new(self.pinned.load(Ordering::SeqCst)),
            _marker: PhantomData,
        }
    }
}

impl<T: Data, V: Data, K: Data, F> MapPartitionsPairRdd<T, V, K, F>
where
    F: RddPartitionFn<T, (K, V)>,
{
    pub fn new(id: usize, prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        let mut vals = RddVals::new(id);
        vals.dependencies
            .push(Dependency::new_one_to_one(prev.get_rdd_base()));
        let vals = Arc::new(vals);
        MapPartitionsPairRdd {
            name: Mutex::new("map_partitions_pair".to_owned()),
            prev,
            vals,
            f: Arc::new(f),
            pinned: AtomicBool::new(false),
            _marker: PhantomData,
        }
    }
}

impl<T, V, K, F> RddBase for MapPartitionsPairRdd<T, V, K, F>
where
    F: RddPartitionFn<T, (K, V)>,
    T: Data,
    V: Data + Clone,
    K: Data + Clone,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_op_name(&self) -> String {
        self.name.lock().to_owned()
    }

    fn register_op_name(&self, name: &str) {
        *self.name.lock() = name.to_owned();
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        self.prev.preferred_locations(split)
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }

    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn Data>),
        ))
    }

    /// Key and value are boxed separately so `CoGroupedRdd` can downcast them individually.
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, Box::new(v))) as Box<dyn Data>),
        ))
    }

    fn is_pinned(&self) -> bool {
        self.pinned.load(Ordering::SeqCst)
    }
}

impl<T: Data, V: Data + Clone, K: Data + Clone, F: 'static> Rdd for MapPartitionsPairRdd<T, V, K, F>
where
    F: RddPartitionFn<T, (K, V)>,
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
        let index = split.get_index();
        Ok(Box::new(f(index, self.prev.iterator(split)?)))
    }
}

impl<T: Data, U: Data, F: 'static> Rdd for MapPartitionsRdd<T, U, F>
where
    F: RddPartitionFn<T, U>,
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
        let index = split.get_index();
        let f_result = f(index, self.prev.iterator(split)?);
        Ok(Box::new(f_result))
    }
}
