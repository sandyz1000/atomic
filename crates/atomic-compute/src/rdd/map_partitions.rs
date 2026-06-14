use std::{marker::PhantomData, net::Ipv4Addr, sync::Arc};

use crate::rdd::{Data, Rdd, core::RddCore};
use atomic_data::{
    dependency::Dependency, error::BaseError, fn_traits::RddPartitionFn, rdd::RddBase, split::Split,
};

/// An RDD that applies the provided function to every partition of the parent RDD.
pub struct MapPartitionsRdd<T: Data, U: Data, F>
where
    F: RddPartitionFn<T, U>,
{
    core: RddCore<T>,
    f: Arc<F>,
    _marker: PhantomData<U>,
}

impl<T: Data, U: Data, F> Clone for MapPartitionsRdd<T, U, F>
where
    F: RddPartitionFn<T, U>,
{
    fn clone(&self) -> Self {
        MapPartitionsRdd {
            core: self.core.clone(),
            f: self.f.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> MapPartitionsRdd<T, U, F>
where
    F: RddPartitionFn<T, U>,
{
    pub fn new(id: usize, prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        MapPartitionsRdd {
            core: RddCore::new(id, prev, "map_partitions"),
            f: Arc::new(f),
            _marker: PhantomData,
        }
    }

    pub fn pin(self) -> Self {
        self.core
            .pinned
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self
    }
}

impl<T: Data, U: Data, F> RddBase for MapPartitionsRdd<T, U, F>
where
    F: RddPartitionFn<T, U>,
{
    fn get_rdd_id(&self) -> usize {
        self.core.rdd_id()
    }
    fn get_op_name(&self) -> String {
        self.core.op_name()
    }
    fn register_op_name(&self, name: &str) {
        self.core.set_op_name(name)
    }
    fn get_dependencies(&self) -> Vec<Dependency> {
        self.core.dependencies()
    }
    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.core.splits()
    }
    fn number_of_splits(&self) -> usize {
        self.core.number_of_splits()
    }
    fn preferred_locations(&self, s: Box<dyn Split>) -> Vec<Ipv4Addr> {
        self.core.preferred_locations(s)
    }
    fn is_pinned(&self) -> bool {
        self.core.is_pinned()
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
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }
}

impl<T: Data, U: Data, F: 'static> Rdd for MapPartitionsRdd<T, U, F>
where
    F: RddPartitionFn<T, U>,
{
    type Item = U;

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone())
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
        Ok(Box::new(f(index, self.core.prev.iterator(split)?)))
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
    core: RddCore<T>,
    f: Arc<F>,
    _marker: PhantomData<(K, V)>,
}

impl<T: Data, V: Data, K: Data, F> Clone for MapPartitionsPairRdd<T, V, K, F>
where
    F: RddPartitionFn<T, (K, V)>,
{
    fn clone(&self) -> Self {
        MapPartitionsPairRdd {
            core: self.core.clone(),
            f: self.f.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: Data, V: Data, K: Data, F> MapPartitionsPairRdd<T, V, K, F>
where
    F: RddPartitionFn<T, (K, V)>,
{
    pub fn new(id: usize, prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        MapPartitionsPairRdd {
            core: RddCore::new(id, prev, "map_partitions_pair"),
            f: Arc::new(f),
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
        self.core.rdd_id()
    }
    fn get_op_name(&self) -> String {
        self.core.op_name()
    }
    fn register_op_name(&self, name: &str) {
        self.core.set_op_name(name)
    }
    fn get_dependencies(&self) -> Vec<Dependency> {
        self.core.dependencies()
    }
    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.core.splits()
    }
    fn number_of_splits(&self) -> usize {
        self.core.number_of_splits()
    }
    fn preferred_locations(&self, s: Box<dyn Split>) -> Vec<Ipv4Addr> {
        self.core.preferred_locations(s)
    }
    fn is_pinned(&self) -> bool {
        self.core.is_pinned()
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
}

impl<T: Data, V: Data + Clone, K: Data + Clone, F: 'static> Rdd for MapPartitionsPairRdd<T, V, K, F>
where
    F: RddPartitionFn<T, (K, V)>,
{
    type Item = (K, V);

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone())
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
        Ok(Box::new(f(index, self.core.prev.iterator(split)?)))
    }
}
