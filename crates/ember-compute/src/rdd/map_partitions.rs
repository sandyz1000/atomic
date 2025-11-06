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
use ember_data::{
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
    pub(crate) fn new(id: usize, prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
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

    pub(crate) fn pin(self) -> Self {
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

pub struct MapPartitionsPairRdd<T: Data, V: Data, U: Data, F>
where
    F: RddPartitionFn<T, (U, V)>,
{
    name: Mutex<String>,
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: F,
    pinned: AtomicBool,
    _marker_t: PhantomData<(T, U, V)>,
}

impl<T, V, U, F> RddBase for MapPartitionsPairRdd<T, V, U, F>
where
    F: RddPartitionFn<T, (U, V)>,
    T: Data,
    V: Data,
    U: Data + Clone,
{
    fn cogroup_iterator_any(
        &self,
        _split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside iterator_any map_partitions_rdd",);

        // Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
        //     Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        // })))
        // TODO: MapPartitionsPairRdd is incomplete - implement when needed
        todo!("MapPartitionsPairRdd::cogroup_iterator_any not yet implemented")
    }

    fn get_rdd_id(&self) -> usize {
        todo!("MapPartitionsPairRdd::get_rdd_id not yet implemented")
    }

    fn get_dependencies(&self) -> Vec<ember_data::dependency::Dependency> {
        todo!("MapPartitionsPairRdd::get_dependencies not yet implemented")
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        todo!("MapPartitionsPairRdd::splits not yet implemented")
    }

    fn iterator_any(
        &self,
        _split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        todo!("MapPartitionsPairRdd::iterator_any not yet implemented")
    }
}

// TODO: Implement Rdd trait for MapPartitionsPairRdd when it's actually needed
impl<T: Data, V: Data + Clone, U: Data + Clone, F> Rdd for MapPartitionsPairRdd<T, V, U, F>
where
    F: RddPartitionFn<T, (U, V)>,
{
    type Item = (U, V);

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        todo!()
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        todo!()
    }

    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        todo!()
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
