use std::{
    marker::PhantomData,
    net::Ipv4Addr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use ember_data::{dependency::Dependency, error::BaseError, fn_traits::RddPartitionFn, rdd::RddBase, split::Split};

use crate::{
    context::RddContext,
    rdd::{Context, Data, Rdd, rdd_val::RddVals},
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
    _marker_t: PhantomData<T>,
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
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> MapPartitionsRdd<T, U, F>
where
    F: RddPartitionFn<T, U>,
{
    pub(crate) fn new(prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::new_one_to_one(prev.get_rdd_base()));
        let vals = Arc::new(vals);
        MapPartitionsRdd {
            name: Mutex::new("map_partitions".to_owned()),
            prev,
            vals,
            f: Arc::new(f),
            pinned: AtomicBool::new(false),
            _marker_t: PhantomData,
        }
    }

    pub(crate) fn pin(self) -> Self {
        self.pinned.store(true, Ordering::SeqCst);
        self
    }
}

impl<T, U, F> RddContext for MapPartitionsRdd<T, U, F> {
    fn get_context(&self) -> Arc<Context> {
        self.vals.get_context()
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
        Ok(Box::new(self.iterator(split)?.map(|x| Box::new(x))))
    }

    fn is_pinned(&self) -> bool {
        self.pinned.load(Ordering::SeqCst)
    }
}

pub struct MapPartitionsPairRdd<T: Data, V: Data, U: Data, F: Data>
where
    F: Fn(usize, Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = (V, U)>> + Clone,
{
    name: Mutex<String>,
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: F,
    pinned: AtomicBool,
    _marker_t: PhantomData<T>,
}

impl<T: Data, V: Data, U: Data, F> RddBase for MapPartitionsPairRdd<T, V, U, F>
where
    F: Fn(usize, Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = (V, U)>>,
{
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        log::debug!("inside iterator_any map_partitions_rdd",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn Data>)) as Box<dyn Data>
        })))
    }

    fn get_rdd_id(&self) -> usize {
        todo!()
    }

    fn get_dependencies(&self) -> Vec<ember_data::dependency::Dependency> {
        todo!()
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        todo!()
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
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
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>, BaseError> {
        let f = self.f.clone();
        let index = split.get_index();
        let f_result = f(index, self.prev.iterator(split)?);
        Ok(Box::new(f_result))
    }
}
