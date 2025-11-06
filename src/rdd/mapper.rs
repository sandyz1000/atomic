use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::{Arc, atomic::AtomicBool, atomic::Ordering::SeqCst};

use crate::context::{Context, RddContext};
use crate::rdd::rdd_val::RddVals;
use crate::rdd::{Rdd, RddBase};
use ember_data::data::Data;
use ember_data::dependency::Dependency;
use ember_data::error::BaseError;
use ember_data::fn_traits::RddFn;
use ember_data::split::Split;
use parking_lot::Mutex;

pub struct MapperRdd<T: Data, U: Data, F>
where
    F: RddFn<T, U>,
{
    name: Mutex<String>,
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: Arc<F>,
    pinned: AtomicBool,
    _marker_t: PhantomData<(T, U)>, // phantom data is necessary because of type parameter T
}

pub struct MapperPairRdd<T: Data, K: Data, V: Data, F>
where
    F: RddFn<T, (K, V)>,
{
    name: Mutex<String>,
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: Arc<F>,
    pinned: AtomicBool,
    _marker_t: PhantomData<(T, K, V)>,
}

// Can't derive clone automatically
impl<T: Data, U: Data, F> Clone for MapperRdd<T, U, F>
where
    F: RddFn<T, U>,
{
    fn clone(&self) -> Self {
        MapperRdd {
            name: Mutex::new(self.name.lock().clone()),
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            pinned: AtomicBool::new(self.pinned.load(SeqCst)),
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> MapperRdd<T, U, F>
where
    F: RddFn<T, U>,
{
    pub(crate) fn new(prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies.push(Dependency::OneToOne {
            rdd_base: prev.get_rdd_base(),
        });
        let vals = Arc::new(vals);
        MapperRdd {
            name: Mutex::new("map".to_owned()),
            prev,
            vals,
            f: Arc::new(f),
            pinned: AtomicBool::new(false),
            _marker_t: PhantomData,
        }
    }

    pub(crate) fn pin(self) -> Self {
        self.pinned.store(true, SeqCst);
        self
    }
}

impl<T, U, F> RddContext for MapperRdd<T, U, F>
where
    T: Data,
    U: Data,
    F: RddFn<T, U>,
{
    fn get_context(&self) -> Arc<Context> {
        self.vals.get_context()
    }
}

impl<T: Data, U: Data, F> RddBase for MapperRdd<T, U, F>
where
    F: RddFn<T, U>,
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
        log::debug!("inside iterator_any maprdd",);
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }

    fn is_pinned(&self) -> bool {
        self.pinned.load(SeqCst)
    }
}

impl<T: Data, U: Data, F: 'static> Rdd for MapperRdd<T, U, F>
where
    F: RddFn<T, U>,
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
        Ok(Box::new(self.prev.iterator(split)?.map(move |x| f(x))))
    }
}

// ============================================================================
// MapperPairRdd - Specialized implementation for tuple outputs (K, V)
// ============================================================================

impl<T: Data, K: Data, V: Data, F> Clone for MapperPairRdd<T, K, V, F>
where
    F: RddFn<T, (K, V)>,
{
    fn clone(&self) -> Self {
        MapperPairRdd {
            name: Mutex::new(self.name.lock().clone()),
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            pinned: AtomicBool::new(self.pinned.load(SeqCst)),
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, K: Data, V: Data, F> MapperPairRdd<T, K, V, F>
where
    F: RddFn<T, (K, V)>,
{
    pub(crate) fn new(prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies.push(Dependency::OneToOne {
            rdd_base: prev.get_rdd_base(),
        });
        let vals = Arc::new(vals);
        MapperPairRdd {
            name: Mutex::new("map".to_owned()),
            prev,
            vals,
            f: Arc::new(f),
            pinned: AtomicBool::new(false),
            _marker_t: PhantomData,
        }
    }

    pub(crate) fn pin(self) -> Self {
        self.pinned.store(true, SeqCst);
        self
    }
}

impl<T, K, V, F> RddContext for MapperPairRdd<T, K, V, F>
where
    T: Data,
    K: Data,
    V: Data,
    F: RddFn<T, (K, V)>,
{
    fn get_context(&self) -> Arc<Context> {
        self.vals.get_context()
    }
}

impl<T: Data, K: Data, V: Data, F> RddBase for MapperPairRdd<T, K, V, F>
where
    F: RddFn<T, (K, V)>,
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
        log::debug!("inside cogroup_iterator_any mapperpairrdd");
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn Data>)) as Box<dyn Data>
        })))
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        log::debug!("inside iterator_any mapperpairrdd");
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }

    fn is_pinned(&self) -> bool {
        self.pinned.load(SeqCst)
    }
}

impl<T: Data, K: Data, V: Data, F: 'static> Rdd for MapperPairRdd<T, K, V, F>
where
    F: RddFn<T, (K, V)>,
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
        Ok(Box::new(self.prev.iterator(split)?.map(move |x| f(x))))
    }
}
