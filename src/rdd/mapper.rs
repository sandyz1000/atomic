use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::{atomic::AtomicBool, atomic::Ordering::SeqCst, Arc};

use crate::context::{Context, RddContext};
use ember_data::dependency::{Dependency, OneToOneDependency};
use crate::error::Result;
use crate::rdd::rdd_val::RddVals;
use crate::rdd::{Rdd, RddBase};
use ember_data::split::Split;
use ember_data::data::Data;
use parking_lot::Mutex;


pub struct MapperRdd<T: Data, U: Data, F>
where
    F: Fn(T) -> U + Clone,
{
    name: Mutex<String>,
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: F,
    pinned: AtomicBool,
    _marker_t: PhantomData<T>, // phantom data is necessary because of type parameter T
}

// Can't derive clone automatically
impl<T: Data, U: Data, F> Clone for MapperRdd<T, U, F>
where
    F: Fn(T) -> U + Clone,
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
    F: Fn(T) -> U,
{
    pub(crate) fn new(prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        MapperRdd {
            name: Mutex::new("map".to_owned()),
            prev,
            vals,
            f,
            pinned: AtomicBool::new(false),
            _marker_t: PhantomData,
        }
    }

    pub(crate) fn pin(self) -> Self {
        self.pinned.store(true, SeqCst);
        self
    }
}

impl<T, U, F> RddContext for MapperRdd<T, U, F> {
    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }
}

impl<T: Data, U: Data, F> RddBase for MapperRdd<T, U, F>
where
    F: Fn(T) -> U,
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
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        self.iterator_any(split)
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        log::debug!("inside iterator_any maprdd",);
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x) ),
        ))
    }

    fn is_pinned(&self) -> bool {
        self.pinned.load(SeqCst)
    }
}

impl<T: Data, V: Data, U: Data, F> RddBase for MapperRdd<T, (V, U), F>
where
    F: Fn(T) -> (V, U),
{
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        log::debug!("inside iterator_any maprdd",);
        let val = Box::new(self.iterator(split)?.map(|(k, v)| Box::new((k, Box::new(v)))));
        Ok(val)
    }
    
    fn get_rdd_id(&self) -> usize {
        todo!()
    }
    
    fn get_dependencies(&self) -> Vec<Dependency> {
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

impl<T: Data, U: Data, F: 'static> Rdd for MapperRdd<T, U, F>
where
    F: Fn(T) -> U,
{
    type Item = U;
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        Ok(Box::new(self.prev.iterator(split)?.map(self.f.clone())))
    }
}
