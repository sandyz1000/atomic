use std::marker::PhantomData;
use std::sync::Arc;

use crate::rdd::core::RddCore;
use crate::rdd::{Rdd, RddBase};
use atomic_data::data::Data;
use atomic_data::dependency::Dependency;
use atomic_data::error::DataError;
use atomic_data::fn_traits::RddFlatMapFn;
use atomic_data::split::Split;
use std::net::Ipv4Addr;

pub struct FlatMapperRdd<T: Data, U: Data, F>
where
    F: RddFlatMapFn<T, U>,
{
    core: RddCore<T>,
    f: Arc<F>,
    _marker: PhantomData<U>,
}

impl<T: Data, U: Data, F> Clone for FlatMapperRdd<T, U, F>
where
    F: RddFlatMapFn<T, U>,
{
    fn clone(&self) -> Self {
        FlatMapperRdd {
            core: self.core.clone(),
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
        FlatMapperRdd {
            core: RddCore::new(id, prev, "flat_map"),
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
    fn is_pinned(&self) -> bool {
        self.core.is_pinned()
    }
    fn preferred_locations(&self, s: Box<dyn Split>) -> Vec<Ipv4Addr> {
        self.core.preferred_locations(s)
    }
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, DataError> {
        self.iterator_any(split)
    }
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, DataError> {
        log::debug!("inside iterator_any flatmaprdd");
        self.core.iterator_any(split, self)
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
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>, DataError> {
        let f = self.f.clone();
        Ok(Box::new(
            self.core.prev.iterator(split)?.flat_map(move |x| f(x)),
        ))
    }
}
