use std::marker::PhantomData;
use std::sync::Arc;

use crate::context::{Context, RddContext};
use crate::rdd::rdd_val::RddVals;
use ember_data::data::Data;
use ember_data::dependency::Dependency;
use crate::error::Result;
use crate::rdd::{Rdd, RddBase};
use ember_data::split::Split;

pub struct FlatMapperRdd<T: Data, U: Data, F>
where
    F: Fn(T) -> Box<dyn Iterator<Item = U>> + Clone,
{
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: F,
    _marker_t: PhantomData<T>, // phantom data is necessary because of type parameter T
}

impl<T: Data, U: Data, F> Clone for FlatMapperRdd<T, U, F>
where
    F: Fn(T) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn clone(&self) -> Self {
        FlatMapperRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> FlatMapperRdd<T, U, F>
where
    F: Fn(T) -> Box<dyn Iterator<Item = U>>,
{
    pub(crate) fn new(prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies.push(Dependency::OneToOne { rdd_base: prev.get_rdd_base() });
        let vals = Arc::new(vals);
        FlatMapperRdd {
            prev,
            vals,
            f,
            _marker_t: PhantomData,
        }
    }
}

impl<T, U, F> RddContext for FlatMapperRdd<T, U, F> {
    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }
}

impl<T: Data, U: Data, F> RddBase for FlatMapperRdd<T, U, F>
where
    F: Fn(T) -> Box<dyn Iterator<Item = U>>,
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
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        self.iterator_any(split)
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        log::debug!("inside iterator_any flatmaprdd",);
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x)),
        ))
    }
}

impl<T: Data, V: Data, U: Data, F: 'static> RddBase for FlatMapperRdd<T, (V, U), F>
where
    F: Fn(T) -> Box<dyn Iterator<Item = (V, U)>>,
{
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        log::debug!("inside iterator_any flatmaprdd",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn Data>))
        })))
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
    ) -> BaseResult<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        todo!()
    }
}

impl<T: Data, U: Data, F: 'static> Rdd for FlatMapperRdd<T, U, F>
where
    F: Fn(T) -> Box<dyn Iterator<Item = U>>,
{
    type Item = U;
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let f = self.f.clone();
        Ok(Box::new(self.prev.iterator(split)?.flat_map(f)))
    }
}
