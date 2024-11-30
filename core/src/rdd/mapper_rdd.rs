use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::{atomic::AtomicBool, atomic::Ordering::SeqCst, Arc};

use crate::context::Context;
use crate::dependency::{Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependencyTrait};
use crate::error::Result;
use crate::rdd::rdd::{Rdd, RddBase, RddVals};
use crate::ser_data::{AnyData, Data, SerFunc};
use crate::split::Split;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct MapperRdd<T: Data, U: Data, F, RDD, ND, SD> {
    #[serde(skip_serializing, skip_deserializing)]
    name: Mutex<String>,
    prev: Arc<RDD>,
    vals: Arc<RddVals<ND, SD>>,
    f: F,
    pinned: AtomicBool,
    _marker_t: PhantomData<T>, // phantom data is necessary because of type parameter T
    _marker_u: PhantomData<U>
}

// Can't derive clone automatically
impl<T: Data, U: Data, F, RDD, ND, SD> Clone for MapperRdd<T, U, F, RDD, ND, SD>
where
    F: Fn(T) -> U + Clone,
    RDD: Rdd<Item = T>,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait
{
    fn clone(&self) -> Self {
        MapperRdd {
            name: Mutex::new(self.name.lock().clone()),
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            pinned: AtomicBool::new(self.pinned.load(SeqCst)),
            _marker_t: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F, RDD, ND, SD> MapperRdd<T, U, F, RDD, ND, SD>
where
    F: SerFunc<T, Output = U>,
    RDD: Rdd<Item = T>,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait
{
    /// TODO: Add Docs
    /// 
    pub(crate) fn new(prev: Arc<RDD>, f: F) -> Self {
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
            _marker_u: PhantomData,
        }
    }

    pub(crate) fn pin(self) -> Self {
        self.pinned.store(true, SeqCst);
        self
    }
}

impl<T: Data, U: Data, F, RDD, ND, SD> RddBase for MapperRdd<T, U, F, RDD, ND, SD>
where
    F: SerFunc<T, Output = U>,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_op_name(&self) -> String {
        self.name.lock().to_owned()
    }

    fn register_op_name(&self, name: &str) {
        let own_name = &mut *self.name.lock();
        *own_name = name.to_owned();
    }

    fn get_dependencies(&self) -> Vec<Dependency<ND, SD>> {
        self.vals.dependencies.clone()
    }

    fn preferred_locations<S: Split + ?Sized>(&self, split: Box<S>) -> Vec<Ipv4Addr> {
        self.prev.preferred_locations(split)
    }

    fn splits<S: Split + ?Sized>(&self) -> Vec<Box<S>> {
        self.prev.splits()
    }

    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    fn cogroup_iterator_any<S: Split + ?Sized>(
        &self,
        split: Box<S>,
    ) -> Result<Box<impl Iterator<Item = Box<impl AnyData>>>> {
        self.iterator_any(split)
    }

    fn iterator_any<S: Split + ?Sized>(
        &self,
        split: Box<S>,
    ) -> Result<Box<impl Iterator<Item = Box<impl AnyData>>>> {
        log::debug!("inside iterator_any maprdd",);
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x)),
        ))
    }

    fn is_pinned(&self) -> bool {
        self.pinned.load(SeqCst)
    }
}

// impl<T: Data, V: Data, U: Data, F> RddBase for MapperRdd<T, (V, U), F>
// where
//     F: SerFunc(T) -> (V, U),
// {
//     fn cogroup_iterator_any(
//         &self,
//         split: Box<dyn Split>,
//     ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
//         log::debug!("inside iterator_any maprdd",);
//         Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
//             Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
//         })))
//     }
// }

impl<T: Data, U: Data, F, RDD, ND, SD> Rdd for MapperRdd<T, U, F, RDD, ND, SD>
where
    F: SerFunc<T, Output = U> + 'static,
{
    type Item = U;
    fn get_rdd_base(&self) -> Arc<impl RddBase> {
        Arc::new(self.clone())
    }

    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute<S: Split + ?Sized>(&self, split: Box<S>) -> Result<Box<impl Iterator<Item = Self::Item>>> {
        Ok(Box::new(self.prev.iterator(split)?.map(self.f.clone())))
    }
}
