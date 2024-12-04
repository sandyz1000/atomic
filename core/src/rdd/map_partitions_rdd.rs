use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::{atomic::AtomicBool, atomic::Ordering::SeqCst, Arc};
use crate::context::Context;
use crate::dependency::{Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependencyTrait};
use crate::error::Result;
use crate::rdd::rdd::{Rdd, RddBase, RddVals};
use crate::ser_data::{AnyData, Data, SerFunc};
// use crate::serializable_traits::SerFunc;
use crate::split::Split;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// An RDD that applies the provided function to every partition of the parent RDD.
#[derive(Serialize, Deserialize)]
pub struct MapPartitionsRdd<T: Data, U: Data, F, RDD, ND, SD> {
    #[serde(skip_serializing, skip_deserializing)]
    name: Mutex<String>,
    prev: Arc<RDD>,
    vals: Arc<RddVals<ND, SD>>,
    f: F,
    pinned: AtomicBool,
    _marker_u: PhantomData<U>,
    _marker_t: PhantomData<T>,
}

impl<T: Data, U: Data, F, RDD, ND, SD> Clone for MapPartitionsRdd<T, U, F, RDD, ND, SD>
where
    RDD: Rdd<Item = T>,
    F: Fn(usize, Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn clone(&self) -> Self {
        MapPartitionsRdd {
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

impl<T: Data, U: Data, F, RDD, ND, SD> MapPartitionsRdd<T, U, F, RDD, ND, SD>
where
    RDD: Rdd<Item = T>,
    F: SerFunc<(usize, Box<dyn Iterator<Item = T>>), Output = Box<dyn Iterator<Item = U>>>,
{
    pub(crate) fn new(prev: Arc<RDD>, f: F) -> Self {
        let mut vals: RddVals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        MapPartitionsRdd {
            name: Mutex::new("map_partitions".to_owned()),
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

impl<T: Data, U: Data, F, RDD, N, S> RddBase for MapPartitionsRdd<T, U, F, RDD, N, S>
where
    RDD: Rdd<Item = T>,
    F: SerFunc<(usize, Box<dyn Iterator<Item = T>>), Output = Box<dyn Iterator<Item = U>>>,
    N: NarrowDependencyTrait + 'static,
    S: ShuffleDependencyTrait + 'static
{
    type Split = RDD::Split;
    type Partitioner = RDD::Partitioner;
    type ShuffleDeps = S;
    type NarrowDeps = N;

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

    fn get_dependencies(&self) -> Vec<Dependency<N, S>> {
        self.vals.dependencies.clone()
    }

    fn preferred_locations(&self, split: Box<Self::Split>) -> Vec<Ipv4Addr> {
        self.prev.preferred_locations(split)
    }

    fn splits(&self) -> Vec<Box<Self::Split>> {
        self.prev.splits()
    }

    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    fn cogroup_iterator_any(
        &self,
        split: Box<Self::Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        self.iterator_any(split)
    }

    fn iterator_any(
        &self,
        split: Box<Self::Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        log::debug!("inside iterator_any map_partitions_rdd",);
        Ok(Box::new(
            self.iterator(split)?.map(|x| Box::new(x)),
        ))
    }

    fn is_pinned(&self) -> bool {
        self.pinned.load(SeqCst)
    }
    
}

// impl<T: Data, V: Data, U: Data, F> RddBase for MapPartitionsRdd<T, (V, U), F>
// where
//     F: SerFunc(usize, Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = (V, U)>>,
// {
//     fn cogroup_iterator_any(
//         &self,
//         split: Box<Self::Split>,
//     ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
//         log::debug!("inside iterator_any map_partitions_rdd",);
//         Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
//             Box::new((k, Box::new(v)))
//         })))
//     }
// }

impl<T: Data, U: Data, F, RDD, ND, SD> Rdd for MapPartitionsRdd<T, U, F, RDD, ND, SD>
where
    F: SerFunc<(usize, Box<dyn Iterator<Item = T>>), Output = Box<dyn Iterator<Item = U>>> + 'static,
    RDD: Rdd<Item = T>,
    ND: NarrowDependencyTrait + 'static,
    SD: ShuffleDependencyTrait + 'static
{
    type Item = U;
    type RddBase = RDD;

    fn get_rdd_base(&self) -> Arc<RDD> {
        Arc::new(self.clone())
    }

    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    
    fn compute(&self, split: Box<Self::Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let f_result = self.f.clone()(split.get_index(), self.prev.iterator(split)?);
        Ok(Box::new(f_result))
    }
    
}
