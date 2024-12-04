use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::context::Context;
use crate::dependency::{
    Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependencyTrait,
};
use crate::error::Result;
use crate::rdd::rdd::{Rdd, RddBase, RddVals};
use crate::ser_data::SerFunc;
use crate::ser_data::{AnyData, Data};

#[derive(Serialize, Deserialize)]
pub struct FlatMapperRdd<T: Data, U: Data, F, RDD, ND, SD> {
    prev: Arc<RDD>,
    vals: Arc<RddVals<ND, SD>>,
    f: F,
    _marker_t: PhantomData<T>, // phantom data is necessary because of type parameter T
    _marker_u: PhantomData<U>,
}

impl<T: Data, U: Data, F, RDD, ND, SD> Clone for FlatMapperRdd<T, U, F, RDD, ND, SD>
where
    RDD: Rdd<Item = T>,
    F: Fn(T) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn clone(&self) -> Self {
        FlatMapperRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            _marker_t: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F, RDD, N, S> FlatMapperRdd<T, U, F, RDD, N, S>
where
    RDD: Rdd<Item = T>,
    N: NarrowDependencyTrait,
    S: ShuffleDependencyTrait,
    F: SerFunc<T, Output = Box<dyn Iterator<Item = U>>>,
{
    pub(crate) fn new(prev: Arc<RDD>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        FlatMapperRdd {
            prev,
            vals,
            f,
            _marker_t: PhantomData,
            _marker_u: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F, RDD, N, S> RddBase for FlatMapperRdd<T, U, F, RDD, N, S>
where
    RDD: Rdd<Item = T>,
    N: NarrowDependencyTrait + 'static,
    S: ShuffleDependencyTrait + 'static,
    F: SerFunc<T, Output = Box<dyn Iterator<Item = U>>>,
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

    fn get_dependencies(&self) -> Vec<Dependency<N, S>> {
        self.vals.dependencies.clone()
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
        log::debug!("inside iterator_any flatmaprdd",);
        Ok(Box::new(self.iterator(split)?.map(|x| Box::new(x))))
    }
}

// impl<T: Data, V: Data, U: Data, F: 'static> RddBase for FlatMapperRdd<T, (V, U), F>
// where
//     F: SerFunc(T) -> Box<dyn Iterator<Item = (V, U)>>,
// {
//     fn cogroup_iterator_any(
//         &self,
//         split: Box<Self::Split>,
//     ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
//         log::debug!("inside iterator_any flatmaprdd",);
//         Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
//             Box::new((k, Box::new(v)))
//         })))
//     }
// }

impl<T: Data, U: Data, F, RDD, N, S> Rdd for FlatMapperRdd<T, U, F, RDD, N, S>
where
    F: SerFunc<T, Output = Box<dyn Iterator<Item = U>>> + 'static,
    RDD: Rdd<Item = T>,
    N: NarrowDependencyTrait + 'static,
    S: ShuffleDependencyTrait + 'static,
{
    type Item = U;
    type RddBase = RDD;

    fn get_rdd_base(&self) -> Arc<Self::RddBase> {
        Arc::new(self.clone())
    }

    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<Self::Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let f = self.f.clone();
        Ok(Box::new(self.prev.iterator(split)?.flat_map(f)))
    }
}
