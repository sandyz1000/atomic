use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::context::Context;
use crate::dependency::{Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependencyTrait};
use crate::error::Result;
use crate::rdd::rdd::{Rdd, RddBase, RddVals};
use crate::ser_data::{AnyData, Data};
use crate::ser_data::SerFunc;
use crate::split::Split;

#[derive(Serialize, Deserialize)]
pub struct FlatMapperRdd<T: Data, U: Data, F, RDD, ND, SD> {
    prev: Arc<RDD>,
    vals: Arc<RddVals<ND, SD>>,
    f: F,
    _marker_t: PhantomData<T>, // phantom data is necessary because of type parameter T
    _marker_u: PhantomData<U>
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

impl<T: Data, U: Data, F, RDD, ND, SD> FlatMapperRdd<T, U, F, RDD, ND, SD>
where
    RDD: Rdd<Item = T>,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait,
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

impl<T: Data, U: Data, F, RDD, ND, SD> RddBase<ND, SD> for FlatMapperRdd<T, U, F, RDD, ND, SD>
where
    RDD: Rdd<Item = T>,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait,
    F: SerFunc<T, Output = Box<dyn Iterator<Item = U>>>,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency<ND, SD>> {
        self.vals.dependencies.clone()
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
    ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        self.iterator_any(split)
    }

    fn iterator_any<S: Split + ?Sized>(
        &self,
        split: Box<S>,
    ) -> Result<Box<dyn Iterator<Item = Box<impl AnyData>>>> {
        log::debug!("inside iterator_any flatmaprdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        ))
    }
}

// impl<T: Data, V: Data, U: Data, F: 'static> RddBase for FlatMapperRdd<T, (V, U), F>
// where
//     F: SerFunc(T) -> Box<dyn Iterator<Item = (V, U)>>,
// {
//     fn cogroup_iterator_any(
//         &self,
//         split: Box<dyn Split>,
//     ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
//         log::debug!("inside iterator_any flatmaprdd",);
//         Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
//             Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
//         })))
//     }
// }

impl<T: Data, U: Data, F, RDD, ND, SD> Rdd for FlatMapperRdd<T, U, F, RDD, ND, SD>
where
    F: SerFunc<T, Output = Box<dyn Iterator<Item = U>>> + 'static,
    RDD: Rdd<Item = T>,
    ND: NarrowDependencyTrait,
    SD: ShuffleDependencyTrait,
{
    type Item = U;
    fn get_rdd_base<RDB: RddBase>(&self) -> Arc<RDB> {
        Arc::new(self.clone())
    }

    fn get_rdd(&self) -> Arc<impl Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute<S: Split + ?Sized>(&self, split: Box<S>) -> Result<Box<impl Iterator<Item = Self::Item>>> {
        let f = self.f.clone();
        Ok(Box::new(self.prev.iterator(split)?.flat_map(f)))
    }
}
