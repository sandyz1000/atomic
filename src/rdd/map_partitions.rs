use std::{marker::PhantomData, sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}};

use crate::rdd::{Data, Rdd};



/// An RDD that applies the provided function to every partition of the parent RDD.
pub struct MapPartitionsRdd<T: Data, U: Data, F>
where
    F: Fn(usize, Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = U>> + Clone,
{
    name: Mutex<String>,
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: F,
    pinned: AtomicBool,
    _marker_t: PhantomData<T>,
}

impl<T: Data, U: Data, F> Clone for MapPartitionsRdd<T, U, F>
where
    F: Fn(usize, Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = U>> + Clone,
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
