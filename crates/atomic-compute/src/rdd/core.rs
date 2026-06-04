//! Shared fields and delegation helpers for narrow-dependency RDDs.
//!
//! Embed [`RddCore<T>`] in any RDD struct whose [`RddBase`] implementation is a
//! straight pass-through to a single parent RDD.  Call `self.core.xyz()` inside
//! the `RddBase` impl — no duplication, no macros, easy to step through in a debugger.
//!
//! # Usage
//!
//! ```ignore
//! pub struct MyRdd<T, U, F> {
//!     core: RddCore<T>,
//!     f: Arc<F>,
//!     _out: PhantomData<U>,
//! }
//!
//! impl<T, U, F> RddBase for MyRdd<T, U, F> {
//!     fn get_rdd_id(&self) -> usize { self.core.rdd_id() }
//!     fn splits(&self) -> Vec<Box<dyn Split>> { self.core.splits() }
//!     // … one line per method, logic lives in RddCore
//! }
//! ```

use parking_lot::Mutex;
use std::{
    net::Ipv4Addr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::rdd::{Data, Rdd, rdd_val::RddVals};
use atomic_data::{
    dependency::Dependency,
    error::BaseError,
    split::Split,
};

/// Common fields for narrow-dependency RDDs: parent pointer, RDD metadata,
/// op name, and pinned flag.
///
/// All `RddBase` delegation methods live here so each RDD only needs a one-liner
/// in its own `impl RddBase`.
pub(crate) struct RddCore<T: Data> {
    pub vals: Arc<RddVals>,
    pub prev: Arc<dyn Rdd<Item = T>>,
    pub name: Mutex<String>,
    pub pinned: AtomicBool,
}

impl<T: Data> RddCore<T> {
    /// Create a new core: allocates `RddVals`, registers a `OneToOne` dependency on `prev`,
    /// and names the operation `op_name`.
    pub fn new(id: usize, prev: Arc<dyn Rdd<Item = T>>, op_name: &str) -> Self {
        let rdd_base = prev.get_rdd_base();
        let mut vals = RddVals::new(id);
        vals.dependencies.push(Dependency::OneToOne { rdd_base });
        RddCore {
            vals: Arc::new(vals),
            prev,
            name: Mutex::new(op_name.to_owned()),
            pinned: AtomicBool::new(false),
        }
    }

    // ── RddBase delegation helpers ──────────────────────────────────────────

    pub fn rdd_id(&self) -> usize {
        self.vals.id
    }

    pub fn op_name(&self) -> String {
        self.name.lock().to_owned()
    }

    pub fn set_op_name(&self, name: &str) {
        *self.name.lock() = name.to_owned();
    }

    pub fn dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    pub fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }

    pub fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    pub fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        self.prev.preferred_locations(split)
    }

    pub fn is_pinned(&self) -> bool {
        self.pinned.load(Ordering::SeqCst)
    }

    /// Standard `iterator_any` implementation: boxes each output item as `Box<dyn Data>`.
    ///
    /// RDDs that need a custom `cogroup_iterator_any` (e.g. pair RDDs that box key and value
    /// separately) must still implement that method manually.
    pub fn iterator_any<U: Data + 'static>(
        &self,
        split: Box<dyn Split>,
        rdd: &dyn Rdd<Item = U>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        Ok(Box::new(
            rdd.iterator(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }
}

impl<T: Data> Clone for RddCore<T> {
    fn clone(&self) -> Self {
        RddCore {
            vals: self.vals.clone(),
            prev: self.prev.clone(),
            name: Mutex::new(self.name.lock().clone()),
            pinned: AtomicBool::new(self.pinned.load(Ordering::SeqCst)),
        }
    }
}
