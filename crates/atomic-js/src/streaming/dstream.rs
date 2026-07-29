use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::Mutex;
use serde_json::Value as JV;

/// `FunctionRef` type aliases for multi-argument JS callbacks.
/// `FnArgs<(A, B)>` selects the tuple `JsValuesTupleIntoVec` impl so each
/// element becomes a *separate* JS function argument, rather than the single-T
/// blanket impl that would serialize `(A, B)` as one JS array argument.
type ReduceFnRef = FunctionRef<FnArgs<(JV, JV)>, JV>;
type StateUpdateFnRef = FunctionRef<FnArgs<(Vec<JV>, Option<JV>)>, Option<JV>>;
type StateUpdateFn<'a> = Function<'a, FnArgs<(Vec<JV>, Option<JV>)>, Option<JV>>;

// JS callbacks are stored as persistent napi references (`FunctionRef`) so they
// survive scope boundaries; `FunctionRef::borrow_back(&env)` re-materialises a
// callable handle. Each `JsStreamTransform` variant holds the concrete
// `FunctionRef` for its operation, so calling never has to re-discriminate.

/// Persist a `reduceByKey` callback as a `FnArgs` ref so `(a, b)` is passed as
/// two separate JS arguments rather than one array.
fn reduce_ref(f: Function<(JV, JV), JV>) -> Result<ReduceFnRef> {
    // SAFETY: Function<(A,B), R> and Function<FnArgs<(A,B)>, R> have the same
    // memory layout (phantom types only). The transmute selects the multi-arg
    // JsValuesTupleIntoVec impl so (a, b) is passed as two separate JS args.
    let f: Function<FnArgs<(JV, JV)>, JV> = unsafe { std::mem::transmute(f) };
    f.create_ref()
}

/// Persist an `updateStateByKey` callback; same `FnArgs` rationale as
/// [`reduce_ref`].
fn state_update_ref(f: Function<(Vec<JV>, Option<JV>), Option<JV>>) -> Result<StateUpdateFnRef> {
    // SAFETY: same layout rationale as `reduce_ref`.
    let f: StateUpdateFn<'_> = unsafe { std::mem::transmute(f) };
    f.create_ref()
}

pub(crate) enum JsStreamTransform {
    Map(FunctionRef<JV, JV>),
    Filter(FunctionRef<JV, bool>),
    FlatMap(FunctionRef<JV, Vec<JV>>),
    ReduceByKey(ReduceFnRef),
    GroupByKey,
    Join(Arc<JsDStreamInner>),
    LeftOuterJoin(Arc<JsDStreamInner>),
    UpdateStateByKey(StateUpdateFnRef),
    MapValues(FunctionRef<JV, JV>),
}

pub(crate) enum JsDStreamInner {
    Queue {
        queue: Arc<Mutex<VecDeque<Vec<JV>>>>,
    },
    Transform {
        parent: Arc<JsDStreamInner>,
        op: JsStreamTransform,
    },
    Windowed {
        parent: Arc<JsDStreamInner>,
        window_ms: u64,
        buffer: Mutex<VecDeque<(Instant, Vec<JV>)>>,
    },
}

/// A lazy handle to a DStream transform chain (Node.js).
#[napi(js_name = "DStream")]
pub struct JsDStream {
    pub(crate) inner: Arc<JsDStreamInner>,
    pub(crate) is_pair: bool,
}

#[napi]
impl JsDStream {
    #[napi]
    pub fn map(&self, f: Function<JV, JV>) -> Result<JsDStream> {
        Ok(JsDStream {
            inner: Arc::new(JsDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: JsStreamTransform::Map(f.create_ref()?),
            }),
            is_pair: false,
        })
    }

    #[napi]
    pub fn filter(&self, f: Function<JV, bool>) -> Result<JsDStream> {
        let is_pair = self.is_pair;
        Ok(JsDStream {
            inner: Arc::new(JsDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: JsStreamTransform::Filter(f.create_ref()?),
            }),
            is_pair,
        })
    }

    #[napi]
    pub fn flat_map(&self, f: Function<JV, Vec<JV>>) -> Result<JsDStream> {
        Ok(JsDStream {
            inner: Arc::new(JsDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: JsStreamTransform::FlatMap(f.create_ref()?),
            }),
            is_pair: false,
        })
    }

    #[napi]
    pub fn reduce_by_key(&self, f: Function<(JV, JV), JV>) -> Result<JsDStream> {
        if !self.is_pair {
            return Err(Error::from_reason("reduceByKey requires a pair DStream"));
        }
        Ok(JsDStream {
            inner: Arc::new(JsDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: JsStreamTransform::ReduceByKey(reduce_ref(f)?),
            }),
            is_pair: true,
        })
    }

    #[napi]
    pub fn group_by_key(&self) -> Result<JsDStream> {
        if !self.is_pair {
            return Err(Error::from_reason("groupByKey requires a pair DStream"));
        }
        Ok(JsDStream {
            inner: Arc::new(JsDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: JsStreamTransform::GroupByKey,
            }),
            is_pair: true,
        })
    }

    #[napi]
    pub fn join(&self, other: &JsDStream) -> Result<JsDStream> {
        if !self.is_pair || !other.is_pair {
            return Err(Error::from_reason("join requires pair DStreams"));
        }
        Ok(JsDStream {
            inner: Arc::new(JsDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: JsStreamTransform::Join(Arc::clone(&other.inner)),
            }),
            is_pair: true,
        })
    }

    #[napi]
    pub fn left_outer_join(&self, other: &JsDStream) -> Result<JsDStream> {
        if !self.is_pair || !other.is_pair {
            return Err(Error::from_reason("leftOuterJoin requires pair DStreams"));
        }
        Ok(JsDStream {
            inner: Arc::new(JsDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: JsStreamTransform::LeftOuterJoin(Arc::clone(&other.inner)),
            }),
            is_pair: true,
        })
    }

    #[napi]
    pub fn update_state_by_key(
        &self,
        f: Function<(Vec<JV>, Option<JV>), Option<JV>>,
    ) -> Result<JsDStream> {
        if !self.is_pair {
            return Err(Error::from_reason(
                "updateStateByKey requires a pair DStream",
            ));
        }
        Ok(JsDStream {
            inner: Arc::new(JsDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: JsStreamTransform::UpdateStateByKey(state_update_ref(f)?),
            }),
            is_pair: true,
        })
    }

    #[napi]
    pub fn map_values(&self, f: Function<JV, JV>) -> Result<JsDStream> {
        if !self.is_pair {
            return Err(Error::from_reason("mapValues requires a pair DStream"));
        }
        Ok(JsDStream {
            inner: Arc::new(JsDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: JsStreamTransform::MapValues(f.create_ref()?),
            }),
            is_pair: true,
        })
    }

    /// Return a new DStream that unions the parent's batches over a sliding window.
    ///
    /// `windowMs`  — how far back (in milliseconds) to include batches.
    /// `slideMs`   — accepted for API compatibility; every `runOneBatch()` call
    ///               advances the window by one tick in this in-process model.
    #[napi]
    pub fn window(&self, window_ms: u32, _slide_ms: u32) -> JsDStream {
        JsDStream {
            inner: Arc::new(JsDStreamInner::Windowed {
                parent: Arc::clone(&self.inner),
                window_ms: window_ms as u64,
                buffer: Mutex::new(VecDeque::new()),
            }),
            is_pair: self.is_pair,
        }
    }

    /// Reduce elements per window.
    #[napi]
    pub fn reduce_by_window(
        &self,
        f: Function<JV, JV>,
        window_ms: u32,
        slide_ms: u32,
    ) -> Result<JsDStream> {
        self.window(window_ms, slide_ms).map(f)
    }

    /// Reduce by key per window.
    #[napi]
    pub fn reduce_by_key_and_window(
        &self,
        f: Function<(JV, JV), JV>,
        window_ms: u32,
        slide_ms: u32,
    ) -> Result<JsDStream> {
        let w = self.window(window_ms, slide_ms);
        w.reduce_by_key(f)
    }

    /// Transform each batch through `func(batch) => newBatch`.
    #[napi]
    pub fn transform(&self, f: Function<JV, JV>) -> Result<JsDStream> {
        self.map(f)
    }

    /// Transform with another DStream: `func(selfBatch, otherBatch) => newBatch`.
    #[napi]
    pub fn transform_with(&self, other: &JsDStream, f: Function<JV, JV>) -> Result<JsDStream> {
        self.join(other)?.map(f)
    }
}
