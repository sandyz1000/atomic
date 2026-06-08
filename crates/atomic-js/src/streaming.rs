/// Streaming bindings for Node.js — Phase 4.4
///
/// Design: All transforms and output callbacks are stored as NAPI `Function`
/// references. Since `Function<'env, A, R>` carries a GC lifetime, we strip
/// the lifetime via `unsafe` transmute so the function can be stored in a
/// struct. This is safe as long as:
///   - All `JsDStream` objects are used only on the same JS thread.
///   - `run_one_batch()` is called synchronously from JS (never from a
///     background Rust thread).
///   - `start()` is a no-op — no background threading is used.
///
/// Elements are `serde_json::Value`. Pair elements are `[key, value]` JSON arrays.
/// State for `updateStateByKey` is stored as `serde_json::Value` per key.
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::Mutex;
use serde_json::Value as JV;

//
// We store NAPI functions by stripping their env lifetime. This is only safe
// because all operations are synchronous and run on the JS main thread.
// We use a newtype wrapper to make the unsafe code explicit.

struct StoredFn(
    // SAFETY: the Function is valid for as long as this struct is alive,
    // because it's only used in run_one_batch() on the JS main thread.
    *const (),
    FnKind,
);

#[derive(Clone, Copy)]
enum FnKind {
    MapJv,       // Function<JV, JV>
    FilterBool,  // Function<JV, bool>
    FlatMapVec,  // Function<JV, Vec<JV>>
    ReduceJvJv,  // Function<(JV, JV), JV>
    StateUpdate, // Function<(Vec<JV>, Option<JV>), Option<JV>>
    OutputCb,    // Function<Vec<JV>, ()>
}

// SAFETY: We only use StoredFn from the JS main thread.
unsafe impl Send for StoredFn {}
unsafe impl Sync for StoredFn {}

impl StoredFn {
    fn from_map(f: Function<JV, JV>) -> Self {
        // SAFETY: strip lifetime; only called from JS main thread
        let ptr = unsafe {
            std::mem::transmute::<Function<JV, JV>, Function<'static, JV, JV>>(f)
        };
        let boxed = Box::new(ptr);
        StoredFn(Box::into_raw(boxed) as *const (), FnKind::MapJv)
    }

    fn from_filter(f: Function<JV, bool>) -> Self {
        let ptr = unsafe {
            std::mem::transmute::<Function<JV, bool>, Function<'static, JV, bool>>(f)
        };
        let boxed = Box::new(ptr);
        StoredFn(Box::into_raw(boxed) as *const (), FnKind::FilterBool)
    }

    fn from_flat_map(f: Function<JV, Vec<JV>>) -> Self {
        let ptr = unsafe {
            std::mem::transmute::<Function<JV, Vec<JV>>, Function<'static, JV, Vec<JV>>>(f)
        };
        let boxed = Box::new(ptr);
        StoredFn(Box::into_raw(boxed) as *const (), FnKind::FlatMapVec)
    }

    fn from_reduce(f: Function<(JV, JV), JV>) -> Self {
        let ptr = unsafe {
            std::mem::transmute::<Function<(JV, JV), JV>, Function<'static, (JV, JV), JV>>(f)
        };
        let boxed = Box::new(ptr);
        StoredFn(Box::into_raw(boxed) as *const (), FnKind::ReduceJvJv)
    }

    fn from_state_update(f: Function<(Vec<JV>, Option<JV>), Option<JV>>) -> Self {
        let ptr = unsafe {
            std::mem::transmute::<
                Function<(Vec<JV>, Option<JV>), Option<JV>>,
                Function<'static, (Vec<JV>, Option<JV>), Option<JV>>,
            >(f)
        };
        let boxed = Box::new(ptr);
        StoredFn(Box::into_raw(boxed) as *const (), FnKind::StateUpdate)
    }

    fn from_output_cb(f: Function<Vec<JV>, ()>) -> Self {
        let ptr = unsafe {
            std::mem::transmute::<Function<Vec<JV>, ()>, Function<'static, Vec<JV>, ()>>(f)
        };
        let boxed = Box::new(ptr);
        StoredFn(Box::into_raw(boxed) as *const (), FnKind::OutputCb)
    }

    fn call_map(&self, x: JV) -> Result<JV> {
        // SAFETY: Only called from the JS main thread in run_one_batch().
        let f = unsafe { &*(self.0 as *const Function<'static, JV, JV>) };
        f.call(x)
    }

    fn call_filter(&self, x: JV) -> Result<bool> {
        let f = unsafe { &*(self.0 as *const Function<'static, JV, bool>) };
        f.call(x)
    }

    fn call_flat_map(&self, x: JV) -> Result<Vec<JV>> {
        let f = unsafe { &*(self.0 as *const Function<'static, JV, Vec<JV>>) };
        f.call(x)
    }

    fn call_reduce(&self, a: JV, b: JV) -> Result<JV> {
        let f = unsafe { &*(self.0 as *const Function<'static, (JV, JV), JV>) };
        f.call((a, b))
    }

    fn call_state_update(&self, new_vals: Vec<JV>, old: Option<JV>) -> Result<Option<JV>> {
        let f = unsafe {
            &*(self.0 as *const Function<'static, (Vec<JV>, Option<JV>), Option<JV>>)
        };
        f.call((new_vals, old))
    }

    fn call_output_cb(&self, batch: Vec<JV>) -> Result<()> {
        let f = unsafe { &*(self.0 as *const Function<'static, Vec<JV>, ()>) };
        f.call(batch)
    }
}

impl Drop for StoredFn {
    fn drop(&mut self) {
        match self.1 {
            FnKind::MapJv => unsafe {
                drop(Box::from_raw(self.0 as *mut Function<'static, JV, JV>))
            },
            FnKind::FilterBool => unsafe {
                drop(Box::from_raw(self.0 as *mut Function<'static, JV, bool>))
            },
            FnKind::FlatMapVec => unsafe {
                drop(Box::from_raw(self.0 as *mut Function<'static, JV, Vec<JV>>))
            },
            FnKind::ReduceJvJv => unsafe {
                drop(Box::from_raw(self.0 as *mut Function<'static, (JV, JV), JV>))
            },
            FnKind::StateUpdate => unsafe {
                drop(Box::from_raw(
                    self.0 as *mut Function<'static, (Vec<JV>, Option<JV>), Option<JV>>,
                ))
            },
            FnKind::OutputCb => unsafe {
                drop(Box::from_raw(self.0 as *mut Function<'static, Vec<JV>, ()>))
            },
        }
    }
}


enum JsStreamTransform {
    Map(StoredFn),
    Filter(StoredFn),
    FlatMap(StoredFn),
    ReduceByKey(StoredFn),
    GroupByKey,
    Join(Arc<JsDStreamInner>),
    LeftOuterJoin(Arc<JsDStreamInner>),
    UpdateStateByKey(StoredFn),
    MapValues(StoredFn),
}


enum JsDStreamInner {
    Queue {
        queue: Arc<Mutex<VecDeque<Vec<JV>>>>,
    },
    Transform {
        parent: Arc<JsDStreamInner>,
        op: JsStreamTransform,
    },
}


/// A lazy handle to a DStream transform chain (Node.js).
#[napi]
pub struct JsDStream {
    inner: Arc<JsDStreamInner>,
    pub(crate) is_pair: bool,
}

#[napi]
impl JsDStream {
    #[napi]
    pub fn map(&self, f: Function<JV, JV>) -> Result<JsDStream> {
        Ok(JsDStream {
            inner: Arc::new(JsDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: JsStreamTransform::Map(StoredFn::from_map(f)),
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
                op: JsStreamTransform::Filter(StoredFn::from_filter(f)),
            }),
            is_pair,
        })
    }

    #[napi]
    pub fn flat_map(&self, f: Function<JV, Vec<JV>>) -> Result<JsDStream> {
        Ok(JsDStream {
            inner: Arc::new(JsDStreamInner::Transform {
                parent: Arc::clone(&self.inner),
                op: JsStreamTransform::FlatMap(StoredFn::from_flat_map(f)),
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
                op: JsStreamTransform::ReduceByKey(StoredFn::from_reduce(f)),
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
                op: JsStreamTransform::UpdateStateByKey(StoredFn::from_state_update(f)),
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
                op: JsStreamTransform::MapValues(StoredFn::from_map(f)),
            }),
            is_pair: true,
        })
    }
}


/// Queue handle for injecting test batches into a `testQueueStream`.
#[napi]
pub struct JsBatchQueue {
    queue: Arc<Mutex<VecDeque<Vec<JV>>>>,
}

#[napi]
impl JsBatchQueue {
    /// Enqueue a JavaScript array as the next batch.
    #[napi]
    pub fn push(&self, batch: Vec<JV>) -> Result<()> {
        self.queue.lock().push_back(batch);
        Ok(())
    }
}


struct OutputOp {
    stream: Arc<JsDStreamInner>,
    callback: StoredFn,
}


fn compute_batch(
    inner: &JsDStreamInner,
    state_store: &mut HashMap<String, JV>,
) -> Result<Vec<JV>> {
    match inner {
        JsDStreamInner::Queue { queue } => {
            let mut q = queue.lock();
            Ok(q.pop_front().unwrap_or_default())
        }
        JsDStreamInner::Transform { parent, op } => {
            let parent_elems = compute_batch(parent, state_store)?;
            apply_transform(parent_elems, op, state_store)
        }
    }
}

fn apply_transform(
    elements: Vec<JV>,
    op: &JsStreamTransform,
    state_store: &mut HashMap<String, JV>,
) -> Result<Vec<JV>> {
    match op {
        JsStreamTransform::Map(f) => elements.into_iter().map(|e| f.call_map(e)).collect(),

        JsStreamTransform::Filter(f) => elements
            .into_iter()
            .filter_map(|e| match f.call_filter(e.clone()) {
                Ok(true) => Some(Ok(e)),
                Ok(false) => None,
                Err(err) => Some(Err(err)),
            })
            .collect(),

        JsStreamTransform::FlatMap(f) => {
            let mut result = Vec::new();
            for e in elements {
                result.extend(f.call_flat_map(e)?);
            }
            Ok(result)
        }

        JsStreamTransform::ReduceByKey(f) => {
            let mut groups: Vec<(JV, JV)> = Vec::new();
            for e in &elements {
                let pair = e
                    .as_array()
                    .ok_or_else(|| Error::from_reason("reduceByKey: element must be [k,v]"))?;
                if pair.len() != 2 {
                    return Err(Error::from_reason("reduceByKey: need 2-element array"));
                }
                let k = pair[0].clone();
                let v = pair[1].clone();
                let k_str = key_str(&k)?;
                let mut found = false;
                for (ek, acc) in &mut groups {
                    if key_str(ek)? == k_str {
                        *acc = f.call_reduce(acc.clone(), v.clone())?;
                        found = true;
                        break;
                    }
                }
                if !found {
                    groups.push((k, v));
                }
            }
            Ok(groups
                .into_iter()
                .map(|(k, v)| serde_json::json!([k, v]))
                .collect())
        }

        JsStreamTransform::GroupByKey => {
            let mut groups: Vec<(JV, Vec<JV>)> = Vec::new();
            for e in &elements {
                let pair = e
                    .as_array()
                    .ok_or_else(|| Error::from_reason("groupByKey: element must be [k,v]"))?;
                if pair.len() != 2 {
                    return Err(Error::from_reason("groupByKey: need 2-element array"));
                }
                let k = pair[0].clone();
                let v = pair[1].clone();
                let k_str = key_str(&k)?;
                let mut found = false;
                for (ek, vals) in &mut groups {
                    if key_str(ek)? == k_str {
                        vals.push(v.clone());
                        found = true;
                        break;
                    }
                }
                if !found {
                    groups.push((k, vec![v]));
                }
            }
            Ok(groups
                .into_iter()
                .map(|(k, vals)| serde_json::json!([k, vals]))
                .collect())
        }

        JsStreamTransform::Join(right_inner) => {
            let mut dummy: HashMap<String, JV> = HashMap::new();
            let right_elems = compute_batch(right_inner, &mut dummy)?;
            let mut result = Vec::new();
            for le in &elements {
                let lp = le
                    .as_array()
                    .ok_or_else(|| Error::from_reason("join: left must be [k,v]"))?;
                let lk = &lp[0];
                let lv = &lp[1];
                let lk_str = key_str(lk)?;
                for re in &right_elems {
                    let rp = re
                        .as_array()
                        .ok_or_else(|| Error::from_reason("join: right must be [k,v]"))?;
                    let rk = &rp[0];
                    let rv = &rp[1];
                    if key_str(rk)? == lk_str {
                        result.push(serde_json::json!([lk, [lv, rv]]));
                    }
                }
            }
            Ok(result)
        }

        JsStreamTransform::LeftOuterJoin(right_inner) => {
            let mut dummy: HashMap<String, JV> = HashMap::new();
            let right_elems = compute_batch(right_inner, &mut dummy)?;
            let mut result = Vec::new();
            for le in &elements {
                let lp = le
                    .as_array()
                    .ok_or_else(|| Error::from_reason("leftOuterJoin: left must be [k,v]"))?;
                let lk = &lp[0];
                let lv = &lp[1];
                let lk_str = key_str(lk)?;
                let mut matched = false;
                for re in &right_elems {
                    let rp = re.as_array().ok_or_else(|| {
                        Error::from_reason("leftOuterJoin: right must be [k,v]")
                    })?;
                    let rk = &rp[0];
                    let rv = &rp[1];
                    if key_str(rk)? == lk_str {
                        result.push(serde_json::json!([lk, [lv, rv]]));
                        matched = true;
                    }
                }
                if !matched {
                    result.push(serde_json::json!([lk, [lv, JV::Null]]));
                }
            }
            Ok(result)
        }

        JsStreamTransform::UpdateStateByKey(f) => {
            let mut new_vals: Vec<(String, JV, Vec<JV>)> = Vec::new();
            for e in &elements {
                let pair = e
                    .as_array()
                    .ok_or_else(|| Error::from_reason("updateStateByKey: element must be [k,v]"))?;
                let k = &pair[0];
                let v = pair[1].clone();
                let k_str = key_str(k)?;
                let mut found = false;
                for (ks, _, vals) in &mut new_vals {
                    if *ks == k_str {
                        vals.push(v.clone());
                        found = true;
                        break;
                    }
                }
                if !found {
                    new_vals.push((k_str, k.clone(), vec![v]));
                }
            }

            let mut all_keys: Vec<(String, JV)> = new_vals
                .iter()
                .map(|(ks, k, _)| (ks.clone(), k.clone()))
                .collect();
            for k_str in state_store.keys() {
                if !new_vals.iter().any(|(ks, _, _)| ks == k_str) {
                    all_keys.push((k_str.clone(), JV::String(k_str.clone())));
                }
            }

            let mut result = Vec::new();
            for (key_str_val, key_obj) in &all_keys {
                let new_values_for_key: Vec<JV> = new_vals
                    .iter()
                    .find(|(ks, _, _)| ks == key_str_val)
                    .map(|(_, _, v)| v.clone())
                    .unwrap_or_default();
                let old_state = state_store.get(key_str_val).cloned();
                let new_state = f.call_state_update(new_values_for_key, old_state)?;
                match new_state {
                    None => {
                        state_store.remove(key_str_val);
                    }
                    Some(ns) => {
                        state_store.insert(key_str_val.clone(), ns.clone());
                        result.push(serde_json::json!([key_obj, ns]));
                    }
                }
            }
            Ok(result)
        }

        JsStreamTransform::MapValues(f) => elements
            .into_iter()
            .map(|e| {
                let pair = e
                    .as_array()
                    .ok_or_else(|| Error::from_reason("mapValues: element must be [k,v]"))?;
                if pair.len() != 2 {
                    return Err(Error::from_reason("mapValues: need 2-element array"));
                }
                let new_v = f.call_map(pair[1].clone())?;
                Ok(serde_json::json!([pair[0], new_v]))
            })
            .collect(),
    }
}

fn key_str(val: &JV) -> Result<String> {
    match val {
        JV::String(s) => Ok(s.clone()),
        JV::Number(n) => Ok(n.to_string()),
        JV::Bool(b) => Ok(b.to_string()),
        other => Ok(other.to_string()),
    }
}


/// Streaming context for Node.js.
///
/// ```javascript
/// const ssc = new StreamingContext(0.1);
/// const [stream, queue] = ssc.testQueueStream();
/// const results = [];
/// ssc.foreachRdd(stream.map(x => x * 2), batch => results.push(...batch));
/// queue.push([1, 2, 3]);
/// ssc.runOneBatch();
/// // results === [2, 4, 6]
/// ```
#[napi]
pub struct JsStreamingContext {
    output_ops: Vec<OutputOp>,
    state_stores: Vec<HashMap<String, JV>>,
}

#[napi]
impl JsStreamingContext {
    #[napi(constructor)]
    pub fn new(_batch_secs: f64) -> Self {
        Self {
            output_ops: Vec::new(),
            state_stores: Vec::new(),
        }
    }

    /// Create a queue-backed stream for testing. Returns `[DStream, BatchQueue]`.
    #[napi]
    pub fn test_queue_stream(&self) -> (JsDStream, JsBatchQueue) {
        let queue: Arc<Mutex<VecDeque<Vec<JV>>>> = Arc::new(Mutex::new(VecDeque::new()));
        let dstream = JsDStream {
            inner: Arc::new(JsDStreamInner::Queue {
                queue: Arc::clone(&queue),
            }),
            is_pair: false,
        };
        (dstream, JsBatchQueue { queue })
    }

    /// Create a pair queue-backed stream. Returns `[DStream, BatchQueue]`.
    #[napi]
    pub fn test_pair_queue_stream(&self) -> (JsDStream, JsBatchQueue) {
        let queue: Arc<Mutex<VecDeque<Vec<JV>>>> = Arc::new(Mutex::new(VecDeque::new()));
        let dstream = JsDStream {
            inner: Arc::new(JsDStreamInner::Queue {
                queue: Arc::clone(&queue),
            }),
            is_pair: true,
        };
        (dstream, JsBatchQueue { queue })
    }

    /// Register an output operation: `callback(batchArray)` called once per batch.
    #[napi]
    pub fn foreach_rdd(
        &mut self,
        stream: &JsDStream,
        callback: Function<Vec<JV>, ()>,
    ) -> Result<()> {
        self.output_ops.push(OutputOp {
            stream: Arc::clone(&stream.inner),
            callback: StoredFn::from_output_cb(callback),
        });
        self.state_stores.push(HashMap::new());
        Ok(())
    }

    /// Run exactly one batch tick synchronously.
    #[napi]
    pub fn run_one_batch(&mut self) -> Result<()> {
        for (idx, op) in self.output_ops.iter().enumerate() {
            let state_store = &mut self.state_stores[idx];
            let elements = compute_batch(&op.stream, state_store)?;
            op.callback.call_output_cb(elements)?;
        }
        Ok(())
    }

    /// No-op — use `runOneBatch()` for testing.
    #[napi]
    pub fn start(&self) -> Result<()> {
        Ok(())
    }

    /// No-op.
    #[napi]
    pub fn stop(&self) {}
}
