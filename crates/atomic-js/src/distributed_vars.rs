use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::Mutex;
use serde_json::Value as JV;
use std::mem::ManuallyDrop;
use std::sync::Arc;

// Stored JS merge function `(current, delta) -> new_value`.
// Lifetime is stripped via transmute — safe because Accumulator is always
// used synchronously on the JS main thread (never sent across threads).
pub(crate) struct MergeFn(ManuallyDrop<Function<'static, (JV, JV), JV>>);

// SAFETY: MergeFn is only used synchronously on the JS main thread.
unsafe impl Send for MergeFn {}
unsafe impl Sync for MergeFn {}

impl MergeFn {
    pub(crate) fn new(f: Function<(JV, JV), JV>) -> Self {
        let f_static = unsafe {
            std::mem::transmute::<Function<(JV, JV), JV>, Function<'static, (JV, JV), JV>>(f)
        };
        Self(ManuallyDrop::new(f_static))
    }

    fn call(&self, current: JV, delta: JV) -> Result<JV> {
        self.0.call((current, delta))
    }
}

impl Drop for MergeFn {
    fn drop(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.0) }
    }
}

#[napi]
pub struct BroadcastVar {
    data: Arc<Vec<u8>>, // JSON-serialized bytes
}

#[napi]
impl BroadcastVar {
    /// Deserialize and return the broadcast value.
    #[napi]
    pub fn value(&self) -> Result<serde_json::Value> {
        serde_json::from_slice(&self.data).map_err(|e| napi::Error::from_reason(e.to_string()))
    }
}

impl BroadcastVar {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data: Arc::new(data),
        }
    }
}

#[napi]
pub struct Accumulator {
    inner: Arc<Mutex<JV>>,
    initial: JV,
    merge_fn: Option<MergeFn>,
}

#[napi]
impl Accumulator {
    /// Add a delta using the merge function (if set) or default add logic.
    #[napi]
    pub fn add(&self, delta: JV) -> Result<()> {
        let mut guard = self.inner.lock();
        if let Some(f) = &self.merge_fn {
            *guard = f.call(guard.clone(), delta)?;
        } else {
            *guard = json_add(guard.clone(), delta).map_err(napi::Error::from_reason)?;
        }
        Ok(())
    }

    /// Return the current accumulated value.
    #[napi]
    pub fn value(&self) -> JV {
        self.inner.lock().clone()
    }

    /// Reset accumulator to its initial value.
    #[napi]
    pub fn reset(&self) {
        *self.inner.lock() = self.initial.clone();
    }
}

impl Accumulator {
    pub fn new(initial: JV, merge_fn: Option<MergeFn>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(initial.clone())),
            initial,
            merge_fn,
        }
    }
}

fn json_add(a: JV, b: JV) -> std::result::Result<JV, String> {
    match (a, b) {
        (JV::Number(x), JV::Number(y)) => {
            if let (Some(xi), Some(yi)) = (x.as_i64(), y.as_i64()) {
                Ok(JV::Number((xi + yi).into()))
            } else {
                let xf = x.as_f64().unwrap_or(0.0);
                let yf = y.as_f64().unwrap_or(0.0);
                let n = serde_json::Number::from_f64(xf + yf).ok_or("non-finite float")?;
                Ok(JV::Number(n))
            }
        }
        (JV::Array(mut av), JV::Array(bv)) => {
            av.extend(bv);
            Ok(JV::Array(av))
        }
        (JV::Array(mut av), single) => {
            av.push(single);
            Ok(JV::Array(av))
        }
        (JV::String(mut s), JV::String(t)) => {
            s.push_str(&t);
            Ok(JV::String(s))
        }
        _ => Err("incompatible accumulator types".to_string()),
    }
}
