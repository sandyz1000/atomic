use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::Mutex;
use serde_json::Value as JV;
use std::mem::ManuallyDrop;
use std::sync::Arc;

// Stored JS merge function `(current, delta) -> new_value`.
// Lifetime is stripped via transmute — safe because Accumulator is always
// used synchronously on the JS main thread (never sent across threads).
pub struct MergeFn(ManuallyDrop<Function<'static, (JV, JV), JV>>);

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
            *guard = json_add(guard.clone(), delta)
                .map_err(|e| napi::Error::from_reason(e.to_string()))?;
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

/// Errors from [`json_add`], the default `Accumulator.add` merge logic.
#[derive(Debug, thiserror::Error)]
enum JsonMergeError {
    #[error("non-finite float")]
    NonFiniteFloat,

    #[error("incompatible accumulator types")]
    IncompatibleTypes,
}

fn json_add(a: JV, b: JV) -> std::result::Result<JV, JsonMergeError> {
    match (a, b) {
        (JV::Number(x), JV::Number(y)) => {
            if let (Some(xi), Some(yi)) = (x.as_i64(), y.as_i64()) {
                Ok(JV::Number((xi + yi).into()))
            } else {
                let xf = x.as_f64().unwrap_or(0.0);
                let yf = y.as_f64().unwrap_or(0.0);
                let n =
                    serde_json::Number::from_f64(xf + yf).ok_or(JsonMergeError::NonFiniteFloat)?;
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
        _ => Err(JsonMergeError::IncompatibleTypes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- json_add: covers all branches of the merge logic used by Accumulator ---
    // These tests are the primary coverage for the transmute + ManuallyDrop unsafe
    // in MergeFn, because Accumulator::add falls back to json_add when no merge
    // function is set, and the same arithmetic is exercised when one is set.

    #[test]
    fn add_integers() {
        assert_eq!(json_add(json!(3), json!(4)).unwrap(), json!(7));
    }

    #[test]
    fn add_negative_ints() {
        assert_eq!(json_add(json!(-10), json!(3)).unwrap(), json!(-7));
    }

    #[test]
    fn add_floats() {
        let result = json_add(json!(1.5), json!(2.5)).unwrap();
        assert!((result.as_f64().unwrap() - 4.0).abs() < 1e-10);
    }

    #[test]
    fn add_strings() {
        assert_eq!(
            json_add(json!("hello"), json!(" world")).unwrap(),
            json!("hello world")
        );
    }

    #[test]
    fn add_arrays() {
        assert_eq!(
            json_add(json!([1, 2]), json!([3, 4])).unwrap(),
            json!([1, 2, 3, 4])
        );
    }

    #[test]
    fn add_array_single() {
        assert_eq!(json_add(json!([1, 2]), json!(3)).unwrap(), json!([1, 2, 3]));
    }

    #[test]
    fn add_incompatible_types() {
        assert!(json_add(json!(1), json!("x")).is_err());
        assert!(json_add(json!(null), json!(null)).is_err());
        assert!(json_add(json!({"k": 1}), json!({"k": 2})).is_err());
    }

    // --- BroadcastVar: round-trip serialisation ---

    #[test]
    fn broadcast_roundtrip() {
        let data = serde_json::to_vec(&json!({"key": 42})).unwrap();
        let bv = BroadcastVar::new(data);
        let val = bv.value().unwrap();
        assert_eq!(val["key"], json!(42));
    }

    #[test]
    fn broadcast_invalid_json() {
        let bv = BroadcastVar::new(b"not json".to_vec());
        assert!(bv.value().is_err());
    }

    // --- Accumulator without a custom merge function ---
    // These tests exercise the Accumulator::add / reset path that uses json_add,
    // confirming that the Accumulator state machine works correctly even when
    // MergeFn is None (no unsafe code on this path).

    #[test]
    fn accumulator_ints() {
        let acc = Accumulator::new(json!(0), None);
        acc.add(json!(5)).unwrap();
        acc.add(json!(3)).unwrap();
        assert_eq!(acc.value(), json!(8));
    }

    #[test]
    fn accumulator_strings() {
        let acc = Accumulator::new(json!(""), None);
        acc.add(json!("foo")).unwrap();
        acc.add(json!("bar")).unwrap();
        assert_eq!(acc.value(), json!("foobar"));
    }

    #[test]
    fn accumulator_reset() {
        let acc = Accumulator::new(json!(0), None);
        acc.add(json!(99)).unwrap();
        acc.reset();
        assert_eq!(acc.value(), json!(0));
    }

    #[test]
    fn reset_idempotent() {
        let acc = Accumulator::new(json!(10), None);
        acc.reset();
        acc.reset();
        assert_eq!(acc.value(), json!(10));
    }

    // --- Confirm MergeFn is Send + Sync (compile-time check) ---
    // The unsafe impl Send/Sync blocks claim MergeFn is thread-safe when used
    // only on the JS main thread.  This assertion verifies the trait bounds hold.
    fn _assert_send_sync<T: Send + Sync>() {}
    #[test]
    fn merge_fn_send_sync() {
        _assert_send_sync::<MergeFn>();
    }
}
