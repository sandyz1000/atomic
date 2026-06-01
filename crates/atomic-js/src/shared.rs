use std::sync::Arc;
use parking_lot::Mutex;
use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi]
pub struct BroadcastVar {
    data: Arc<Vec<u8>>, // JSON-serialized bytes
}

#[napi]
impl BroadcastVar {
    /// Deserialize and return the broadcast value.
    #[napi]
    pub fn value(&self) -> Result<serde_json::Value> {
        serde_json::from_slice(&self.data)
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }
}

impl BroadcastVar {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data: Arc::new(data) }
    }
}

#[napi]
pub struct Accumulator {
    inner: Arc<Mutex<serde_json::Value>>,
    initial: serde_json::Value,
}

#[napi]
impl Accumulator {
    /// Add a numeric, array, or string delta.
    #[napi]
    pub fn add(&self, delta: serde_json::Value) -> Result<()> {
        let mut guard = self.inner.lock();
        *guard = json_add(guard.clone(), delta)
            .map_err(|e| napi::Error::from_reason(e))?;
        Ok(())
    }

    /// Return the current accumulated value.
    #[napi]
    pub fn value(&self) -> serde_json::Value {
        self.inner.lock().clone()
    }

    /// Reset accumulator to its initial value.
    #[napi]
    pub fn reset(&self) {
        *self.inner.lock() = self.initial.clone();
    }
}

impl Accumulator {
    pub fn new(initial: serde_json::Value) -> Self {
        Self {
            inner: Arc::new(Mutex::new(initial.clone())),
            initial,
        }
    }
}

fn json_add(
    a: serde_json::Value,
    b: serde_json::Value,
) -> std::result::Result<serde_json::Value, String> {
    match (a, b) {
        (serde_json::Value::Number(x), serde_json::Value::Number(y)) => {
            if let (Some(xi), Some(yi)) = (x.as_i64(), y.as_i64()) {
                Ok(serde_json::Value::Number((xi + yi).into()))
            } else {
                let xf = x.as_f64().unwrap_or(0.0);
                let yf = y.as_f64().unwrap_or(0.0);
                let n = serde_json::Number::from_f64(xf + yf)
                    .ok_or("non-finite float")?;
                Ok(serde_json::Value::Number(n))
            }
        }
        (serde_json::Value::Array(mut av), serde_json::Value::Array(bv)) => {
            av.extend(bv);
            Ok(serde_json::Value::Array(av))
        }
        (serde_json::Value::Array(mut av), single) => {
            av.push(single);
            Ok(serde_json::Value::Array(av))
        }
        (serde_json::Value::String(mut s), serde_json::Value::String(t)) => {
            s.push_str(&t);
            Ok(serde_json::Value::String(s))
        }
        _ => Err("incompatible accumulator types".to_string()),
    }
}
