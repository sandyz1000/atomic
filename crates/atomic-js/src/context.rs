use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::rdd::JsRdd;

/// The Atomic execution context for JavaScript.
///
/// Entry point for creating RDDs. In local mode (default) transformations run
/// eagerly in the Node.js thread. In distributed mode (set
/// `ATOMIC_DEPLOYMENT_MODE=distributed`) the context dispatches pipeline ops to
/// workers over TCP.
///
/// ```javascript
/// const { Context } = require('@atomic-compute/js');
/// const ctx = new Context();
/// const result = ctx.parallelize([1, 2, 3, 4])
///   .map(x => x * 2)
///   .filter(x => x > 4)
///   .collect();
/// // [6, 8]
/// ```
#[napi]
pub struct JsContext {
    pub(crate) inner: Arc<atomic_compute::context::Context>,
    default_parallelism: usize,
}

#[napi]
impl JsContext {
    /// Create an Atomic context.
    ///
    /// @param defaultParallelism - Default number of partitions when not
    ///   specified on individual operations. Defaults to the number of logical CPUs.
    #[napi(constructor)]
    pub fn new(default_parallelism: Option<u32>) -> Result<Self> {
        let inner = atomic_compute::context::Context::new()
            .map_err(|e| Error::from_reason(e.to_string()))?;
        let parallelism = default_parallelism.map(|n| n as usize).unwrap_or_else(|| {
            std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2)
        });
        Ok(Self { inner, default_parallelism: parallelism })
    }

    /// Distribute a JavaScript array as an RDD.
    ///
    /// @param data - Any JSON-serializable JavaScript array.
    /// @param numPartitions - Number of partitions. Defaults to `defaultParallelism`.
    #[napi]
    pub fn parallelize(
        &self,
        data: Vec<serde_json::Value>,
        num_partitions: Option<u32>,
    ) -> JsRdd {
        let partitions = num_partitions
            .map(|n| n as usize)
            .unwrap_or(self.default_parallelism)
            .max(1);
        JsRdd::from_data(data, partitions, Arc::clone(&self.inner))
    }

    /// Create an RDD of lines from a text file.
    ///
    /// @param path - Absolute or relative path to the file.
    #[napi]
    pub fn text_file(&self, path: String) -> Result<JsRdd> {
        let content = std::fs::read_to_string(&path)
            .map_err(|e| Error::from_reason(format!("text_file: {e}")))?;
        let elements: Vec<serde_json::Value> = content
            .lines()
            .map(|l| serde_json::Value::String(l.to_string()))
            .collect();
        Ok(JsRdd::from_data(elements, self.default_parallelism.max(1), Arc::clone(&self.inner)))
    }

    /// Create an RDD of integers in `[start, end)` with optional `step`.
    ///
    /// @param start - Range start (inclusive).
    /// @param end - Range end (exclusive).
    /// @param step - Increment (default 1).
    /// @param numPartitions - Number of partitions. Defaults to `defaultParallelism`.
    #[napi]
    pub fn range(
        &self,
        start: i64,
        end: i64,
        step: Option<i64>,
        num_partitions: Option<u32>,
    ) -> Result<JsRdd> {
        let step = step.unwrap_or(1);
        if step == 0 {
            return Err(Error::from_reason("step cannot be zero"));
        }
        // A Rust range `a..b` is empty when a >= b, so negative steps need explicit generation.
        let abs_step = step.unsigned_abs() as usize;
        let elements: Vec<serde_json::Value> = if step > 0 {
            let mut v = Vec::new();
            let mut cur = start;
            while cur < end {
                v.push(serde_json::Value::from(cur));
                cur += step;
            }
            v
        } else {
            let mut v = Vec::new();
            let mut cur = start;
            while cur > end {
                v.push(serde_json::Value::from(cur));
                cur -= abs_step as i64;
            }
            v
        };
        let partitions = num_partitions
            .map(|n| n as usize)
            .unwrap_or(self.default_parallelism)
            .max(1);
        Ok(JsRdd::from_data(elements, partitions, Arc::clone(&self.inner)))
    }

    /// Return the default number of partitions (CPU count or constructor value).
    #[napi]
    pub fn default_parallelism(&self) -> u32 {
        self.default_parallelism as u32
    }

    /// Stop the context and release resources.
    ///
    /// In distributed mode, sends a graceful-shutdown signal to every worker.
    #[napi]
    pub fn stop(&self) {
        self.inner.stop();
    }

    /// Broadcast a JSON-serializable value to all tasks.
    ///
    /// The returned `BroadcastVar` holds a serialized copy of the value that
    /// any task can read via `.value()`.
    #[napi]
    pub fn broadcast(&self, value: serde_json::Value) -> Result<crate::shared::BroadcastVar> {
        let data = serde_json::to_vec(&value)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(crate::shared::BroadcastVar::new(data))
    }

    /// Create an accumulator with an initial numeric, array, or string value.
    ///
    /// Call `.add(delta)` from within `forEach` / `map` etc. to accumulate
    /// values.  The accumulator is not automatically reset between actions.
    #[napi]
    pub fn accumulator(&self, zero: serde_json::Value) -> crate::shared::Accumulator {
        crate::shared::Accumulator::new(zero)
    }
}
