use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::rdd::JsRdd;

/// The Atomic execution context for JavaScript.
///
/// Entry point for creating RDDs. In local mode (default) transformations run
/// eagerly in the Node.js thread. In distributed mode (set
/// `VEGA_DEPLOYMENT_MODE=distributed`) the context dispatches pipeline ops to
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
        let elements: Vec<serde_json::Value> = (start..end)
            .step_by(step.unsigned_abs() as usize)
            .map(serde_json::Value::from)
            .collect();
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
}
