use std::sync::Arc;

use atomic_compute::context::Context;
use atomic_data::distributed::{JsUdfPayload, PipelineOp, TaskAction, TaskRuntime};
use napi::JsValue as _;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde_json::Value as JsonValue;

mod actions;
mod agent;
mod errors;
mod pair_ops;
mod sort;
mod transforms;
mod with_context;

#[derive(Clone)]
struct StagedJsPipeline {
    source_partitions: Vec<Vec<u8>>,
    ops: Vec<PipelineOp>,
}

/// A distributed dataset (RDD) of JavaScript values.
///
/// In local mode all transformations execute eagerly in the Node.js thread.
/// In distributed mode transformations build a lazy pipeline that is dispatched
/// to workers as a single `TaskEnvelope` per partition when an action is called.
///
/// ```javascript
/// const rdd = ctx.parallelize([1, 2, 3, 4]);
/// const result = rdd
///   .map(x => x * 2)
///   .filter(x => x > 4)
///   .collect();
/// // [6, 8]
/// ```
#[napi]
pub struct JsRdd {
    pub(crate) elements: Vec<JsonValue>,
    pub(crate) num_partitions: usize,
    pub(crate) context: Arc<Context>,
    staged: Option<StagedJsPipeline>,
}

impl JsRdd {
    pub fn from_data(
        elements: Vec<JsonValue>,
        num_partitions: usize,
        context: Arc<Context>,
    ) -> Self {
        Self {
            elements,
            num_partitions,
            context,
            staged: None,
        }
    }

    fn fn_to_source<A: JsValuesTupleIntoVec, R>(f: &Function<'_, A, R>) -> Result<String> {
        let src = f.coerce_to_string()?.into_utf8()?.into_owned()?;
        errors::reject_native_source(&src)?;
        Ok(src)
    }

    fn encode_source_partitions(&self) -> Result<Vec<Vec<u8>>> {
        let total = self.elements.len();
        let np = self.num_partitions.max(1);
        let chunk_size = total.div_ceil(np).max(1);

        let mut partitions = Vec::with_capacity(np);
        for chunk in self.elements.chunks(chunk_size) {
            let json = serde_json::to_vec(chunk)
                .map_err(|e| Error::from_reason(format!("encode_partition: {e}")))?;
            partitions.push(json);
        }
        while partitions.len() < np {
            partitions.push(b"[]".to_vec());
        }
        Ok(partitions)
    }

    fn stage_js_udf(
        &mut self,
        fn_source: String,
        action: TaskAction,
        context_json: Option<String>,
    ) -> Result<()> {
        let payload_struct = JsUdfPayload {
            fn_source,
            zero_json: String::new(),
            context_json,
        };
        let payload = serde_json::to_vec(&payload_struct)
            .map_err(|e| Error::from_reason(format!("JsUdfPayload encode: {e}")))?;
        let op = PipelineOp {
            op_id: "atomic::udf::js".to_string(),
            action,
            runtime: TaskRuntime::JavaScript,
            payload,
        };
        if let Some(ref mut staged) = self.staged {
            staged.ops.push(op);
        } else {
            let source_partitions = self.encode_source_partitions()?;
            self.staged = Some(StagedJsPipeline {
                source_partitions,
                ops: vec![op],
            });
        }
        Ok(())
    }

    fn take_as_new(&mut self) -> JsRdd {
        JsRdd {
            elements: Vec::new(),
            num_partitions: self.num_partitions,
            context: Arc::clone(&self.context),
            staged: self.staged.take(),
        }
    }

    /// Collect rows without `&mut` — used internally (e.g. SQL registration).
    pub(crate) fn collect_rows(&self) -> Result<Vec<JsonValue>> {
        if self.context.is_distributed() && self.staged.is_some() {
            self.dispatch_and_collect()
        } else {
            Ok(self.elements.clone())
        }
    }

    fn dispatch_and_collect(&self) -> Result<Vec<JsonValue>> {
        let staged = self
            .staged
            .as_ref()
            .ok_or_else(|| Error::from_reason("no staged pipeline to dispatch"))?;
        let result_bytes = self
            .context
            .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
            .map_err(|e| Error::from_reason(format!("dispatch_pipeline: {e}")))?;
        let mut all = Vec::new();
        for bytes in result_bytes {
            let items: Vec<JsonValue> = serde_json::from_slice(&bytes)
                .map_err(|e| Error::from_reason(format!("decode result: {e}")))?;
            all.extend(items);
        }
        Ok(all)
    }

    /// Like `dispatch_and_collect` but returns one `Vec<JsonValue>` per partition.
    /// Used by `sort_by_key` to feed the k-way merge with pre-sorted runs.
    pub(crate) fn dispatch_and_collect_partitioned(&self) -> Result<Vec<Vec<JsonValue>>> {
        let staged = self
            .staged
            .as_ref()
            .ok_or_else(|| Error::from_reason("no staged pipeline to dispatch"))?;
        let result_bytes = self
            .context
            .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
            .map_err(|e| Error::from_reason(format!("dispatch_pipeline: {e}")))?;
        result_bytes
            .into_iter()
            .map(|bytes| {
                serde_json::from_slice::<Vec<JsonValue>>(&bytes)
                    .map_err(|e| Error::from_reason(format!("decode result: {e}")))
            })
            .collect()
    }

    pub(crate) fn key_to_string(val: &JsonValue) -> Result<String> {
        match val {
            JsonValue::String(s) => Ok(s.clone()),
            JsonValue::Number(n) => Ok(n.to_string()),
            JsonValue::Bool(b) => Ok(b.to_string()),
            other => Err(Error::from_reason(format!("Unsupported key type: {other}"))),
        }
    }

    pub(crate) fn json_compare(a: &JsonValue, b: &JsonValue) -> std::cmp::Ordering {
        match (a, b) {
            (JsonValue::Number(an), JsonValue::Number(bn)) => an
                .as_f64()
                .unwrap_or(0.0)
                .partial_cmp(&bn.as_f64().unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal),
            (JsonValue::String(as_), JsonValue::String(bs)) => as_.cmp(bs),
            _ => a.to_string().cmp(&b.to_string()),
        }
    }
}
