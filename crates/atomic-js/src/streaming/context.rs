use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::Mutex;
use serde_json::Value as JV;

use super::batch_queue::JsBatchQueue;
use super::dstream::{JsDStream, JsDStreamInner};
use super::engine::compute_batch;

struct OutputOp {
    stream: Arc<JsDStreamInner>,
    callback: FunctionRef<Vec<JV>, ()>,
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
#[napi(js_name = "StreamingContext")]
pub struct JsStreamingContext {
    batch_secs: f64,
    output_ops: Vec<OutputOp>,
    state_stores: Vec<HashMap<String, JV>>,
    checkpoint_dir: Option<String>,
}

#[napi]
impl JsStreamingContext {
    #[napi(constructor)]
    pub fn new(batch_secs: f64) -> Self {
        Self {
            batch_secs,
            output_ops: Vec::new(),
            state_stores: Vec::new(),
            checkpoint_dir: None,
        }
    }

    /// Enable checkpointing to `dir`. State is written after each `runOneBatch()`.
    #[napi]
    pub fn checkpoint(&mut self, dir: String) -> Result<()> {
        std::fs::create_dir_all(&dir)
            .map_err(|e| Error::from_reason(format!("checkpoint dir: {e}")))?;
        self.checkpoint_dir = Some(dir);
        Ok(())
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
        let op_idx = self.output_ops.len();
        self.output_ops.push(OutputOp {
            stream: Arc::clone(&stream.inner),
            callback: callback.create_ref()?,
        });
        // When restored from checkpoint, state_stores may already have an entry.
        if self.state_stores.len() <= op_idx {
            self.state_stores.push(HashMap::new());
        }
        Ok(())
    }

    /// Run exactly one batch tick synchronously.
    #[napi]
    pub fn run_one_batch(&mut self, env: Env) -> Result<()> {
        for (idx, op) in self.output_ops.iter().enumerate() {
            let state_store = &mut self.state_stores[idx];
            let elements = compute_batch(&env, &op.stream, state_store)?;
            op.callback.borrow_back(&env)?.call(elements)?;
        }
        self.write_checkpoint_if_enabled()?;
        Ok(())
    }

    fn write_checkpoint_if_enabled(&self) -> Result<()> {
        let Some(ref dir) = self.checkpoint_dir else {
            return Ok(());
        };
        let serialisable: Vec<serde_json::Value> = self
            .state_stores
            .iter()
            .map(|store| {
                let obj: serde_json::Map<String, serde_json::Value> =
                    store.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                serde_json::Value::Object(obj)
            })
            .collect();
        let data = serde_json::json!({
            "batch_secs": self.batch_secs,
            "state_stores": serialisable,
        });
        let tmp = std::path::Path::new(dir).join("checkpoint.json.tmp");
        let final_path = std::path::Path::new(dir).join("checkpoint.json");
        std::fs::write(&tmp, data.to_string())
            .map_err(|e| Error::from_reason(format!("write checkpoint: {e}")))?;
        std::fs::rename(&tmp, &final_path)
            .map_err(|e| Error::from_reason(format!("rename checkpoint: {e}")))
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

/// Restore a `StreamingContext` from the latest checkpoint written to `dir`.
///
/// Returns `null` if no checkpoint exists. The caller must re-register all
/// DStreams and output operations on the returned context; the saved
/// `updateStateByKey` state stores are pre-loaded automatically.
///
/// ```javascript
/// const ssc = streamingContextFromCheckpoint('/tmp/cp') ?? new StreamingContext(1.0);
/// ```
#[napi]
#[allow(dead_code)] // exported to JS via napi; no Rust-side caller
pub fn streaming_context_from_checkpoint(dir: String) -> Result<Option<JsStreamingContext>> {
    let path = std::path::Path::new(&dir).join("checkpoint.json");
    if !path.exists() {
        return Ok(None);
    }
    let bytes =
        std::fs::read(&path).map_err(|e| Error::from_reason(format!("read checkpoint: {e}")))?;
    let data: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| Error::from_reason(format!("parse checkpoint: {e}")))?;
    let batch_secs = data["batch_secs"].as_f64().unwrap_or(1.0);
    let raw_stores = data["state_stores"].as_array().cloned().unwrap_or_default();
    let state_stores: Vec<HashMap<String, JV>> = raw_stores
        .iter()
        .map(|store| {
            store
                .as_object()
                .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default()
        })
        .collect();
    Ok(Some(JsStreamingContext {
        batch_secs,
        output_ops: Vec::new(),
        state_stores,
        checkpoint_dir: Some(dir),
    }))
}
