use std::collections::VecDeque;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::Mutex;
use serde_json::Value as JV;

/// Queue handle for injecting test batches into a `testQueueStream`.
#[napi(js_name = "BatchQueue")]
pub struct JsBatchQueue {
    pub(crate) queue: Arc<Mutex<VecDeque<Vec<JV>>>>,
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
