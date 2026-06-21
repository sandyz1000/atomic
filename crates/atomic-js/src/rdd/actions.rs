use std::collections::HashMap;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde_json::Value as JsonValue;

use super::JsRdd;

#[napi]
impl JsRdd {
    /// Return all elements as a JavaScript array.
    #[napi]
    pub fn collect(&mut self) -> Result<Vec<JsonValue>> {
        if self.context.is_distributed() && self.staged.is_some() {
            return self.dispatch_and_collect();
        }
        Ok(self.elements.clone())
    }

    /// Return the number of elements.
    #[napi]
    pub fn count(&mut self) -> Result<u32> {
        if self.context.is_distributed() && self.staged.is_some() {
            let items = self.dispatch_and_collect()?;
            return Ok(items.len() as u32);
        }
        Ok(self.elements.len() as u32)
    }

    /// Return the first element. Throws if the RDD is empty.
    #[napi]
    pub fn first(&self) -> Result<JsonValue> {
        self.elements
            .first()
            .cloned()
            .ok_or_else(|| Error::from_reason("first: RDD is empty"))
    }

    /// Return the first `n` elements.
    #[napi]
    pub fn take(&self, n: u32) -> Vec<JsonValue> {
        self.elements.iter().take(n as usize).cloned().collect()
    }

    /// Aggregate all elements using `f(acc, element) => acc`. Throws if empty.
    #[napi]
    pub fn reduce(
        &mut self,
        f: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsonValue> {
        if self.context.is_distributed() {
            let fn_src = Self::fn_to_source(&f)?;
            let partition_fn = format!(
                "(partition) => partition.length === 0 ? [] : [partition.reduce(({}))]",
                fn_src
            );
            self.stage_js_task(
                partition_fn,
                atomic_data::distributed::TaskAction::Reduce,
                None,
            )?;
            let staged = self
                .staged
                .as_ref()
                .ok_or_else(|| Error::from_reason("no staged pipeline"))?;
            let result_bytes = self
                .context
                .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
                .map_err(|e| Error::from_reason(format!("dispatch_pipeline: {e}")))?;

            let mut acc: Option<JsonValue> = None;
            for bytes in result_bytes {
                let items: Vec<JsonValue> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::from_reason(format!("reduce decode: {e}")))?;
                for item in items {
                    acc = Some(match acc {
                        None => item,
                        Some(a) => f.call(FnArgs::from((a, item)))?,
                    });
                }
            }
            return acc.ok_or_else(|| Error::from_reason("reduce on empty RDD"));
        }
        let mut iter = self.elements.iter();
        let first = iter
            .next()
            .ok_or_else(|| Error::from_reason("reduce on empty RDD"))?;
        let mut acc = first.clone();
        for val in iter {
            acc = f.call(FnArgs::from((acc, val.clone())))?;
        }
        Ok(acc)
    }

    /// Aggregate with an initial value using `f(acc, element) => acc`.
    #[napi]
    pub fn fold(
        &mut self,
        zero: JsonValue,
        f: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsonValue> {
        if self.context.is_distributed() {
            let zero_json = zero.to_string();
            let fn_src = Self::fn_to_source(&f)?;
            let partition_fn = format!(
                "(partition) => [partition.reduce(({})  , {})]",
                fn_src, zero_json
            );
            self.stage_js_task(
                partition_fn,
                atomic_data::distributed::TaskAction::Fold,
                None,
            )?;
            let staged = self
                .staged
                .as_ref()
                .ok_or_else(|| Error::from_reason("no staged pipeline"))?;
            let result_bytes = self
                .context
                .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
                .map_err(|e| Error::from_reason(format!("dispatch_pipeline: {e}")))?;

            let mut acc = zero;
            for bytes in result_bytes {
                let items: Vec<JsonValue> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::from_reason(format!("fold decode: {e}")))?;
                for item in items {
                    acc = f.call(FnArgs::from((acc, item)))?;
                }
            }
            return Ok(acc);
        }
        let mut acc = zero;
        for val in &self.elements {
            acc = f.call(FnArgs::from((acc, val.clone())))?;
        }
        Ok(acc)
    }

    /// Apply `f` to each element for side effects.
    #[napi]
    pub fn for_each(&self, f: Function<JsonValue, Unknown>) -> Result<()> {
        for elem in &self.elements {
            f.call(elem.clone())?;
        }
        Ok(())
    }

    /// Apply `f` to each logical partition for side effects.
    #[napi]
    pub fn for_each_partition(&self, f: Function<Vec<JsonValue>, Unknown>) -> Result<()> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        let chunk_size = total.div_ceil(np).max(1);
        for chunk in self.elements.chunks(chunk_size) {
            f.call(chunk.to_vec())?;
        }
        Ok(())
    }

    /// Count occurrences of each distinct element. Returns `{element: count}`.
    #[napi]
    pub fn count_by_value(&self) -> JsonValue {
        let mut counts: HashMap<String, u64> = HashMap::new();
        for elem in &self.elements {
            *counts.entry(elem.to_string()).or_insert(0) += 1;
        }
        serde_json::to_value(counts).unwrap_or(JsonValue::Object(Default::default()))
    }

    /// Count elements per key in a pair RDD. Returns `{key: count}`.
    #[napi]
    pub fn count_by_key(&self) -> Result<JsonValue> {
        let mut counts: HashMap<String, u64> = HashMap::new();
        for elem in &self.elements {
            let pair = elem
                .as_array()
                .ok_or_else(|| Error::from_reason("count_by_key requires [key, value] arrays"))?;
            if pair.is_empty() {
                return Err(Error::from_reason("count_by_key requires non-empty arrays"));
            }
            let key = Self::key_to_string(&pair[0])?;
            *counts.entry(key).or_insert(0) += 1;
        }
        Ok(serde_json::to_value(counts).unwrap_or(JsonValue::Object(Default::default())))
    }

    /// Return true if the RDD has no elements.
    #[napi]
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Return the maximum element. Optional comparator `f(a, b) => number` (positive = a > b).
    #[napi]
    pub fn max(
        &self,
        comparator: Option<Function<FnArgs<(JsonValue, JsonValue)>, f64>>,
    ) -> Result<JsonValue> {
        if self.elements.is_empty() {
            return Err(Error::from_reason("max on empty RDD"));
        }
        let mut result = self.elements[0].clone();
        for elem in &self.elements[1..] {
            let is_greater = match &comparator {
                Some(cmp) => cmp.call(FnArgs::from((elem.clone(), result.clone())))? > 0.0,
                None => Self::json_compare(elem, &result) == std::cmp::Ordering::Greater,
            };
            if is_greater {
                result = elem.clone();
            }
        }
        Ok(result)
    }

    /// Return the minimum element. Optional comparator `f(a, b) => number` (negative = a < b).
    #[napi]
    pub fn min(
        &self,
        comparator: Option<Function<FnArgs<(JsonValue, JsonValue)>, f64>>,
    ) -> Result<JsonValue> {
        if self.elements.is_empty() {
            return Err(Error::from_reason("min on empty RDD"));
        }
        let mut result = self.elements[0].clone();
        for elem in &self.elements[1..] {
            let is_less = match &comparator {
                Some(cmp) => cmp.call(FnArgs::from((elem.clone(), result.clone())))? < 0.0,
                None => Self::json_compare(elem, &result) == std::cmp::Ordering::Less,
            };
            if is_less {
                result = elem.clone();
            }
        }
        Ok(result)
    }

    /// Write each element as a line to `path`.
    ///
    /// Accepts a local file path or, when built with the `s3` feature, an
    /// S3 URI (`s3://bucket/prefix`).
    #[napi]
    pub fn save_as_text_file(&self, path: String) -> Result<()> {
        #[cfg(feature = "s3")]
        if path.starts_with("s3://") {
            use atomic_compute::io::s3::s3_impl::{S3Uri, write_text};
            let s3uri = S3Uri::parse(&path).ok_or_else(|| {
                Error::from_reason(format!("save_as_text_file: invalid S3 URI: {path}"))
            })?;
            let content: String = self
                .elements
                .iter()
                .map(|elem| {
                    let line = match elem {
                        JsonValue::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    format!("{line}\n")
                })
                .collect();
            let key = format!("{}/part-0", s3uri.key.trim_end_matches('/'));
            write_text(&s3uri.bucket, &key, content)
                .map_err(|e| Error::from_reason(format!("save_as_text_file S3: {e}")))?;
            return Ok(());
        }
        #[cfg(not(feature = "s3"))]
        if path.starts_with("s3://") {
            return Err(Error::from_reason(
                "save_as_text_file: s3:// URIs require the 's3' feature flag",
            ));
        }
        use std::io::Write;
        let mut file = std::fs::File::create(&path)
            .map_err(|e| Error::from_reason(format!("save_as_text_file: {e}")))?;
        for elem in &self.elements {
            let line = match elem {
                JsonValue::String(s) => s.clone(),
                other => other.to_string(),
            };
            writeln!(file, "{}", line)
                .map_err(|e| Error::from_reason(format!("save_as_text_file write: {e}")))?;
        }
        Ok(())
    }

    /// Two-phase aggregation: `seqFn(acc, elem) => acc` within partitions,
    /// `combFn(acc, acc) => acc` across partition results.
    #[napi]
    pub fn aggregate(
        &self,
        zero: JsonValue,
        seq_fn: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
        comb_fn: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsonValue> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        let chunk_size = total.div_ceil(np).max(1);

        let mut partition_results = Vec::new();
        for chunk in self.elements.chunks(chunk_size) {
            let mut acc = zero.clone();
            for elem in chunk {
                acc = seq_fn.call(FnArgs::from((acc, elem.clone())))?;
            }
            partition_results.push(acc);
        }

        let mut result = zero;
        for part_acc in partition_results {
            result = comb_fn.call(FnArgs::from((result, part_acc)))?;
        }
        Ok(result)
    }

    /// Return elements divided into `num_partitions` logical partition arrays.
    #[napi]
    pub fn glom(&self) -> Vec<Vec<JsonValue>> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        let chunk_size = total.div_ceil(np).max(1);
        let mut result: Vec<Vec<JsonValue>> = self
            .elements
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();
        while result.len() < np {
            result.push(Vec::new());
        }
        result
    }

    /// Mark this RDD to be cached in memory on first action (no-op in local mode).
    #[napi]
    pub fn cache(&self) -> JsRdd {
        JsRdd::from_data(
            self.elements.clone(),
            self.num_partitions,
            Arc::clone(&self.context),
        )
    }

    /// Mark this RDD to be persisted (alias for `cache` in local mode).
    #[napi]
    pub fn persist(&self) -> JsRdd {
        self.cache()
    }

    /// Remove this RDD from the cache (no-op in local mode).
    #[napi]
    pub fn unpersist(&self) -> JsRdd {
        self.cache()
    }

    /// Write all elements to `{path}/checkpoint.json` and return a new RDD
    /// backed by the same elements.
    ///
    /// The write is atomic: data is first written to `{path}/checkpoint.json.tmp`
    /// and then renamed to `{path}/checkpoint.json`.
    #[napi]
    pub fn checkpoint(&self, path: String) -> Result<JsRdd> {
        std::fs::create_dir_all(&path)
            .map_err(|e| Error::from_reason(format!("checkpoint: create_dir_all failed: {e}")))?;

        let tmp_path = format!("{path}/checkpoint.json.tmp");
        let final_path = format!("{path}/checkpoint.json");

        let json = serde_json::to_vec(&self.elements)
            .map_err(|e| Error::from_reason(format!("checkpoint: serialize failed: {e}")))?;

        std::fs::write(&tmp_path, &json)
            .map_err(|e| Error::from_reason(format!("checkpoint: write failed: {e}")))?;

        std::fs::rename(&tmp_path, &final_path)
            .map_err(|e| Error::from_reason(format!("checkpoint: rename failed: {e}")))?;

        Ok(JsRdd::from_data(
            self.elements.clone(),
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Return the logical number of partitions.
    #[napi(getter)]
    pub fn num_partitions(&self) -> u32 {
        self.num_partitions as u32
    }

    #[napi]
    pub fn length(&self) -> u32 {
        self.elements.len() as u32
    }
}
