use std::collections::HashMap;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi::JsValue as _;
use napi_derive::napi;

use atomic_compute::context::Context;
use atomic_data::distributed::{JsUdfPayload, PipelineOp, TaskAction, TaskRuntime};

/// Accumulated lazy ops for distributed execution.
struct StagedJsPipeline {
    /// JSON-encoded partition data (one `Vec<u8>` per partition).
    source_partitions: Vec<Vec<u8>>,
    /// Ordered ops to dispatch to workers.
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
    pub(crate) elements: Vec<serde_json::Value>,
    pub(crate) num_partitions: usize,
    pub(crate) context: Arc<Context>,
    staged: Option<StagedJsPipeline>,
}

// ── Internal constructors and helpers ────────────────────────────────────────

impl JsRdd {
    pub fn from_data(
        elements: Vec<serde_json::Value>,
        num_partitions: usize,
        context: Arc<Context>,
    ) -> Self {
        Self { elements, num_partitions, context, staged: None }
    }

    /// Capture the source string of a JS function via `Function.prototype.toString()`.
    fn fn_to_source<A: JsValuesTupleIntoVec, R>(f: &Function<'_, A, R>) -> Result<String> {
        f.coerce_to_string()?.into_utf8()?.into_owned()
    }

    /// JSON-encode elements split into `num_partitions` chunks.
    fn encode_source_partitions(&self) -> Result<Vec<Vec<u8>>> {
        let total = self.elements.len();
        let np = self.num_partitions.max(1);
        let chunk_size = ((total + np - 1) / np).max(1);

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

    /// Build a JS UDF PipelineOp and push it into the staged pipeline.
    fn stage_js_udf(&mut self, fn_source: String, action: TaskAction) -> Result<()> {
        let payload_struct = JsUdfPayload {
            fn_source,
            zero_json: String::new(),
            context_json: None,
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
            self.staged = Some(StagedJsPipeline { source_partitions, ops: vec![op] });
        }
        Ok(())
    }

    /// Move staged state into a new JsRdd (data is now in source_partitions).
    fn take_as_new(&mut self) -> JsRdd {
        JsRdd {
            elements: Vec::new(),
            num_partitions: self.num_partitions,
            context: Arc::clone(&self.context),
            staged: self.staged.take(),
        }
    }

    /// Dispatch staged pipeline and decode result bytes.
    fn dispatch_and_collect(&self) -> Result<Vec<serde_json::Value>> {
        let staged = self.staged.as_ref()
            .ok_or_else(|| Error::from_reason("no staged pipeline to dispatch"))?;
        let result_bytes = self.context
            .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
            .map_err(|e| Error::from_reason(format!("dispatch_pipeline: {e}")))?;
        let mut all = Vec::new();
        for bytes in result_bytes {
            let items: Vec<serde_json::Value> = serde_json::from_slice(&bytes)
                .map_err(|e| Error::from_reason(format!("decode result: {e}")))?;
            all.extend(items);
        }
        Ok(all)
    }

    fn key_to_string(val: &serde_json::Value) -> Result<String> {
        match val {
            serde_json::Value::String(s) => Ok(s.clone()),
            serde_json::Value::Number(n) => Ok(n.to_string()),
            serde_json::Value::Bool(b) => Ok(b.to_string()),
            other => Err(Error::from_reason(format!(
                "key must be a string, number, or boolean; got: {other}"
            ))),
        }
    }

    /// Default JSON value comparison for ordering operations.
    fn json_compare(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
        match (a, b) {
            (serde_json::Value::Number(an), serde_json::Value::Number(bn)) => {
                an.as_f64().unwrap_or(0.0)
                    .partial_cmp(&bn.as_f64().unwrap_or(0.0))
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
            (serde_json::Value::String(as_), serde_json::Value::String(bs)) => as_.cmp(bs),
            _ => a.to_string().cmp(&b.to_string()),
        }
    }
}

// ── napi methods ─────────────────────────────────────────────────────────────

#[napi]
impl JsRdd {
    // ── Transformations ──────────────────────────────────────────────────────

    /// Apply `f` to each element, returning a new RDD.
    #[napi]
    pub fn map(
        &mut self,
        f: Function<serde_json::Value, serde_json::Value>,
    ) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(format!("(partition) => partition.map((x) => ({})(x))", Self::fn_to_source(&f)?), TaskAction::Map)?;
            return Ok(self.take_as_new());
        }
        let elements = self.elements.iter()
            .map(|elem| f.call(elem.clone()))
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Keep only elements for which `f` returns truthy.
    #[napi]
    pub fn filter(
        &mut self,
        f: Function<serde_json::Value, bool>,
    ) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(format!("(partition) => partition.filter((x) => ({})(x))", Self::fn_to_source(&f)?), TaskAction::Filter)?;
            return Ok(self.take_as_new());
        }
        let mut elements = Vec::new();
        for elem in &self.elements {
            if f.call(elem.clone())? {
                elements.push(elem.clone());
            }
        }
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Apply `f` to each element and flatten (f must return an Array).
    #[napi]
    pub fn flat_map(
        &mut self,
        f: Function<serde_json::Value, Vec<serde_json::Value>>,
    ) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(format!("(partition) => partition.flatMap((x) => ({})(x))", Self::fn_to_source(&f)?), TaskAction::FlatMap)?;
            return Ok(self.take_as_new());
        }
        let mut elements = Vec::new();
        for elem in &self.elements {
            let result = f.call(elem.clone())
                .map_err(|e| Error::from_reason(format!("flat_map: f must return an Array: {e}")))?;
            elements.extend(result);
        }
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Apply `f` only to the value in each `[key, value]` pair.
    #[napi]
    pub fn map_values(
        &mut self,
        f: Function<serde_json::Value, serde_json::Value>,
    ) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(format!("(partition) => partition.map((p) => [p[0], ({})(p[1])])", Self::fn_to_source(&f)?), TaskAction::Map)?;
            return Ok(self.take_as_new());
        }
        let elements = self.elements.iter()
            .map(|elem| {
                let pair = elem.as_array()
                    .ok_or_else(|| Error::from_reason("map_values requires [key, value] arrays"))?;
                if pair.len() != 2 {
                    return Err(Error::from_reason("map_values requires 2-element arrays"));
                }
                let key = pair[0].clone();
                let new_val = f.call(pair[1].clone())?;
                Ok(serde_json::json!([key, new_val]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Apply `f` to each value in `[key, value]` pairs and flatten.
    #[napi]
    pub fn flat_map_values(
        &mut self,
        f: Function<serde_json::Value, Vec<serde_json::Value>>,
    ) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(format!("(partition) => partition.flatMap((p) => ({})(p[1]).map((v) => [p[0], v]))", Self::fn_to_source(&f)?), TaskAction::FlatMap)?;
            return Ok(self.take_as_new());
        }
        let mut elements = Vec::new();
        for elem in &self.elements {
            let pair = elem.as_array()
                .ok_or_else(|| Error::from_reason("flat_map_values requires [key, value] arrays"))?;
            if pair.len() != 2 {
                return Err(Error::from_reason("flat_map_values requires 2-element arrays"));
            }
            let key = pair[0].clone();
            let vals = f.call(pair[1].clone())?;
            for val in vals {
                elements.push(serde_json::json!([key, val]));
            }
        }
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Produce `[f(element), element]` pairs.
    #[napi]
    pub fn key_by(
        &mut self,
        f: Function<serde_json::Value, serde_json::Value>,
    ) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(format!("(partition) => partition.map((x) => [({})(x), x])", Self::fn_to_source(&f)?), TaskAction::Map)?;
            return Ok(self.take_as_new());
        }
        let elements = self.elements.iter()
            .map(|elem| {
                let key = f.call(elem.clone())?;
                Ok(serde_json::json!([key, elem]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Group elements by `f(element)` → `[key, [elements]]` pairs.
    #[napi]
    pub fn group_by(
        &mut self,
        f: Function<serde_json::Value, serde_json::Value>,
    ) -> Result<JsRdd> {
        let keyed = self.key_by(f)?;
        keyed.group_by_key()
    }

    /// Group `[key, value]` pairs by key → `[key, [values]]` pairs.
    #[napi]
    pub fn group_by_key(&self) -> Result<JsRdd> {
        let mut groups: HashMap<String, (serde_json::Value, Vec<serde_json::Value>)> =
            HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for elem in &self.elements {
            let pair = elem.as_array()
                .ok_or_else(|| Error::from_reason("group_by_key requires [key, value] arrays"))?;
            if pair.len() != 2 {
                return Err(Error::from_reason("group_by_key requires 2-element arrays"));
            }
            let key = pair[0].clone();
            let val = pair[1].clone();
            let key_str = Self::key_to_string(&key)?;
            let entry = groups.entry(key_str.clone()).or_insert_with(|| {
                order.push(key_str);
                (key, Vec::new())
            });
            entry.1.push(val);
        }

        let elements = order.iter()
            .map(|k| {
                let (key, vals) = &groups[k];
                Ok(serde_json::json!([key, vals]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Aggregate values with the same key using `f(acc, value) => acc`.
    #[napi]
    pub fn reduce_by_key(
        &self,
        f: Function<FnArgs<(serde_json::Value, serde_json::Value)>, serde_json::Value>,
    ) -> Result<JsRdd> {
        let mut accum: HashMap<String, (serde_json::Value, serde_json::Value)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for elem in &self.elements {
            let pair = elem.as_array()
                .ok_or_else(|| Error::from_reason("reduce_by_key requires [key, value] arrays"))?;
            if pair.len() != 2 {
                return Err(Error::from_reason("reduce_by_key requires 2-element arrays"));
            }
            let key = pair[0].clone();
            let val = pair[1].clone();
            let key_str = Self::key_to_string(&key)?;
            match accum.get_mut(&key_str) {
                Some((_, acc)) => {
                    *acc = f.call(FnArgs::from((acc.clone(), val)))?;
                }
                None => {
                    order.push(key_str.clone());
                    accum.insert(key_str, (key, val));
                }
            }
        }

        let elements = order.iter()
            .map(|k| {
                let (key, val) = &accum[k];
                Ok(serde_json::json!([key, val]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Remove duplicate elements.
    #[napi]
    pub fn distinct(&self) -> JsRdd {
        let mut seen = std::collections::HashSet::new();
        let elements = self.elements.iter()
            .filter(|e| seen.insert(e.to_string()))
            .cloned()
            .collect();
        JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context))
    }

    /// Return elements in `self` that are not in `other`.
    #[napi]
    pub fn subtract(&self, other: &JsRdd) -> JsRdd {
        let other_set: std::collections::HashSet<String> =
            other.elements.iter().map(|e| e.to_string()).collect();
        let elements = self.elements.iter()
            .filter(|e| !other_set.contains(&e.to_string()))
            .cloned()
            .collect();
        JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context))
    }

    /// Return elements present in both `self` and `other` (no duplicates).
    #[napi]
    pub fn intersection(&self, other: &JsRdd) -> JsRdd {
        let other_set: std::collections::HashSet<String> =
            other.elements.iter().map(|e| e.to_string()).collect();
        let mut seen = std::collections::HashSet::new();
        let elements = self.elements.iter()
            .filter(|e| {
                let s = e.to_string();
                other_set.contains(&s) && seen.insert(s)
            })
            .cloned()
            .collect();
        JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context))
    }

    /// Apply `f` to each logical partition (array of elements), returning a flattened RDD.
    #[napi]
    pub fn map_partitions(
        &self,
        f: Function<Vec<serde_json::Value>, Vec<serde_json::Value>>,
    ) -> Result<JsRdd> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        let chunk_size = ((total + np - 1) / np).max(1);
        let mut elements = Vec::new();
        for chunk in self.elements.chunks(chunk_size) {
            let result = f.call(chunk.to_vec())?;
            elements.extend(result);
        }
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Extract the key from each `[key, value]` pair.
    #[napi]
    pub fn keys(&self) -> Result<JsRdd> {
        let elements = self.elements.iter()
            .map(|elem| {
                let pair = elem.as_array()
                    .ok_or_else(|| Error::from_reason("keys requires [key, value] arrays"))?;
                if pair.is_empty() {
                    return Err(Error::from_reason("keys requires non-empty arrays"));
                }
                Ok(pair[0].clone())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Extract the value from each `[key, value]` pair.
    #[napi]
    pub fn values(&self) -> Result<JsRdd> {
        let elements = self.elements.iter()
            .map(|elem| {
                let pair = elem.as_array()
                    .ok_or_else(|| Error::from_reason("values requires [key, value] arrays"))?;
                if pair.len() < 2 {
                    return Err(Error::from_reason("values requires 2-element arrays"));
                }
                Ok(pair[1].clone())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Return all values associated with `key` from a pair RDD.
    #[napi]
    pub fn lookup(&self, key: serde_json::Value) -> Result<Vec<serde_json::Value>> {
        let key_str = Self::key_to_string(&key)?;
        let vals = self.elements.iter()
            .filter_map(|elem| {
                let pair = elem.as_array()?;
                if pair.len() != 2 { return None; }
                let k = Self::key_to_string(&pair[0]).ok()?;
                if k == key_str { Some(pair[1].clone()) } else { None }
            })
            .collect();
        Ok(vals)
    }

    /// Merge two RDDs into one.
    #[napi]
    pub fn union(&self, other: &JsRdd) -> JsRdd {
        let mut elements = self.elements.clone();
        elements.extend_from_slice(&other.elements);
        JsRdd::from_data(
            elements,
            self.num_partitions + other.num_partitions,
            Arc::clone(&self.context),
        )
    }

    /// Zip two equal-length RDDs into an RDD of `[a, b]` pairs.
    #[napi]
    pub fn zip(&self, other: &JsRdd) -> Result<JsRdd> {
        if self.elements.len() != other.elements.len() {
            return Err(Error::from_reason(format!(
                "zip requires equal-length RDDs: {} vs {}",
                self.elements.len(),
                other.elements.len()
            )));
        }
        let elements = self.elements.iter()
            .zip(other.elements.iter())
            .map(|(a, b)| Ok(serde_json::json!([a, b])))
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Compute the Cartesian product of two RDDs as `[a, b]` pairs.
    #[napi]
    pub fn cartesian(&self, other: &JsRdd) -> JsRdd {
        let mut elements = Vec::new();
        for a in &self.elements {
            for b in &other.elements {
                elements.push(serde_json::json!([a, b]));
            }
        }
        let partitions = self.num_partitions * other.num_partitions.max(1);
        JsRdd::from_data(elements, partitions, Arc::clone(&self.context))
    }

    /// Reduce to `n` logical partitions.
    #[napi]
    pub fn coalesce(&self, n: u32) -> JsRdd {
        JsRdd::from_data(
            self.elements.clone(),
            (n as usize).max(1),
            Arc::clone(&self.context),
        )
    }

    /// Change partition count (alias for `coalesce`).
    #[napi]
    pub fn repartition(&self, n: u32) -> JsRdd {
        self.coalesce(n)
    }

    // ── Actions ──────────────────────────────────────────────────────────────

    /// Return all elements as a JavaScript array.
    #[napi]
    pub fn collect(&mut self) -> Result<Vec<serde_json::Value>> {
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
    pub fn first(&self) -> Result<serde_json::Value> {
        self.elements.first()
            .cloned()
            .ok_or_else(|| Error::from_reason("first: RDD is empty"))
    }

    /// Return the first `n` elements.
    #[napi]
    pub fn take(&self, n: u32) -> Vec<serde_json::Value> {
        self.elements.iter().take(n as usize).cloned().collect()
    }

    /// Aggregate all elements using `f(acc, element) => acc`. Throws if empty.
    #[napi]
    pub fn reduce(
        &mut self,
        f: Function<FnArgs<(serde_json::Value, serde_json::Value)>, serde_json::Value>,
    ) -> Result<serde_json::Value> {
        if self.context.is_distributed() {
            let fn_src = Self::fn_to_source(&f)?;
            let partition_fn = format!("(partition) => partition.length === 0 ? [] : [partition.reduce(({}))]", fn_src);
            self.stage_js_udf(partition_fn, TaskAction::Reduce)?;
            let staged = self.staged.as_ref()
                .ok_or_else(|| Error::from_reason("no staged pipeline"))?;
            let result_bytes = self.context
                .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
                .map_err(|e| Error::from_reason(format!("dispatch_pipeline: {e}")))?;

            let mut acc: Option<serde_json::Value> = None;
            for bytes in result_bytes {
                let items: Vec<serde_json::Value> = serde_json::from_slice(&bytes)
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
        let first = iter.next().ok_or_else(|| Error::from_reason("reduce on empty RDD"))?;
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
        zero: serde_json::Value,
        f: Function<FnArgs<(serde_json::Value, serde_json::Value)>, serde_json::Value>,
    ) -> Result<serde_json::Value> {
        if self.context.is_distributed() {
            let zero_json = zero.to_string();
            let fn_src = Self::fn_to_source(&f)?;
            let partition_fn = format!("(partition) => [partition.reduce(({})  , {})]", fn_src, zero_json);
            self.stage_js_udf(partition_fn, TaskAction::Fold)?;
            let staged = self.staged.as_ref()
                .ok_or_else(|| Error::from_reason("no staged pipeline"))?;
            let result_bytes = self.context
                .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
                .map_err(|e| Error::from_reason(format!("dispatch_pipeline: {e}")))?;

            let mut acc = zero;
            for bytes in result_bytes {
                let items: Vec<serde_json::Value> = serde_json::from_slice(&bytes)
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
    pub fn for_each(
        &self,
        f: Function<serde_json::Value, serde_json::Value>,
    ) -> Result<()> {
        for elem in &self.elements {
            f.call(elem.clone())?;
        }
        Ok(())
    }

    /// Apply `f` to each logical partition for side effects.
    #[napi]
    pub fn for_each_partition(
        &self,
        f: Function<Vec<serde_json::Value>, serde_json::Value>,
    ) -> Result<()> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        let chunk_size = ((total + np - 1) / np).max(1);
        for chunk in self.elements.chunks(chunk_size) {
            f.call(chunk.to_vec())?;
        }
        Ok(())
    }

    /// Count occurrences of each distinct element. Returns `{element: count}`.
    #[napi]
    pub fn count_by_value(&self) -> serde_json::Value {
        let mut counts: HashMap<String, u64> = HashMap::new();
        for elem in &self.elements {
            *counts.entry(elem.to_string()).or_insert(0) += 1;
        }
        serde_json::to_value(counts).unwrap_or(serde_json::Value::Object(Default::default()))
    }

    /// Count elements per key in a pair RDD. Returns `{key: count}`.
    #[napi]
    pub fn count_by_key(&self) -> Result<serde_json::Value> {
        let mut counts: HashMap<String, u64> = HashMap::new();
        for elem in &self.elements {
            let pair = elem.as_array()
                .ok_or_else(|| Error::from_reason("count_by_key requires [key, value] arrays"))?;
            if pair.is_empty() {
                return Err(Error::from_reason("count_by_key requires non-empty arrays"));
            }
            let key = Self::key_to_string(&pair[0])?;
            *counts.entry(key).or_insert(0) += 1;
        }
        Ok(serde_json::to_value(counts).unwrap_or(serde_json::Value::Object(Default::default())))
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
        comparator: Option<Function<FnArgs<(serde_json::Value, serde_json::Value)>, f64>>,
    ) -> Result<serde_json::Value> {
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
        comparator: Option<Function<FnArgs<(serde_json::Value, serde_json::Value)>, f64>>,
    ) -> Result<serde_json::Value> {
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

    /// Return the top `n` elements (largest first). Optional comparator `f(a, b) => number`.
    #[napi]
    pub fn top(
        &self,
        n: u32,
        comparator: Option<Function<FnArgs<(serde_json::Value, serde_json::Value)>, f64>>,
    ) -> Result<Vec<serde_json::Value>> {
        let mut sorted = self.elements.clone();
        if let Some(ref cmp) = comparator {
            let mut sort_error: Option<Error> = None;
            sorted.sort_by(|a, b| {
                if sort_error.is_some() { return std::cmp::Ordering::Equal; }
                match cmp.call(FnArgs::from((b.clone(), a.clone()))) {
                    Ok(v) => {
                        if v > 0.0 { std::cmp::Ordering::Greater }
                        else if v < 0.0 { std::cmp::Ordering::Less }
                        else { std::cmp::Ordering::Equal }
                    }
                    Err(e) => { sort_error = Some(e); std::cmp::Ordering::Equal }
                }
            });
            if let Some(e) = sort_error { return Err(e); }
        } else {
            sorted.sort_by(|a, b| Self::json_compare(b, a));
        }
        Ok(sorted.into_iter().take(n as usize).collect())
    }

    /// Return the `n` smallest elements. Optional comparator `f(a, b) => number`.
    #[napi]
    pub fn take_ordered(
        &self,
        n: u32,
        comparator: Option<Function<FnArgs<(serde_json::Value, serde_json::Value)>, f64>>,
    ) -> Result<Vec<serde_json::Value>> {
        let mut sorted = self.elements.clone();
        if let Some(ref cmp) = comparator {
            let mut sort_error: Option<Error> = None;
            sorted.sort_by(|a, b| {
                if sort_error.is_some() { return std::cmp::Ordering::Equal; }
                match cmp.call(FnArgs::from((a.clone(), b.clone()))) {
                    Ok(v) => {
                        if v > 0.0 { std::cmp::Ordering::Greater }
                        else if v < 0.0 { std::cmp::Ordering::Less }
                        else { std::cmp::Ordering::Equal }
                    }
                    Err(e) => { sort_error = Some(e); std::cmp::Ordering::Equal }
                }
            });
            if let Some(e) = sort_error { return Err(e); }
        } else {
            sorted.sort_by(Self::json_compare);
        }
        Ok(sorted.into_iter().take(n as usize).collect())
    }

    /// Write each element as a line to `path`.
    #[napi]
    pub fn save_as_text_file(&self, path: String) -> Result<()> {
        use std::io::Write;
        let mut file = std::fs::File::create(&path)
            .map_err(|e| Error::from_reason(format!("save_as_text_file: {e}")))?;
        for elem in &self.elements {
            let line = match elem {
                serde_json::Value::String(s) => s.clone(),
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
        zero: serde_json::Value,
        seq_fn: Function<FnArgs<(serde_json::Value, serde_json::Value)>, serde_json::Value>,
        comb_fn: Function<FnArgs<(serde_json::Value, serde_json::Value)>, serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        let chunk_size = ((total + np - 1) / np).max(1);

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
