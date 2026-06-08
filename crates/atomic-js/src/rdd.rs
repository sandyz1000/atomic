use std::collections::HashMap;
use std::sync::Arc;

use napi::JsValue as _;
use napi::bindgen_prelude::*;
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


impl JsRdd {
    pub fn from_data(
        elements: Vec<serde_json::Value>,
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
            self.staged = Some(StagedJsPipeline {
                source_partitions,
                ops: vec![op],
            });
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
            other => Err(Error::from_reason(format!("Unsupported key type: {other}"))),
        }
    }

    /// Default JSON value comparison for ordering operations.
    fn json_compare(a: &serde_json::Value, b: &serde_json::Value) -> std::cmp::Ordering {
        match (a, b) {
            (serde_json::Value::Number(an), serde_json::Value::Number(bn)) => an
                .as_f64()
                .unwrap_or(0.0)
                .partial_cmp(&bn.as_f64().unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal),
            (serde_json::Value::String(as_), serde_json::Value::String(bs)) => as_.cmp(bs),
            _ => a.to_string().cmp(&b.to_string()),
        }
    }
}


#[napi]
impl JsRdd {

    /// Apply `f` to each element, returning a new RDD.
    #[napi]
    pub fn map(&mut self, f: Function<serde_json::Value, serde_json::Value>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!(
                    "(partition) => partition.map((x) => ({})(x))",
                    Self::fn_to_source(&f)?
                ),
                TaskAction::Map,
            )?;
            return Ok(self.take_as_new());
        }
        let elements = self
            .elements
            .iter()
            .map(|elem| f.call(elem.clone()))
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Keep only elements for which `f` returns truthy.
    #[napi]
    pub fn filter(&mut self, f: Function<serde_json::Value, bool>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!(
                    "(partition) => partition.filter((x) => ({})(x))",
                    Self::fn_to_source(&f)?
                ),
                TaskAction::Filter,
            )?;
            return Ok(self.take_as_new());
        }
        let mut elements = Vec::new();
        for elem in &self.elements {
            if f.call(elem.clone())? {
                elements.push(elem.clone());
            }
        }
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Apply `f` to each element and flatten (f must return an Array).
    #[napi]
    pub fn flat_map(
        &mut self,
        f: Function<serde_json::Value, Vec<serde_json::Value>>,
    ) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!(
                    "(partition) => partition.flatMap((x) => ({})(x))",
                    Self::fn_to_source(&f)?
                ),
                TaskAction::FlatMap,
            )?;
            return Ok(self.take_as_new());
        }
        let mut elements = Vec::new();
        for elem in &self.elements {
            let result = f.call(elem.clone()).map_err(|e| {
                Error::from_reason(format!("flat_map: f must return an Array: {e}"))
            })?;
            elements.extend(result);
        }
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Apply `f` only to the value in each `[key, value]` pair.
    #[napi]
    pub fn map_values(
        &mut self,
        f: Function<serde_json::Value, serde_json::Value>,
    ) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!(
                    "(partition) => partition.map((p) => [p[0], ({})(p[1])])",
                    Self::fn_to_source(&f)?
                ),
                TaskAction::Map,
            )?;
            return Ok(self.take_as_new());
        }
        let elements = self
            .elements
            .iter()
            .map(|elem| {
                let pair = elem
                    .as_array()
                    .ok_or_else(|| Error::from_reason("map_values requires [key, value] arrays"))?;
                if pair.len() != 2 {
                    return Err(Error::from_reason("map_values requires 2-element arrays"));
                }
                let key = pair[0].clone();
                let new_val = f.call(pair[1].clone())?;
                Ok(serde_json::json!([key, new_val]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Apply `f` to each value in `[key, value]` pairs and flatten.
    #[napi]
    pub fn flat_map_values(
        &mut self,
        f: Function<serde_json::Value, Vec<serde_json::Value>>,
    ) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!(
                    "(partition) => partition.flatMap((p) => ({})(p[1]).map((v) => [p[0], v]))",
                    Self::fn_to_source(&f)?
                ),
                TaskAction::FlatMap,
            )?;
            return Ok(self.take_as_new());
        }
        let mut elements = Vec::new();
        for elem in &self.elements {
            let pair = elem.as_array().ok_or_else(|| {
                Error::from_reason("flat_map_values requires [key, value] arrays")
            })?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "flat_map_values requires 2-element arrays",
                ));
            }
            let key = pair[0].clone();
            let vals = f.call(pair[1].clone())?;
            for val in vals {
                elements.push(serde_json::json!([key, val]));
            }
        }
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Produce `[f(element), element]` pairs.
    #[napi]
    pub fn key_by(&mut self, f: Function<serde_json::Value, serde_json::Value>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!(
                    "(partition) => partition.map((x) => [({})(x), x])",
                    Self::fn_to_source(&f)?
                ),
                TaskAction::Map,
            )?;
            return Ok(self.take_as_new());
        }
        let elements = self
            .elements
            .iter()
            .map(|elem| {
                let key = f.call(elem.clone())?;
                Ok(serde_json::json!([key, elem]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Group elements by `f(element)` → `[key, [elements]]` pairs.
    #[napi]
    pub fn group_by(&mut self, f: Function<serde_json::Value, serde_json::Value>) -> Result<JsRdd> {
        let mut keyed = self.key_by(f)?;
        keyed.group_by_key()
    }

    /// Group `[key, value]` pairs by key → `[key, [values]]` pairs.
    #[napi]
    pub fn group_by_key(&mut self) -> Result<JsRdd> {
        if self.context.is_distributed() {
            // Per-partition partial grouping dispatched to workers; driver merges.
            let wrapper = "(partition) => { const g = new Map(), o = []; \
                for (const p of partition) { const k = JSON.stringify(p[0]); \
                if (!g.has(k)) { g.set(k, [p[0], []]); o.push(k); } \
                g.get(k)[1].push(p[1]); } \
                return o.map(k => g.get(k)); }"
                .to_string();
            self.stage_js_udf(wrapper, TaskAction::Map)?;
            let partials = self.dispatch_and_collect()?;

            let mut groups: HashMap<String, (serde_json::Value, Vec<serde_json::Value>)> =
                HashMap::new();
            let mut order: Vec<String> = Vec::new();
            for pair in &partials {
                let arr = pair
                    .as_array()
                    .ok_or_else(|| Error::from_reason("group_by_key partial: not an array"))?;
                if arr.len() != 2 {
                    return Err(Error::from_reason(
                        "group_by_key partial: expected [key, vals]",
                    ));
                }
                let key = arr[0].clone();
                let vals = arr[1]
                    .as_array()
                    .ok_or_else(|| Error::from_reason("group_by_key partial: vals not an array"))?;
                let key_str = Self::key_to_string(&key)?;
                let entry = groups.entry(key_str.clone()).or_insert_with(|| {
                    order.push(key_str.clone());
                    (key, Vec::new())
                });
                entry.1.extend(vals.iter().cloned());
            }

            let elements = order
                .iter()
                .map(|k| {
                    let (key, vals) = &groups[k];
                    Ok(serde_json::json!([key, vals]))
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(JsRdd::from_data(
                elements,
                self.num_partitions,
                Arc::clone(&self.context),
            ));
        }

        let mut groups: HashMap<String, (serde_json::Value, Vec<serde_json::Value>)> =
            HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for elem in &self.elements {
            let pair = elem
                .as_array()
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

        let elements = order
            .iter()
            .map(|k| {
                let (key, vals) = &groups[k];
                Ok(serde_json::json!([key, vals]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Aggregate values with the same key using `f(acc, value) => acc`.
    #[napi]
    pub fn reduce_by_key(
        &mut self,
        f: Function<FnArgs<(serde_json::Value, serde_json::Value)>, serde_json::Value>,
    ) -> Result<JsRdd> {
        if self.context.is_distributed() {
            let fn_src = Self::fn_to_source(&f)?;
            // Per-partition partial reduce dispatched to workers; driver merges.
            let wrapper = format!(
                "(partition) => {{ const g = new Map(), o = []; \
                 for (const p of partition) {{ const k = JSON.stringify(p[0]); \
                 if (g.has(k)) {{ g.set(k, [p[0], ({fn_src})(g.get(k)[1], p[1])]); }} \
                 else {{ g.set(k, [p[0], p[1]]); o.push(k); }} }} \
                 return o.map(k => g.get(k)); }}"
            );
            self.stage_js_udf(wrapper, TaskAction::Map)?;
            let partials = self.dispatch_and_collect()?;

            let mut accum: HashMap<String, (serde_json::Value, serde_json::Value)> = HashMap::new();
            let mut order: Vec<String> = Vec::new();
            for pair in &partials {
                let arr = pair
                    .as_array()
                    .ok_or_else(|| Error::from_reason("reduce_by_key partial: not an array"))?;
                if arr.len() != 2 {
                    return Err(Error::from_reason(
                        "reduce_by_key partial: expected [key, val]",
                    ));
                }
                let key = arr[0].clone();
                let val = arr[1].clone();
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

            let elements = order
                .iter()
                .map(|k| {
                    let (key, val) = &accum[k];
                    Ok(serde_json::json!([key, val]))
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(JsRdd::from_data(
                elements,
                self.num_partitions,
                Arc::clone(&self.context),
            ));
        }

        let mut accum: HashMap<String, (serde_json::Value, serde_json::Value)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for elem in &self.elements {
            let pair = elem
                .as_array()
                .ok_or_else(|| Error::from_reason("reduce_by_key requires [key, value] arrays"))?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "reduce_by_key requires 2-element arrays",
                ));
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

        let elements = order
            .iter()
            .map(|k| {
                let (key, val) = &accum[k];
                Ok(serde_json::json!([key, val]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Remove duplicate elements.
    #[napi]
    pub fn distinct(&self) -> JsRdd {
        let mut seen = std::collections::HashSet::new();
        let elements = self
            .elements
            .iter()
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
        let elements = self
            .elements
            .iter()
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
        let elements = self
            .elements
            .iter()
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
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Extract the key from each `[key, value]` pair.
    #[napi]
    pub fn keys(&self) -> Result<JsRdd> {
        let elements = self
            .elements
            .iter()
            .map(|elem| {
                let pair = elem
                    .as_array()
                    .ok_or_else(|| Error::from_reason("keys requires [key, value] arrays"))?;
                if pair.is_empty() {
                    return Err(Error::from_reason("keys requires non-empty arrays"));
                }
                Ok(pair[0].clone())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Extract the value from each `[key, value]` pair.
    #[napi]
    pub fn values(&self) -> Result<JsRdd> {
        let elements = self
            .elements
            .iter()
            .map(|elem| {
                let pair = elem
                    .as_array()
                    .ok_or_else(|| Error::from_reason("values requires [key, value] arrays"))?;
                if pair.len() < 2 {
                    return Err(Error::from_reason("values requires 2-element arrays"));
                }
                Ok(pair[1].clone())
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Return all values associated with `key` from a pair RDD.
    #[napi]
    pub fn lookup(&self, key: serde_json::Value) -> Result<Vec<serde_json::Value>> {
        let key_str = Self::key_to_string(&key)?;
        let vals = self
            .elements
            .iter()
            .filter_map(|elem| {
                let pair = elem.as_array()?;
                if pair.len() != 2 {
                    return None;
                }
                let k = Self::key_to_string(&pair[0]).ok()?;
                if k == key_str {
                    Some(pair[1].clone())
                } else {
                    None
                }
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
        let elements = self
            .elements
            .iter()
            .zip(other.elements.iter())
            .map(|(a, b)| Ok(serde_json::json!([a, b])))
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
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
        self.elements
            .first()
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
            let partition_fn = format!(
                "(partition) => partition.length === 0 ? [] : [partition.reduce(({}))]",
                fn_src
            );
            self.stage_js_udf(partition_fn, TaskAction::Reduce)?;
            let staged = self
                .staged
                .as_ref()
                .ok_or_else(|| Error::from_reason("no staged pipeline"))?;
            let result_bytes = self
                .context
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
        zero: serde_json::Value,
        f: Function<FnArgs<(serde_json::Value, serde_json::Value)>, serde_json::Value>,
    ) -> Result<serde_json::Value> {
        if self.context.is_distributed() {
            let zero_json = zero.to_string();
            let fn_src = Self::fn_to_source(&f)?;
            let partition_fn = format!(
                "(partition) => [partition.reduce(({})  , {})]",
                fn_src, zero_json
            );
            self.stage_js_udf(partition_fn, TaskAction::Fold)?;
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
    pub fn for_each(&self, f: Function<serde_json::Value, serde_json::Value>) -> Result<()> {
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
            let pair = elem
                .as_array()
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
                if sort_error.is_some() {
                    return std::cmp::Ordering::Equal;
                }
                match cmp.call(FnArgs::from((b.clone(), a.clone()))) {
                    Ok(v) => {
                        if v > 0.0 {
                            std::cmp::Ordering::Greater
                        } else if v < 0.0 {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Equal
                        }
                    }
                    Err(e) => {
                        sort_error = Some(e);
                        std::cmp::Ordering::Equal
                    }
                }
            });
            if let Some(e) = sort_error {
                return Err(e);
            }
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
                if sort_error.is_some() {
                    return std::cmp::Ordering::Equal;
                }
                match cmp.call(FnArgs::from((a.clone(), b.clone()))) {
                    Ok(v) => {
                        if v > 0.0 {
                            std::cmp::Ordering::Greater
                        } else if v < 0.0 {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Equal
                        }
                    }
                    Err(e) => {
                        sort_error = Some(e);
                        std::cmp::Ordering::Equal
                    }
                }
            });
            if let Some(e) = sort_error {
                return Err(e);
            }
        } else {
            sorted.sort_by(Self::json_compare);
        }
        Ok(sorted.into_iter().take(n as usize).collect())
    }

    /// Write each element as a line to `path`.
    ///
    /// Accepts a local file path or, when built with the `s3` feature, an
    /// S3 URI (`s3://bucket/prefix`).  S3 writes upload a single `part-0`
    /// object under the given prefix.
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
                        serde_json::Value::String(s) => s.clone(),
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


    /// Inner join two pair RDDs on their first element (key).
    ///
    /// Both RDDs must contain `[key, value]` arrays.
    /// Emits `[key, [left_value, right_value]]` for each matching key pair.
    #[napi]
    pub fn join(&self, other: &JsRdd) -> Result<JsRdd> {
        let mut right_map: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for elem in &other.elements {
            let pair = elem.as_array().ok_or_else(|| {
                Error::from_reason("join: right RDD requires [key, value] arrays")
            })?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "join: right RDD requires 2-element arrays",
                ));
            }
            let key_str = Self::key_to_string(&pair[0])?;
            right_map.entry(key_str).or_default().push(pair[1].clone());
        }

        let mut elements = Vec::new();
        for elem in &self.elements {
            let pair = elem
                .as_array()
                .ok_or_else(|| Error::from_reason("join: left RDD requires [key, value] arrays"))?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "join: left RDD requires 2-element arrays",
                ));
            }
            let key = &pair[0];
            let left_val = &pair[1];
            let key_str = Self::key_to_string(key)?;
            if let Some(right_vals) = right_map.get(&key_str) {
                for right_val in right_vals {
                    elements.push(serde_json::json!([key, [left_val, right_val]]));
                }
            }
        }
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Left outer join two pair RDDs on their first element (key).
    ///
    /// Both RDDs must contain `[key, value]` arrays.
    /// Emits `[key, [left_value, right_value]]` for matched keys and
    /// `[key, [left_value, null]]` for unmatched left keys.
    #[napi]
    pub fn left_outer_join(&self, other: &JsRdd) -> Result<JsRdd> {
        let mut right_map: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for elem in &other.elements {
            let pair = elem.as_array().ok_or_else(|| {
                Error::from_reason("left_outer_join: right RDD requires [key, value] arrays")
            })?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "left_outer_join: right RDD requires 2-element arrays",
                ));
            }
            let key_str = Self::key_to_string(&pair[0])?;
            right_map.entry(key_str).or_default().push(pair[1].clone());
        }

        let mut elements = Vec::new();
        for elem in &self.elements {
            let pair = elem.as_array().ok_or_else(|| {
                Error::from_reason("left_outer_join: left RDD requires [key, value] arrays")
            })?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "left_outer_join: left RDD requires 2-element arrays",
                ));
            }
            let key = &pair[0];
            let left_val = &pair[1];
            let key_str = Self::key_to_string(key)?;
            match right_map.get(&key_str) {
                Some(right_vals) => {
                    for right_val in right_vals {
                        elements.push(serde_json::json!([key, [left_val, right_val]]));
                    }
                }
                None => {
                    elements.push(serde_json::json!([
                        key,
                        [left_val, serde_json::Value::Null]
                    ]));
                }
            }
        }
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }


    /// Sort elements by a key extracted via `key_fn(element)`.
    ///
    /// `ascending` defaults to `true`. The key function is called on each
    /// element; results are compared using JSON ordering (numbers numerically,
    /// strings lexicographically).
    #[napi]
    pub fn sort_by(
        &self,
        key_fn: Function<serde_json::Value, serde_json::Value>,
        ascending: Option<bool>,
    ) -> Result<JsRdd> {
        let asc = ascending.unwrap_or(true);
        // Compute (key, element) pairs so the key function is called once per element.
        let mut keyed: Vec<(serde_json::Value, serde_json::Value)> = self
            .elements
            .iter()
            .map(|elem| {
                let key = key_fn.call(elem.clone())?;
                Ok((key, elem.clone()))
            })
            .collect::<Result<Vec<_>>>()?;

        keyed.sort_by(|(ka, _), (kb, _)| {
            let ord = Self::json_compare(ka, kb);
            if asc { ord } else { ord.reverse() }
        });

        let elements = keyed.into_iter().map(|(_, v)| v).collect();
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Sort `[key, value]` pair elements by their first element (key).
    ///
    /// `ascending` defaults to `true`. Keys are compared using JSON ordering.
    #[napi]
    pub fn sort_by_key(&self, ascending: Option<bool>) -> Result<JsRdd> {
        let asc = ascending.unwrap_or(true);
        let mut elements = self.elements.clone();
        let mut sort_error: Option<Error> = None;
        elements.sort_by(|a, b| {
            if sort_error.is_some() {
                return std::cmp::Ordering::Equal;
            }
            let ka = match a.as_array() {
                Some(arr) if !arr.is_empty() => &arr[0],
                _ => {
                    sort_error = Some(Error::from_reason(
                        "sort_by_key: elements must be [key, value] arrays",
                    ));
                    return std::cmp::Ordering::Equal;
                }
            };
            let kb = match b.as_array() {
                Some(arr) if !arr.is_empty() => &arr[0],
                _ => {
                    sort_error = Some(Error::from_reason(
                        "sort_by_key: elements must be [key, value] arrays",
                    ));
                    return std::cmp::Ordering::Equal;
                }
            };
            let ord = Self::json_compare(ka, kb);
            if asc { ord } else { ord.reverse() }
        });
        if let Some(e) = sort_error {
            return Err(e);
        }
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }


    /// Return elements divided into `num_partitions` logical partition arrays.
    ///
    /// Returns a nested JavaScript array, one inner array per partition.
    #[napi]
    pub fn glom(&self) -> Vec<Vec<serde_json::Value>> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        let chunk_size = ((total + np - 1) / np).max(1);
        let mut result: Vec<Vec<serde_json::Value>> = self
            .elements
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();
        // Pad with empty partitions if there are fewer elements than partitions.
        while result.len() < np {
            result.push(Vec::new());
        }
        result
    }


    /// Mark this RDD to be cached in memory on first action.
    ///
    /// In local mode this is a no-op — returns a clone with the same elements.
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
