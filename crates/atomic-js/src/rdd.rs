use std::collections::HashMap;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi::{Env, JsFunction, JsUnknown};
use napi_derive::napi;

use atomic_compute::context::Context;
use atomic_data::distributed::{JsUdfPayload, PipelineOp, TaskAction, UdfAction};

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

    /// Capture the source string of a JS function via `String(f)` coercion.
    ///
    /// SAFETY: `napi_coerce_to_string` reads the input napi_value without invalidating it;
    /// only the returned new napi_value (the string result) is distinct. Creating a
    /// temporary `JsUnknown` view of the same raw pointer is safe while `f` is alive.
    fn fn_to_source(env: &Env, f: &JsFunction) -> Result<String> {
        use napi::{NapiRaw, NapiValue};
        let f_view: JsUnknown = unsafe { JsUnknown::from_raw(env.raw(), f.raw())? };
        f_view
            .coerce_to_string()?
            .into_utf8()?
            .into_owned()
            .map_err(|e| Error::from_reason(format!("fn_to_source: {e}")))
    }

    /// Call a JS function with a single JSON value argument, returning a JSON value.
    fn call_fn(env: &Env, f: &JsFunction, arg: &serde_json::Value) -> Result<serde_json::Value> {
        let js_arg: JsUnknown = env.to_js_value(arg)?;
        let result: JsUnknown = f.call(None, &[js_arg])?;
        env.from_js_value(result)
    }

    /// Call a JS function with two JSON value arguments (for reduce/fold).
    fn call_fn2(
        env: &Env,
        f: &JsFunction,
        acc: &serde_json::Value,
        val: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let js_acc: JsUnknown = env.to_js_value(acc)?;
        let js_val: JsUnknown = env.to_js_value(val)?;
        let result: JsUnknown = f.call(None, &[js_acc, js_val])?;
        env.from_js_value(result)
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
    fn stage_js_udf(&mut self, fn_source: String, action: UdfAction, zero_json: &str) -> Result<()> {
        let payload_struct = JsUdfPayload { fn_source, zero_json: zero_json.to_string() };
        let payload = serde_json::to_vec(&payload_struct)
            .map_err(|e| Error::from_reason(format!("JsUdfPayload encode: {e}")))?;
        let op = PipelineOp {
            op_id: "atomic::udf::js".to_string(),
            action: TaskAction::JavaScriptUdf(action),
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
}

// ── napi methods ─────────────────────────────────────────────────────────────

#[napi]
impl JsRdd {
    // ── Transformations ──────────────────────────────────────────────────────

    /// Apply `f` to each element, returning a new RDD.
    #[napi]
    pub fn map(&mut self, env: Env, f: JsFunction) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(Self::fn_to_source(&env, &f)?, UdfAction::Map, "")?;
            return Ok(self.take_as_new());
        }
        let elements = self.elements.iter()
            .map(|elem| Self::call_fn(&env, &f, elem))
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Keep only elements for which `f` returns truthy.
    #[napi]
    pub fn filter(&mut self, env: Env, f: JsFunction) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(Self::fn_to_source(&env, &f)?, UdfAction::Filter, "")?;
            return Ok(self.take_as_new());
        }
        let mut elements = Vec::new();
        for elem in &self.elements {
            let keep: bool = {
                let js_arg: JsUnknown = env.to_js_value(elem)?;
                let result: JsUnknown = f.call(None, &[js_arg])?;
                env.from_js_value(result)?
            };
            if keep {
                elements.push(elem.clone());
            }
        }
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Apply `f` to each element and flatten (f must return an Array).
    #[napi]
    pub fn flat_map(&mut self, env: Env, f: JsFunction) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(Self::fn_to_source(&env, &f)?, UdfAction::FlatMap, "")?;
            return Ok(self.take_as_new());
        }
        let mut elements = Vec::new();
        for elem in &self.elements {
            let result: Vec<serde_json::Value> = {
                let js_arg: JsUnknown = env.to_js_value(elem)?;
                let result: JsUnknown = f.call(None, &[js_arg])?;
                env.from_js_value(result)
                    .map_err(|e| Error::from_reason(format!("flat_map: f must return an Array: {e}")))?
            };
            elements.extend(result);
        }
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Apply `f` only to the value in each `[key, value]` pair.
    #[napi]
    pub fn map_values(&mut self, env: Env, f: JsFunction) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(Self::fn_to_source(&env, &f)?, UdfAction::MapValues, "")?;
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
                let new_val = Self::call_fn(&env, &f, &pair[1])?;
                Ok(serde_json::json!([key, new_val]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Apply `f` to each value in `[key, value]` pairs and flatten.
    #[napi]
    pub fn flat_map_values(&mut self, env: Env, f: JsFunction) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(Self::fn_to_source(&env, &f)?, UdfAction::FlatMapValues, "")?;
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
            let vals: Vec<serde_json::Value> = {
                let js_arg: JsUnknown = env.to_js_value(&pair[1])?;
                let result: JsUnknown = f.call(None, &[js_arg])?;
                env.from_js_value(result)?
            };
            for val in vals {
                elements.push(serde_json::json!([key, val]));
            }
        }
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Produce `[f(element), element]` pairs.
    #[napi]
    pub fn key_by(&mut self, env: Env, f: JsFunction) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(Self::fn_to_source(&env, &f)?, UdfAction::KeyBy, "")?;
            return Ok(self.take_as_new());
        }
        let elements = self.elements.iter()
            .map(|elem| {
                let key = Self::call_fn(&env, &f, elem)?;
                Ok(serde_json::json!([key, elem]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(elements, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Group elements by `f(element)` → `[key, [elements]]` pairs.
    #[napi]
    pub fn group_by(&mut self, env: Env, f: JsFunction) -> Result<JsRdd> {
        let keyed = self.key_by(env, f)?;
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
    pub fn reduce_by_key(&self, env: Env, f: JsFunction) -> Result<JsRdd> {
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
                    *acc = Self::call_fn2(&env, &f, acc, &val)?;
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
    pub fn reduce(&mut self, env: Env, f: JsFunction) -> Result<serde_json::Value> {
        if self.context.is_distributed() {
            self.stage_js_udf(Self::fn_to_source(&env, &f)?, UdfAction::Reduce, "")?;
            let result_bytes = self.staged.as_ref()
                .ok_or_else(|| Error::from_reason("no staged pipeline"))?;
            let result_bytes = self.context
                .dispatch_pipeline(
                    result_bytes.source_partitions.clone(),
                    result_bytes.ops.clone(),
                )
                .map_err(|e| Error::from_reason(format!("dispatch_pipeline: {e}")))?;

            let mut acc: Option<serde_json::Value> = None;
            for bytes in result_bytes {
                let items: Vec<serde_json::Value> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::from_reason(format!("reduce decode: {e}")))?;
                for item in items {
                    acc = Some(match acc {
                        None => item,
                        Some(a) => Self::call_fn2(&env, &f, &a, &item)?,
                    });
                }
            }
            return acc.ok_or_else(|| Error::from_reason("reduce on empty RDD"));
        }
        let mut iter = self.elements.iter();
        let first = iter.next().ok_or_else(|| Error::from_reason("reduce on empty RDD"))?;
        let mut acc = first.clone();
        for val in iter {
            acc = Self::call_fn2(&env, &f, &acc, val)?;
        }
        Ok(acc)
    }

    /// Aggregate with an initial value using `f(acc, element) => acc`.
    #[napi]
    pub fn fold(
        &mut self,
        env: Env,
        zero: serde_json::Value,
        f: JsFunction,
    ) -> Result<serde_json::Value> {
        if self.context.is_distributed() {
            let zero_json = zero.to_string();
            self.stage_js_udf(Self::fn_to_source(&env, &f)?, UdfAction::Fold, &zero_json)?;
            let staged = self.staged.as_ref()
                .ok_or_else(|| Error::from_reason("no staged pipeline"))?;
            let result_bytes = self.context
                .dispatch_pipeline(
                    staged.source_partitions.clone(),
                    staged.ops.clone(),
                )
                .map_err(|e| Error::from_reason(format!("dispatch_pipeline: {e}")))?;

            let mut acc = zero;
            for bytes in result_bytes {
                let items: Vec<serde_json::Value> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::from_reason(format!("fold decode: {e}")))?;
                for item in items {
                    acc = Self::call_fn2(&env, &f, &acc, &item)?;
                }
            }
            return Ok(acc);
        }
        let mut acc = zero;
        for val in &self.elements {
            acc = Self::call_fn2(&env, &f, &acc, val)?;
        }
        Ok(acc)
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
