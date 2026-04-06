use std::sync::Arc;

use rquickjs::{
    class::Trace, Array, Class, Ctx, Exception, Function, Persistent, Result,
    Value,
};
use rquickjs::JsLifetime;

use atomic_data::distributed::{JsUdfPayload, PipelineOp, TaskAction};

/// Accumulated lazy ops for distributed execution.
struct StagedJsPipeline {
    /// JSON-encoded partition data (one `Vec<u8>` per partition).
    source_partitions: Vec<Vec<u8>>,
    /// Ordered ops to dispatch to workers.
    ops: Vec<PipelineOp>,
}

/// JavaScript-facing RDD class exposed to QuickJS as `atomic.Rdd`.
///
/// In **local mode** all transformations execute eagerly in the calling thread.
/// In **distributed mode** `map`/`filter`/`flat_map`/`fold` build a lazy pipeline
/// that is dispatched to workers as a single `TaskEnvelope` per partition when an
/// action (`collect`, `count`, `fold`, `reduce`) is called.
///
/// # Example (JS)
/// ```javascript
/// const rdd = atomic.parallelize([1, 2, 3, 4]);
/// const result = rdd
///     .map(x => x + 1)
///     .filter(x => x > 2)
///     .collect();
/// // [3, 4, 5]
/// ```
#[derive(Trace, Clone, JsLifetime)]
#[rquickjs::class(rename = "Rdd")]
pub struct JsRdd {
    #[qjs(skip_trace)]
    elements: Vec<Persistent<Value<'static>>>,
    num_partitions: usize,
    #[qjs(skip_trace)]
    context: Arc<atomic_compute::context::Context>,
    #[qjs(skip_trace)]
    staged: Option<StagedJsPipeline>,
}

// Manual Clone for StagedJsPipeline (not derived since Trace requires Clone on JsRdd)
impl Clone for StagedJsPipeline {
    fn clone(&self) -> Self {
        Self {
            source_partitions: self.source_partitions.clone(),
            ops: self.ops.clone(),
        }
    }
}

impl JsRdd {
    pub fn new(
        elements: Vec<Persistent<Value<'static>>>,
        num_partitions: usize,
        context: Arc<atomic_compute::context::Context>,
    ) -> Self {
        Self { elements, num_partitions, context, staged: None }
    }

    pub fn from_js_values<'js>(
        ctx: &Ctx<'js>,
        values: Vec<Value<'js>>,
        num_partitions: usize,
        context: Arc<atomic_compute::context::Context>,
    ) -> Result<Self> {
        let persisted = values
            .into_iter()
            .map(|v| Persistent::save(ctx, v))
            .collect::<Vec<_>>();
        Ok(Self::new(persisted, num_partitions, context))
    }

    pub fn restore_elements<'js>(&self, ctx: &Ctx<'js>) -> Result<Vec<Value<'js>>> {
        self.elements
            .iter()
            .map(|p: &Persistent<Value<'static>>| p.clone().restore(ctx))
            .collect()
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    /// JSON-encode elements split into `num_partitions` chunks using the QuickJS context.
    fn encode_source_partitions<'js>(&self, ctx: &Ctx<'js>) -> Result<Vec<Vec<u8>>> {
        let elements = self.restore_elements(ctx)?;
        let total = elements.len();
        let np = self.num_partitions.max(1);
        let chunk_size = ((total + np - 1) / np).max(1);

        let mut partitions = Vec::with_capacity(np);
        for chunk in elements.chunks(chunk_size) {
            let arr = Array::new(ctx.clone())?;
            for (i, v) in chunk.iter().enumerate() {
                arr.set(i, v.clone())?;
            }
            let json_str = ctx.json_stringify(arr.into_value())?
                .ok_or_else(|| Exception::throw_type(ctx, "JSON.stringify returned undefined"))?;
            let s: String = json_str.get()?;
            partitions.push(s.into_bytes());
        }
        while partitions.len() < np {
            partitions.push(b"[]".to_vec());
        }
        Ok(partitions)
    }

    /// Get the source string of a JS function using `String(f)`.
    fn fn_to_source<'js>(ctx: &Ctx<'js>, f: &Function<'js>) -> Result<String> {
        // String(f) → fn.toString() source
        let s: rquickjs::String<'js> = ctx
            .globals()
            .get::<_, Function>("String")?
            .call((f.clone(),))?;
        Ok(s.to_string()?)
    }

    /// Build a JS UDF op and push it into the staged pipeline.
    fn stage_js_udf<'js>(
        &mut self,
        ctx: &Ctx<'js>,
        f: &Function<'js>,
        action: &str,
        zero_json: &str,
    ) -> Result<()> {
        let fn_source = Self::fn_to_source(ctx, f)?;
        let payload_struct = JsUdfPayload {
            action: action.to_string(),
            fn_source,
            zero_json: zero_json.to_string(),
        };
        let payload = serde_json::to_vec(&payload_struct)
            .map_err(|e| Exception::throw_type(ctx, &format!("JsUdfPayload encode: {e}")))?;
        let op = PipelineOp {
            op_id: "atomic::udf::js".to_string(),
            action: TaskAction::JavaScriptUdf,
            payload,
        };
        self.push_op(ctx, op)
    }

    /// Push an op into the staged pipeline, encoding source data on first call.
    fn push_op<'js>(&mut self, ctx: &Ctx<'js>, op: PipelineOp) -> Result<()> {
        if let Some(ref mut staged) = self.staged {
            staged.ops.push(op);
        } else {
            let source_partitions = self.encode_source_partitions(ctx)?;
            self.staged = Some(StagedJsPipeline { source_partitions, ops: vec![op] });
        }
        Ok(())
    }

    /// Move staged state into a new JsRdd (empty elements; data is in source_partitions).
    fn take_as_new(&mut self) -> JsRdd {
        JsRdd {
            elements: Vec::new(),
            num_partitions: self.num_partitions,
            context: Arc::clone(&self.context),
            staged: self.staged.take(),
        }
    }

    /// Dispatch staged pipeline and return JSON bytes per partition.
    fn dispatch_staged(&self, ctx: &Ctx<'_>) -> Result<Vec<Vec<u8>>> {
        let staged = self.staged.as_ref()
            .ok_or_else(|| Exception::throw_type(ctx, "no staged pipeline to dispatch"))?;
        self.context
            .dispatch_pipeline(staged.source_partitions.clone(), staged.ops.clone())
            .map_err(|e| Exception::throw_type(ctx, &format!("dispatch_pipeline: {e}")))
    }

    /// Decode distributed result bytes (JSON arrays) into a flat JS Array.
    fn decode_result_bytes<'js>(ctx: &Ctx<'js>, result_bytes: Vec<Vec<u8>>) -> Result<Array<'js>> {
        let out = Array::new(ctx.clone())?;
        let mut idx = 0usize;
        for bytes in result_bytes {
            let json_str = std::str::from_utf8(&bytes)
                .map_err(|e| Exception::throw_type(ctx, &format!("result utf8: {e}")))?;
            let parsed: Value<'js> = ctx.json_parse(json_str.to_string())?;
            let arr = parsed.as_array()
                .ok_or_else(|| Exception::throw_type(ctx, "expected JSON array from worker"))?;
            for item in arr.iter::<Value>() {
                out.set(idx, item?)?;
                idx += 1;
            }
        }
        Ok(out)
    }
}

#[rquickjs::methods]
impl JsRdd {
    // ── Transformations ──────────────────────────────────────────────────────

    /// Apply `f` to each element, returning a new Rdd.
    pub fn map<'js>(&mut self, ctx: Ctx<'js>, f: Function<'js>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(&ctx, &f, "map", "")?;
            return Ok(self.take_as_new());
        }
        // Local eager
        let elements = self.restore_elements(&ctx)?;
        let mapped = elements
            .into_iter()
            .map(|v| {
                let result: Value<'js> = f.call((v,))?;
                Ok(Persistent::save(&ctx, result))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::new(mapped, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Keep only elements for which `f` returns truthy.
    pub fn filter<'js>(&mut self, ctx: Ctx<'js>, f: Function<'js>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(&ctx, &f, "filter", "")?;
            return Ok(self.take_as_new());
        }
        // Local eager
        let elements = self.restore_elements(&ctx)?;
        let filtered = elements
            .into_iter()
            .filter_map(|v| {
                let keep: Result<bool> = f.call::<_, Value>((v.clone(),))
                    .and_then(|r| r.as_bool().ok_or_else(|| {
                        Exception::throw_type(&ctx, "filter function must return a boolean")
                    }));
                match keep {
                    Ok(true) => Some(Ok(Persistent::save(&ctx, v))),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::new(filtered, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Apply `f` to each element and flatten (f must return an Array).
    pub fn flat_map<'js>(&mut self, ctx: Ctx<'js>, f: Function<'js>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(&ctx, &f, "flat_map", "")?;
            return Ok(self.take_as_new());
        }
        // Local eager
        let elements = self.restore_elements(&ctx)?;
        let mut flat = Vec::new();
        for v in elements {
            let result: Value<'js> = f.call((v,))?;
            let arr = result.as_array().ok_or_else(|| {
                Exception::throw_type(&ctx, "flat_map function must return an Array")
            })?;
            for item in arr.iter::<Value>() {
                flat.push(Persistent::save(&ctx, item?));
            }
        }
        Ok(JsRdd::new(flat, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Apply `f` to the value in each `[key, value]` pair.
    pub fn map_values<'js>(&mut self, ctx: Ctx<'js>, f: Function<'js>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(&ctx, &f, "map_values", "")?;
            return Ok(self.take_as_new());
        }
        let elements = self.restore_elements(&ctx)?;
        let mapped = elements
            .into_iter()
            .map(|v| {
                let pair = v.as_array().ok_or_else(|| {
                    Exception::throw_type(&ctx, "map_values requires [key, value] arrays")
                })?;
                if pair.len() != 2 {
                    return Err(Exception::throw_type(
                        &ctx, "map_values requires 2-element arrays",
                    ));
                }
                let key: Value = pair.get(0)?;
                let val: Value = pair.get(1)?;
                let new_val: Value = f.call((val,))?;
                let arr = Array::new(ctx.clone())?;
                arr.set(0, key)?;
                arr.set(1, new_val)?;
                Ok(Persistent::save(&ctx, arr.into_value()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::new(mapped, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Apply `f` to each value in `[key, value]` pairs and flatten results.
    pub fn flat_map_values<'js>(&mut self, ctx: Ctx<'js>, f: Function<'js>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(&ctx, &f, "flat_map_values", "")?;
            return Ok(self.take_as_new());
        }
        let elements = self.restore_elements(&ctx)?;
        let mut flat = Vec::new();
        for v in elements {
            let pair = v.as_array().ok_or_else(|| {
                Exception::throw_type(&ctx, "flat_map_values requires [key, value] arrays")
            })?;
            if pair.len() != 2 {
                return Err(Exception::throw_type(
                    &ctx, "flat_map_values requires 2-element arrays",
                ));
            }
            let key: Value = pair.get(0)?;
            let result_arr: Array = f.call((pair.get::<Value>(1)?,))?;
            for item in result_arr.iter::<Value>() {
                let new_pair = Array::new(ctx.clone())?;
                new_pair.set(0, key.clone())?;
                new_pair.set(1, item?)?;
                flat.push(Persistent::save(&ctx, new_pair.into_value()));
            }
        }
        Ok(JsRdd::new(flat, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Produce `[f(element), element]` pairs.
    pub fn key_by<'js>(&mut self, ctx: Ctx<'js>, f: Function<'js>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(&ctx, &f, "key_by", "")?;
            return Ok(self.take_as_new());
        }
        let elements = self.restore_elements(&ctx)?;
        let keyed = elements
            .into_iter()
            .map(|v| {
                let key: Value = f.call((v.clone(),))?;
                let arr = Array::new(ctx.clone())?;
                arr.set(0, key)?;
                arr.set(1, v)?;
                Ok(Persistent::save(&ctx, arr.into_value()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::new(keyed, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Group elements by `f(element)`, returning `[key, [elements]]` pairs.
    pub fn group_by<'js>(&mut self, ctx: Ctx<'js>, f: Function<'js>) -> Result<JsRdd> {
        let keyed = self.key_by(ctx.clone(), f)?;
        keyed.group_by_key(ctx)
    }

    /// Group `[key, value]` pairs by key, returning `[key, [values]]` pairs.
    pub fn group_by_key<'js>(&self, ctx: Ctx<'js>) -> Result<JsRdd> {
        use std::collections::HashMap;
        let elements = self.restore_elements(&ctx)?;
        let mut groups: HashMap<String, (Value<'js>, Vec<Value<'js>>)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for v in elements {
            let pair = v.as_array().ok_or_else(|| {
                Exception::throw_type(&ctx, "group_by_key requires [key, value] arrays")
            })?;
            let key: Value = pair.get(0)?;
            let val: Value = pair.get(1)?;
            let key_str = key_to_string(&ctx, &key)?;
            let entry = groups.entry(key_str.clone()).or_insert_with(|| {
                order.push(key_str);
                (key.clone(), Vec::new())
            });
            entry.1.push(val);
        }

        let result = order
            .iter()
            .map(|k| {
                let (key, vals) = &groups[k];
                let arr = Array::new(ctx.clone())?;
                let vals_arr = Array::new(ctx.clone())?;
                for (i, v) in vals.iter().enumerate() {
                    vals_arr.set(i, v.clone())?;
                }
                arr.set(0, key.clone())?;
                arr.set(1, vals_arr)?;
                Ok(Persistent::save(&ctx, arr.into_value()))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(JsRdd::new(result, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Aggregate values with the same key using `f(acc, value) => acc`.
    pub fn reduce_by_key<'js>(&self, ctx: Ctx<'js>, f: Function<'js>) -> Result<JsRdd> {
        use std::collections::HashMap;
        let elements = self.restore_elements(&ctx)?;
        let mut accum: HashMap<String, (Value<'js>, Value<'js>)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for v in elements {
            let pair = v.as_array().ok_or_else(|| {
                Exception::throw_type(&ctx, "reduce_by_key requires [key, value] arrays")
            })?;
            let key: Value = pair.get(0)?;
            let val: Value = pair.get(1)?;
            let key_str = key_to_string(&ctx, &key)?;
            match accum.get_mut(&key_str) {
                Some((_, acc)) => {
                    let new_acc: Value = f.call((acc.clone(), val))?;
                    *acc = new_acc;
                }
                None => {
                    order.push(key_str.clone());
                    accum.insert(key_str, (key, val));
                }
            }
        }

        let result = order
            .iter()
            .map(|k| {
                let (key, val) = &accum[k];
                let arr = Array::new(ctx.clone())?;
                arr.set(0, key.clone())?;
                arr.set(1, val.clone())?;
                Ok(Persistent::save(&ctx, arr.into_value()))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(JsRdd::new(result, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Merge two Rdds into one.
    pub fn union<'js>(&self, _ctx: Ctx<'js>, other: Class<'js, JsRdd>) -> Result<JsRdd> {
        let mut elements = self.elements.clone();
        elements.extend(other.borrow().elements.clone());
        Ok(JsRdd::new(
            elements,
            self.num_partitions + other.borrow().num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Zip two Rdds element-wise into an Rdd of `[a, b]` arrays.
    pub fn zip<'js>(&self, ctx: Ctx<'js>, other: Class<'js, JsRdd>) -> Result<JsRdd> {
        let a = self.restore_elements(&ctx)?;
        let b = other.borrow().restore_elements(&ctx)?;
        if a.len() != b.len() {
            return Err(Exception::throw_range(
                &ctx,
                &format!("zip requires equal-length Rdds: {} vs {}", a.len(), b.len()),
            ));
        }
        let zipped = a
            .into_iter()
            .zip(b.into_iter())
            .map(|(av, bv)| {
                let arr = Array::new(ctx.clone())?;
                arr.set(0, av)?;
                arr.set(1, bv)?;
                Ok(Persistent::save(&ctx, arr.into_value()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::new(zipped, self.num_partitions, Arc::clone(&self.context)))
    }

    /// Compute the Cartesian product of two Rdds as `[a, b]` pairs.
    pub fn cartesian<'js>(&self, ctx: Ctx<'js>, other: Class<'js, JsRdd>) -> Result<JsRdd> {
        let a = self.restore_elements(&ctx)?;
        let b = other.borrow().restore_elements(&ctx)?;
        let mut result = Vec::new();
        for av in &a {
            for bv in &b {
                let arr = Array::new(ctx.clone())?;
                arr.set(0, av.clone())?;
                arr.set(1, bv.clone())?;
                result.push(Persistent::save(&ctx, arr.into_value()));
            }
        }
        let partitions = self.num_partitions * other.borrow().num_partitions.max(1);
        Ok(JsRdd::new(result, partitions, Arc::clone(&self.context)))
    }

    /// Coalesce to `n` logical partitions (no-op on data for local execution).
    pub fn coalesce(&self, n: usize) -> JsRdd {
        JsRdd::new(self.elements.clone(), n.max(1), Arc::clone(&self.context))
    }

    /// Alias for coalesce.
    pub fn repartition(&self, n: usize) -> JsRdd {
        self.coalesce(n)
    }

    // ── Actions ──────────────────────────────────────────────────────────────

    /// Return all elements as a JS Array.
    pub fn collect<'js>(&mut self, ctx: Ctx<'js>) -> Result<Array<'js>> {
        if self.context.is_distributed() && self.staged.is_some() {
            let result_bytes = self.dispatch_staged(&ctx)?;
            return Self::decode_result_bytes(&ctx, result_bytes);
        }
        // Local eager
        let elements = self.restore_elements(&ctx)?;
        let arr = Array::new(ctx.clone())?;
        for (i, v) in elements.into_iter().enumerate() {
            arr.set(i, v)?;
        }
        Ok(arr)
    }

    /// Return the number of elements.
    pub fn count<'js>(&mut self, ctx: Ctx<'js>) -> Result<usize> {
        if self.context.is_distributed() && self.staged.is_some() {
            let arr = self.collect(ctx)?;
            return Ok(arr.len());
        }
        Ok(self.elements.len())
    }

    /// Return the first element, or throw if empty.
    pub fn first<'js>(&self, ctx: Ctx<'js>) -> Result<Value<'js>> {
        self.elements
            .first()
            .map(|p: &Persistent<Value<'static>>| p.clone().restore(&ctx))
            .unwrap_or_else(|| Err(Exception::throw_range(&ctx, "Rdd is empty")))
    }

    /// Return the first `n` elements as a JS Array.
    pub fn take<'js>(&self, ctx: Ctx<'js>, n: usize) -> Result<Array<'js>> {
        let arr = Array::new(ctx.clone())?;
        for (i, p) in self.elements.iter().take(n).enumerate() {
            let p: &Persistent<Value<'static>> = p;
            arr.set(i, p.clone().restore(&ctx)?)?;
        }
        Ok(arr)
    }

    /// Aggregate all elements with `f(acc, element) => acc`.
    ///
    /// In distributed mode dispatches to workers (each returns `[scalar]` or `[]`),
    /// then combines non-empty partition results on the driver.
    pub fn reduce<'js>(&mut self, ctx: Ctx<'js>, f: Function<'js>) -> Result<Value<'js>> {
        if self.context.is_distributed() {
            self.stage_js_udf(&ctx, &f, "reduce", "")?;
            let result_bytes = self.dispatch_staged(&ctx)?;

            let mut acc: Option<Value<'js>> = None;
            for bytes in result_bytes {
                let json_str = std::str::from_utf8(&bytes)
                    .map_err(|e| Exception::throw_type(&ctx, &format!("reduce result utf8: {e}")))?;
                let parsed: Value<'js> = ctx.json_parse(json_str.to_string())?;
                let arr = parsed.as_array()
                    .ok_or_else(|| Exception::throw_type(&ctx, "expected array from reduce worker"))?;
                if arr.len() == 0 {
                    continue; // empty partition
                }
                let scalar: Value<'js> = arr.get(0)?;
                acc = Some(match acc {
                    None => scalar,
                    Some(a) => f.call((a, scalar))?,
                });
            }
            return acc.ok_or_else(|| Exception::throw_range(&ctx, "reduce on empty Rdd"));
        }
        // Local eager
        let mut elements = self.restore_elements(&ctx)?.into_iter();
        let first = elements.next().ok_or_else(|| Exception::throw_range(&ctx, "reduce on empty Rdd"))?;
        let mut acc = first;
        for v in elements {
            acc = f.call((acc, v))?;
        }
        Ok(acc)
    }

    /// Aggregate with an initial value using `f(acc, element) => acc`.
    ///
    /// In distributed mode this dispatches the fold to workers, then combines
    /// per-partition `[scalar]` results on the driver.
    pub fn fold<'js>(&mut self, ctx: Ctx<'js>, zero: Value<'js>, f: Function<'js>) -> Result<Value<'js>> {
        if self.context.is_distributed() {
            // Serialize zero to JSON for the payload
            let zero_json = ctx.json_stringify(zero.clone())?
                .ok_or_else(|| Exception::throw_type(&ctx, "fold zero must be JSON-serializable"))?;
            let zero_json_str: String = zero_json.get()?;

            self.stage_js_udf(&ctx, &f, "fold", &zero_json_str)?;

            let result_bytes = self.dispatch_staged(&ctx)?;

            // Each partition returns [scalar]. Combine scalars with f on the driver.
            let mut acc = zero;
            for bytes in result_bytes {
                let json_str = std::str::from_utf8(&bytes)
                    .map_err(|e| Exception::throw_type(&ctx, &format!("fold result utf8: {e}")))?;
                let parsed: Value<'js> = ctx.json_parse(json_str.to_string())?;
                let arr = parsed.as_array()
                    .ok_or_else(|| Exception::throw_type(&ctx, "expected [scalar] from fold worker"))?;
                let scalar: Value<'js> = arr.get(0)?;
                acc = f.call((acc, scalar))?;
            }
            return Ok(acc);
        }
        // Local eager
        let mut acc = zero;
        for p in &self.elements {
            let p: &Persistent<Value<'static>> = p;
            let v = p.clone().restore(&ctx)?;
            acc = f.call((acc, v))?;
        }
        Ok(acc)
    }

    #[qjs(get)]
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }
}

fn key_to_string<'js>(ctx: &Ctx<'js>, key: &Value<'js>) -> Result<String> {
    if let Some(s) = key.as_string() {
        return Ok(s.to_string()?);
    }
    if let Some(n) = key.as_int() {
        return Ok(n.to_string());
    }
    if let Some(f) = key.as_float() {
        return Ok(f.to_string());
    }
    if let Some(b) = key.as_bool() {
        return Ok(b.to_string());
    }
    Err(Exception::throw_type(
        ctx,
        "key must be a string, number, or boolean",
    ))
}
