use std::collections::HashMap;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde_json::Value as JsonValue;

use super::JsRdd;
use super::errors::JsUdfStageError;

#[napi]
impl JsRdd {
    /// Apply `f(element, ctx)` to each element.
    ///
    /// `ctx` is serialized as JSON and injected as `globalThis.__ctx` on the worker.
    /// Use this instead of `map` when your function closes over driver-side state —
    /// the captured scope is lost when `f.toString()` crosses the wire.
    #[napi]
    pub fn map_with_context(
        &mut self,
        ctx: JsonValue,
        f: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsRdd> {
        let context_json = serialize_ctx(&ctx)?;
        let fn_src = Self::fn_to_source(&f)?;
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!("(partition) => partition.map((x) => ({fn_src})(x, globalThis.__ctx))"),
                atomic_data::distributed::TaskAction::Map,
                Some(context_json),
            )?;
            return Ok(self.take_as_new());
        }
        let elements = self
            .elements
            .iter()
            .map(|elem| f.call(FnArgs::from((elem.clone(), ctx.clone()))))
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Keep only elements for which `f(element, ctx)` returns truthy.
    ///
    /// See `mapWithContext` for the captured-context contract.
    #[napi]
    pub fn filter_with_context(
        &mut self,
        ctx: JsonValue,
        f: Function<FnArgs<(JsonValue, JsonValue)>, bool>,
    ) -> Result<JsRdd> {
        let context_json = serialize_ctx(&ctx)?;
        let fn_src = Self::fn_to_source(&f)?;
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!("(partition) => partition.filter((x) => ({fn_src})(x, globalThis.__ctx))"),
                atomic_data::distributed::TaskAction::Filter,
                Some(context_json),
            )?;
            return Ok(self.take_as_new());
        }
        let mut elements = Vec::new();
        for elem in &self.elements {
            if f.call(FnArgs::from((elem.clone(), ctx.clone())))? {
                elements.push(elem.clone());
            }
        }
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Apply `f(element, ctx)` and flatten results.
    ///
    /// See `mapWithContext` for the captured-context contract.
    #[napi]
    pub fn flat_map_with_context(
        &mut self,
        ctx: JsonValue,
        f: Function<FnArgs<(JsonValue, JsonValue)>, Vec<JsonValue>>,
    ) -> Result<JsRdd> {
        let context_json = serialize_ctx(&ctx)?;
        let fn_src = Self::fn_to_source(&f)?;
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!("(partition) => partition.flatMap((x) => ({fn_src})(x, globalThis.__ctx))"),
                atomic_data::distributed::TaskAction::FlatMap,
                Some(context_json),
            )?;
            return Ok(self.take_as_new());
        }
        let mut elements = Vec::new();
        for elem in &self.elements {
            let result = f.call(FnArgs::from((elem.clone(), ctx.clone())))?;
            elements.extend(result);
        }
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Apply `f(value, ctx)` only to the value in each `[key, value]` pair.
    ///
    /// See `mapWithContext` for the captured-context contract.
    #[napi]
    pub fn map_values_with_context(
        &mut self,
        ctx: JsonValue,
        f: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsRdd> {
        let context_json = serialize_ctx(&ctx)?;
        let fn_src = Self::fn_to_source(&f)?;
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!(
                    "(partition) => partition.map((p) => [p[0], ({fn_src})(p[1], globalThis.__ctx)])"
                ),
                atomic_data::distributed::TaskAction::Map,
                Some(context_json),
            )?;
            return Ok(self.take_as_new());
        }
        let elements = self
            .elements
            .iter()
            .map(|elem| {
                let pair = elem.as_array().ok_or_else(|| {
                    Error::from_reason("mapValuesWithContext requires [key, value] arrays")
                })?;
                if pair.len() != 2 {
                    return Err(Error::from_reason(
                        "mapValuesWithContext requires 2-element arrays",
                    ));
                }
                let key = pair[0].clone();
                let new_val = f.call(FnArgs::from((pair[1].clone(), ctx.clone())))?;
                Ok(serde_json::json!([key, new_val]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Reduce pair `[key, value]` elements by key using `f(acc, val, ctx)`.
    ///
    /// See `mapWithContext` for the captured-context contract.
    #[napi]
    pub fn reduce_by_key_with_context(
        &mut self,
        ctx: JsonValue,
        f: Function<FnArgs<(JsonValue, JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsRdd> {
        let context_json = serialize_ctx(&ctx)?;
        let fn_src = Self::fn_to_source(&f)?;
        if self.context.is_distributed() {
            let wrapper = format!(
                r#"
                (partition) => {{ const g = new Map(), o = []; 
                    for (const p of partition) {{ const k = JSON.stringify(p[0]); 
                    if (g.has(k)) {{ g.set(k, [p[0], ({fn_src})(g.get(k)[1], p[1], globalThis.__ctx)]); }} 
                    else {{ g.set(k, [p[0], p[1]]); o.push(k); }} }} 
                    return o.map(k => g.get(k)); }}
                "#
            );
            self.stage_js_udf(
                wrapper,
                atomic_data::distributed::TaskAction::Map,
                Some(context_json.clone()),
            )?;
            let partials = self.dispatch_and_collect()?;

            let mut accum: HashMap<String, (JsonValue, JsonValue)> = HashMap::new();
            let mut order: Vec<String> = Vec::new();
            for pair in &partials {
                let arr = pair.as_array().ok_or_else(|| {
                    Error::from_reason("reduceByKeyWithContext partial: not an array")
                })?;
                if arr.len() != 2 {
                    return Err(Error::from_reason(
                        "reduceByKeyWithContext partial: expected [key, val]",
                    ));
                }
                let key = arr[0].clone();
                let val = arr[1].clone();
                let key_str = Self::key_to_string(&key)?;
                match accum.get_mut(&key_str) {
                    Some((_, acc)) => {
                        *acc = f.call(FnArgs::from((acc.clone(), val, ctx.clone())))?;
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

        let mut accum: HashMap<String, (JsonValue, JsonValue)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();
        for pair in &self.elements {
            let arr = pair.as_array().ok_or_else(|| {
                Error::from_reason("reduceByKeyWithContext: element is not an array")
            })?;
            if arr.len() != 2 {
                return Err(Error::from_reason(
                    "reduceByKeyWithContext: element is not a [key, val] pair",
                ));
            }
            let key = arr[0].clone();
            let val = arr[1].clone();
            let key_str = Self::key_to_string(&key)?;
            match accum.get_mut(&key_str) {
                Some((_, acc)) => {
                    *acc = f.call(FnArgs::from((acc.clone(), val, ctx.clone())))?;
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

    /// Aggregate all elements using `f(acc, element, ctx) => acc`. Throws if empty.
    ///
    /// See `mapWithContext` for the captured-context contract.
    #[napi]
    pub fn reduce_with_context(
        &mut self,
        ctx: JsonValue,
        f: Function<FnArgs<(JsonValue, JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsonValue> {
        let context_json = serialize_ctx(&ctx)?;
        let fn_src = Self::fn_to_source(&f)?;
        if self.context.is_distributed() {
            let partition_fn = format!(
                "(partition) => partition.length === 0 ? [] : \
                 [partition.reduce((a, x) => ({fn_src})(a, x, globalThis.__ctx))]"
            );
            self.stage_js_udf(
                partition_fn,
                atomic_data::distributed::TaskAction::Reduce,
                Some(context_json),
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
                        Some(a) => f.call(FnArgs::from((a, item, ctx.clone())))?,
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
            acc = f.call(FnArgs::from((acc, val.clone(), ctx.clone())))?;
        }
        Ok(acc)
    }

    /// Aggregate with an initial value using `f(acc, element, ctx) => acc`.
    ///
    /// See `mapWithContext` for the captured-context contract.
    #[napi]
    pub fn fold_with_context(
        &mut self,
        ctx: JsonValue,
        zero: JsonValue,
        f: Function<FnArgs<(JsonValue, JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsonValue> {
        let context_json = serialize_ctx(&ctx)?;
        let fn_src = Self::fn_to_source(&f)?;
        if self.context.is_distributed() {
            let zero_json = zero.to_string();
            let partition_fn = format!(
                "(partition) => [partition.reduce((a, x) => ({fn_src})(a, x, globalThis.__ctx), {zero_json})]"
            );
            self.stage_js_udf(
                partition_fn,
                atomic_data::distributed::TaskAction::Fold,
                Some(context_json),
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
                if let Some(item) = items.into_iter().next() {
                    acc = f.call(FnArgs::from((acc, item, ctx.clone())))?;
                }
            }
            return Ok(acc);
        }
        let mut acc = zero;
        for val in &self.elements {
            acc = f.call(FnArgs::from((acc, val.clone(), ctx.clone())))?;
        }
        Ok(acc)
    }
}

fn serialize_ctx(ctx: &JsonValue) -> Result<String> {
    serde_json::to_string(ctx).map_err(|e| JsUdfStageError::ContextEncode(e).into())
}
