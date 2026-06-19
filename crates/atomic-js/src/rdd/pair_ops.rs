use std::collections::HashMap;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde_json::Value as JsonValue;

use super::JsRdd;

#[napi]
impl JsRdd {
    /// Group `[key, value]` pairs by key → `[key, [values]]` pairs.
    #[napi]
    pub fn group_by_key(&mut self) -> Result<JsRdd> {
        if self.context.is_distributed() {
            // group_by_key is an action: dispatch immediately and leave `self` reusable
            // for further transforms/actions, same as the local (non-mutating) path.
            let saved_staged = self.staged.clone();
            let wrapper = "(partition) => { const g = new Map(), o = []; \
                for (const p of partition) { const k = JSON.stringify(p[0]); \
                if (!g.has(k)) { g.set(k, [p[0], []]); o.push(k); } \
                g.get(k)[1].push(p[1]); } \
                return o.map(k => g.get(k)); }"
                .to_string();
            self.stage_js_udf(wrapper, atomic_data::distributed::TaskAction::Map, None)?;
            let partials_result = self.dispatch_and_collect();
            self.staged = saved_staged;
            let partials = partials_result?;

            let mut groups: HashMap<String, (JsonValue, Vec<JsonValue>)> = HashMap::new();
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

        let mut groups: HashMap<String, (JsonValue, Vec<JsonValue>)> = HashMap::new();
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
        f: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsRdd> {
        if self.context.is_distributed() {
            // reduce_by_key is an action: dispatch immediately and leave `self` reusable
            // for further transforms/actions, same as the local (non-mutating) path.
            let saved_staged = self.staged.clone();
            let fn_src = Self::fn_to_source(&f)?;
            let wrapper = format!(
                "(partition) => {{ const g = new Map(), o = []; \
                 for (const p of partition) {{ const k = JSON.stringify(p[0]); \
                 if (g.has(k)) {{ g.set(k, [p[0], ({fn_src})(g.get(k)[1], p[1])]); }} \
                 else {{ g.set(k, [p[0], p[1]]); o.push(k); }} }} \
                 return o.map(k => g.get(k)); }}"
            );
            self.stage_js_udf(wrapper, atomic_data::distributed::TaskAction::Map, None)?;
            let partials_result = self.dispatch_and_collect();
            self.staged = saved_staged;
            let partials = partials_result?;

            let mut accum: HashMap<String, (JsonValue, JsonValue)> = HashMap::new();
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

        let mut accum: HashMap<String, (JsonValue, JsonValue)> = HashMap::new();
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
    pub fn lookup(&self, key: JsonValue) -> Result<Vec<JsonValue>> {
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

    /// Inner join two pair RDDs on their first element (key).
    ///
    /// Both RDDs must contain `[key, value]` arrays.
    /// Emits `[key, [left_value, right_value]]` for each matching key pair.
    ///
    /// In distributed mode the right side (if already in driver memory) is embedded
    /// in the worker closure as JSON — each worker joins against its left partition
    /// without collecting data to the driver first.
    #[napi]
    pub fn join(&mut self, other: &JsRdd) -> Result<JsRdd> {
        if self.context.is_distributed() && other.staged.is_none() {
            // join is an action: dispatch immediately and leave `self` reusable for
            // further transforms/actions, same as the local (non-mutating) path.
            let saved_staged = self.staged.clone();
            let right_json = serde_json::to_string(&other.elements)
                .map_err(|e| Error::from_reason(format!("join: serialize right: {e}")))?;
            let wrapper = format!(
                "(partition) => {{ \
                 const right = new Map(); \
                 for (const p of {right_json}) {{ \
                   const k = JSON.stringify(p[0]); \
                   if (!right.has(k)) right.set(k, [p[0], []]); \
                   right.get(k)[1].push(p[1]); }} \
                 const result = []; \
                 for (const pair of partition) {{ \
                   const k = JSON.stringify(pair[0]); \
                   if (right.has(k)) for (const rv of right.get(k)[1]) \
                     result.push([pair[0], [pair[1], rv]]); }} \
                 return result; }}"
            );
            self.stage_js_udf(wrapper, atomic_data::distributed::TaskAction::Map, None)?;
            let elements_result = self.dispatch_and_collect();
            self.staged = saved_staged;
            let elements = elements_result?;
            return Ok(JsRdd::from_data(
                elements,
                self.num_partitions,
                Arc::clone(&self.context),
            ));
        }

        // Driver-side hash join (local mode or staged right side)
        let mut right_map: HashMap<String, Vec<JsonValue>> = HashMap::new();
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
    /// Emits `[key, [left_value, right_value]]` for matched keys and
    /// `[key, [left_value, null]]` for unmatched left keys.
    ///
    /// In distributed mode the right side is embedded in the worker closure as JSON.
    #[napi]
    pub fn left_outer_join(&mut self, other: &JsRdd) -> Result<JsRdd> {
        if self.context.is_distributed() && other.staged.is_none() {
            // left_outer_join is an action: dispatch immediately and leave `self` reusable
            // for further transforms/actions, same as the local (non-mutating) path.
            let saved_staged = self.staged.clone();
            let right_json = serde_json::to_string(&other.elements)
                .map_err(|e| Error::from_reason(format!("left_outer_join: serialize right: {e}")))?;
            let wrapper = format!(
                "(partition) => {{ \
                 const right = new Map(); \
                 for (const p of {right_json}) {{ \
                   const k = JSON.stringify(p[0]); \
                   if (!right.has(k)) right.set(k, [p[0], []]); \
                   right.get(k)[1].push(p[1]); }} \
                 const result = []; \
                 for (const pair of partition) {{ \
                   const k = JSON.stringify(pair[0]); \
                   if (right.has(k)) {{ \
                     for (const rv of right.get(k)[1]) result.push([pair[0], [pair[1], rv]]); \
                   }} else {{ \
                     result.push([pair[0], [pair[1], null]]); \
                   }} }} \
                 return result; }}"
            );
            self.stage_js_udf(wrapper, atomic_data::distributed::TaskAction::Map, None)?;
            let elements_result = self.dispatch_and_collect();
            self.staged = saved_staged;
            let elements = elements_result?;
            return Ok(JsRdd::from_data(
                elements,
                self.num_partitions,
                Arc::clone(&self.context),
            ));
        }

        // Driver-side (local mode or staged right side)
        let mut right_map: HashMap<String, Vec<JsonValue>> = HashMap::new();
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
                    elements.push(serde_json::json!([key, [left_val, JsonValue::Null]]));
                }
            }
        }
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }
}
