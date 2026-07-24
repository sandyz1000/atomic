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
            self.stage_js_task(wrapper, atomic_data::distributed::TaskAction::Map, None)?;
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
            self.stage_js_task(wrapper, atomic_data::distributed::TaskAction::Map, None)?;
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

    /// Sum the values for each key, returning `[key, sum]`. In distributed mode a `(a,b)=>a+b`
    /// combiner is shipped so workers pre-aggregate per partition; the driver merges the partials.
    #[napi]
    pub fn sum_values(&mut self) -> Result<JsRdd> {
        self.combine_values("(a, b) => a + b", |acc, v| {
            number_json(acc.as_f64().unwrap_or(0.0) + v.as_f64().unwrap_or(0.0))
        })
    }

    /// Keep the maximum value for each key, returning `[key, max]`.
    #[napi]
    pub fn max_values(&mut self) -> Result<JsRdd> {
        self.combine_values("(a, b) => (a >= b ? a : b)", |acc, v| {
            if Self::json_compare(&v, &acc) == std::cmp::Ordering::Greater {
                v
            } else {
                acc
            }
        })
    }

    /// Keep the minimum value for each key, returning `[key, min]`.
    #[napi]
    pub fn min_values(&mut self) -> Result<JsRdd> {
        self.combine_values("(a, b) => (a <= b ? a : b)", |acc, v| {
            if Self::json_compare(&v, &acc) == std::cmp::Ordering::Less {
                v
            } else {
                acc
            }
        })
    }

    /// Count the values per key, returning `[key, count]`. In distributed mode workers count per
    /// partition (a shipped map-side combine) and the driver sums the partial counts.
    #[napi]
    pub fn count_values(&mut self) -> Result<JsRdd> {
        // Map each value to 1 on the workers, then sum. The wrapper counts per key per partition.
        let wrapper = "(partition) => { const g = new Map(), o = []; \
             for (const p of partition) { const k = JSON.stringify(p[0]); \
             if (g.has(k)) { g.set(k, [p[0], g.get(k)[1] + 1]); } \
             else { g.set(k, [p[0], 1]); o.push(k); } } \
             return o.map(k => g.get(k)); }";
        let partials = self.map_side_combine(wrapper)?;
        self.merge_partials(partials, |acc, v| {
            number_json(acc.as_f64().unwrap_or(0.0) + v.as_f64().unwrap_or(0.0))
        })
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
            self.stage_js_task(wrapper, atomic_data::distributed::TaskAction::Map, None)?;
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
            let right_json = serde_json::to_string(&other.elements).map_err(|e| {
                Error::from_reason(format!("left_outer_join: serialize right: {e}"))
            })?;
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
            self.stage_js_task(wrapper, atomic_data::distributed::TaskAction::Map, None)?;
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

    /// Right outer join: every right key is preserved.
    /// Emits `[key, [left_value, right_value]]` for matched keys and
    /// `[key, [null, right_value]]` for unmatched right keys.
    #[napi]
    pub fn right_outer_join(&self, other: &JsRdd) -> Result<JsRdd> {
        let mut left_map: HashMap<String, Vec<JsonValue>> = HashMap::new();
        for elem in &self.elements {
            let pair = elem.as_array().ok_or_else(|| {
                Error::from_reason("right_outer_join: requires [key, value] arrays")
            })?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "right_outer_join: requires 2-element arrays",
                ));
            }
            let key_str = Self::key_to_string(&pair[0])?;
            left_map.entry(key_str).or_default().push(pair[1].clone());
        }
        let mut elements = Vec::new();
        for elem in &other.elements {
            let pair = elem.as_array().ok_or_else(|| {
                Error::from_reason("right_outer_join: requires [key, value] arrays")
            })?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "right_outer_join: requires 2-element arrays",
                ));
            }
            let key = &pair[0];
            let right_val = &pair[1];
            let key_str = Self::key_to_string(key)?;
            match left_map.get(&key_str) {
                Some(left_vals) => {
                    for lv in left_vals {
                        elements.push(serde_json::json!([key, [lv, right_val]]));
                    }
                }
                None => {
                    elements.push(serde_json::json!([key, [JsonValue::Null, right_val]]));
                }
            }
        }
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Full outer join: all keys from both sides preserved.
    #[napi]
    pub fn full_outer_join(&self, other: &JsRdd) -> Result<JsRdd> {
        let mut left_map: HashMap<String, Vec<JsonValue>> = HashMap::new();
        for elem in &self.elements {
            let pair = elem.as_array().ok_or_else(|| {
                Error::from_reason("full_outer_join: requires [key, value] arrays")
            })?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "full_outer_join: requires 2-element arrays",
                ));
            }
            let key_str = Self::key_to_string(&pair[0])?;
            left_map.entry(key_str).or_default().push(pair[1].clone());
        }
        let mut right_map: HashMap<String, Vec<JsonValue>> = HashMap::new();
        for elem in &other.elements {
            let pair = elem.as_array().ok_or_else(|| {
                Error::from_reason("full_outer_join: requires [key, value] arrays")
            })?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "full_outer_join: requires 2-element arrays",
                ));
            }
            let key_str = Self::key_to_string(&pair[0])?;
            right_map.entry(key_str).or_default().push(pair[1].clone());
        }
        let mut all_keys: HashMap<String, (JsonValue, bool)> = HashMap::new();
        for elem in &self.elements {
            let pair = elem.as_array().unwrap();
            all_keys.insert(
                Self::key_to_string(&pair[0]).unwrap_or_default(),
                (pair[0].clone(), true),
            );
        }
        for elem in &other.elements {
            let pair = elem.as_array().unwrap();
            all_keys
                .entry(Self::key_to_string(&pair[0]).unwrap_or_default())
                .or_insert_with(|| (pair[0].clone(), true));
        }
        let mut elements = Vec::new();
        for (key_str, (key_obj, _)) in &all_keys {
            let left_vals = left_map.get(key_str).cloned().unwrap_or_default();
            let right_vals = right_map.get(key_str).cloned().unwrap_or_default();
            if left_vals.is_empty() && right_vals.is_empty() {
                continue;
            }
            if left_vals.is_empty() {
                for rv in &right_vals {
                    elements.push(serde_json::json!([key_obj, [JsonValue::Null, rv]]));
                }
            } else if right_vals.is_empty() {
                for lv in &left_vals {
                    elements.push(serde_json::json!([key_obj, [lv, JsonValue::Null]]));
                }
            } else {
                for lv in &left_vals {
                    for rv in &right_vals {
                        elements.push(serde_json::json!([key_obj, [lv, rv]]));
                    }
                }
            }
        }
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Co-group: `[key, [left_values], [right_values]]` for every key on either side.
    #[napi]
    pub fn cogroup(&self, other: &JsRdd) -> Result<JsRdd> {
        let mut left_map: HashMap<String, Vec<JsonValue>> = HashMap::new();
        for elem in &self.elements {
            let pair = elem
                .as_array()
                .ok_or_else(|| Error::from_reason("cogroup: requires [key, value] arrays"))?;
            if pair.len() != 2 {
                return Err(Error::from_reason("cogroup: requires 2-element arrays"));
            }
            let key_str = Self::key_to_string(&pair[0])?;
            left_map.entry(key_str).or_default().push(pair[1].clone());
        }
        let mut right_map: HashMap<String, Vec<JsonValue>> = HashMap::new();
        for elem in &other.elements {
            let pair = elem
                .as_array()
                .ok_or_else(|| Error::from_reason("cogroup: requires [key, value] arrays"))?;
            if pair.len() != 2 {
                return Err(Error::from_reason("cogroup: requires 2-element arrays"));
            }
            let key_str = Self::key_to_string(&pair[0])?;
            right_map.entry(key_str).or_default().push(pair[1].clone());
        }
        let mut all_keys = std::collections::BTreeSet::new();
        for k in left_map.keys() {
            all_keys.insert(k.clone());
        }
        for k in right_map.keys() {
            all_keys.insert(k.clone());
        }
        let elements: Vec<JsonValue> = all_keys
            .iter()
            .map(|ks| {
                let left_vals: Vec<JsonValue> = left_map.get(ks).cloned().unwrap_or_default();
                let right_vals: Vec<JsonValue> = right_map.get(ks).cloned().unwrap_or_default();
                // Use the first-occurring key object from left or right.
                let key_obj = left_map
                    .get(ks)
                    .and_then(|_v| {
                        self.elements
                            .iter()
                            .find(|e| {
                                e.as_array().is_some_and(|a| {
                                    a.first().is_some_and(|k| {
                                        Self::key_to_string(k).ok() == Some(ks.clone())
                                    })
                                })
                            })
                            .and_then(|e| e.as_array().and_then(|a| a.first().cloned()))
                    })
                    .or_else(|| {
                        other
                            .elements
                            .iter()
                            .find(|e| {
                                e.as_array().is_some_and(|a| {
                                    a.first().is_some_and(|k| {
                                        Self::key_to_string(k).ok() == Some(ks.clone())
                                    })
                                })
                            })
                            .and_then(|e| e.as_array().and_then(|a| a.first().cloned()))
                    })
                    .unwrap_or(JsonValue::Null);
                serde_json::json!([key_obj, left_vals, right_vals])
            })
            .collect();
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Fold values by key: `f(acc, value) => acc`, seeded by `zero`.
    #[napi]
    pub fn fold_by_key(
        &self,
        zero: JsonValue,
        f: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsRdd> {
        let mut accum: HashMap<String, (JsonValue, JsonValue)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();
        for elem in &self.elements {
            let pair = elem
                .as_array()
                .ok_or_else(|| Error::from_reason("fold_by_key: requires [key, value] arrays"))?;
            if pair.len() != 2 {
                return Err(Error::from_reason("fold_by_key: requires 2-element arrays"));
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
                    let acc = f.call(FnArgs::from((zero.clone(), val)))?;
                    accum.insert(key_str, (key, acc));
                }
            }
        }
        let elements = order
            .iter()
            .map(|ks| {
                let (k, v) = &accum[ks];
                Ok(serde_json::json!([k, v]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Aggregate values by key with separate seq and comb functions.
    #[napi]
    pub fn aggregate_by_key(
        &self,
        zero: JsonValue,
        seq_fn: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
        _comb_fn: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsRdd> {
        let mut accum: HashMap<String, (JsonValue, JsonValue)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();
        for elem in &self.elements {
            let pair = elem.as_array().ok_or_else(|| {
                Error::from_reason("aggregate_by_key: requires [key, value] arrays")
            })?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "aggregate_by_key: requires 2-element arrays",
                ));
            }
            let key = pair[0].clone();
            let val = pair[1].clone();
            let key_str = Self::key_to_string(&key)?;
            match accum.get_mut(&key_str) {
                Some((_, acc)) => {
                    *acc = seq_fn.call(FnArgs::from((acc.clone(), val)))?;
                }
                None => {
                    order.push(key_str.clone());
                    let acc = seq_fn.call(FnArgs::from((zero.clone(), val)))?;
                    accum.insert(key_str, (key, acc));
                }
            }
        }
        let elements = order
            .iter()
            .map(|ks| {
                let (k, v) = &accum[ks];
                Ok(serde_json::json!([k, v]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Generalised `combine_by_key`: `create(val) => C`, `mergeVal(C, val) => C`,
    /// `mergeCombiners(C, C) => C`.
    #[napi]
    pub fn combine_by_key(
        &self,
        create_combiner: Function<JsonValue, JsonValue>,
        merge_value: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
        _merge_combiners: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<JsRdd> {
        let mut accum: HashMap<String, (JsonValue, JsonValue)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();
        for elem in &self.elements {
            let pair = elem.as_array().ok_or_else(|| {
                Error::from_reason("combine_by_key: requires [key, value] arrays")
            })?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "combine_by_key: requires 2-element arrays",
                ));
            }
            let key = pair[0].clone();
            let val = pair[1].clone();
            let key_str = Self::key_to_string(&key)?;
            match accum.get_mut(&key_str) {
                Some((_, combiner)) => {
                    *combiner = merge_value.call(FnArgs::from((combiner.clone(), val)))?;
                }
                None => {
                    order.push(key_str.clone());
                    let c = create_combiner.call(val)?;
                    accum.insert(key_str, (key, c));
                }
            }
        }
        let elements = order
            .iter()
            .map(|ks| {
                let (k, v) = &accum[ks];
                Ok(serde_json::json!([k, v]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Driver-side `reduce_by_key` returning a JS object `{key: reducedValue}`.
    #[napi]
    pub fn reduce_by_key_locally(
        &self,
        f: Function<FnArgs<(JsonValue, JsonValue)>, JsonValue>,
    ) -> Result<serde_json::Value> {
        let mut accum: HashMap<String, JsonValue> = HashMap::new();
        for elem in &self.elements {
            let pair = elem.as_array().ok_or_else(|| {
                Error::from_reason("reduce_by_key_locally: requires [key, value] arrays")
            })?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "reduce_by_key_locally: requires 2-element arrays",
                ));
            }
            let key = &pair[0];
            let val = &pair[1];
            let key_str = Self::key_to_string(key)?;
            match accum.remove(&key_str) {
                Some(existing) => {
                    accum.insert(key_str, f.call(FnArgs::from((existing, val.clone())))?);
                }
                None => {
                    accum.insert(key_str, val.clone());
                }
            }
        }
        Ok(serde_json::to_value(accum).unwrap_or_default())
    }

    /// Collect pair RDD into a JS object `{key: value}`. Last value wins on duplicate keys.
    #[napi]
    pub fn collect_as_map(&self) -> Result<serde_json::Value> {
        let mut map = serde_json::Map::new();
        for elem in &self.elements {
            let pair = elem.as_array().ok_or_else(|| {
                Error::from_reason("collect_as_map: requires [key, value] arrays")
            })?;
            if pair.len() != 2 {
                return Err(Error::from_reason(
                    "collect_as_map: requires 2-element arrays",
                ));
            }
            let key_str = Self::key_to_string(&pair[0])?;
            map.insert(key_str, pair[1].clone());
        }
        Ok(serde_json::Value::Object(map))
    }

    /// Remove pairs whose key exists in `other`.
    #[napi]
    pub fn subtract_by_key(&self, other: &JsRdd) -> Result<JsRdd> {
        let excluded: std::collections::HashSet<String> = other
            .elements
            .iter()
            .filter_map(|e| {
                e.as_array()
                    .and_then(|a| a.first())
                    .and_then(|k| Self::key_to_string(k).ok())
            })
            .collect();
        let elements: Vec<JsonValue> = self
            .elements
            .iter()
            .filter(|e| {
                if let Some(arr) = e.as_array()
                    && let Some(k) = arr.first()
                    && let Ok(ks) = Self::key_to_string(k)
                {
                    return !excluded.contains(&ks);
                }
                true
            })
            .cloned()
            .collect();
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }
}

// Non-`#[napi]` helpers for the value-reduction sugar methods above.
impl JsRdd {
    /// Split a `[key, value]` pair, erroring on any other shape.
    fn split_pair(pair: &JsonValue) -> Result<(JsonValue, JsonValue)> {
        let arr = pair
            .as_array()
            .ok_or_else(|| Error::from_reason("expected [key, value] pair"))?;
        if arr.len() != 2 {
            return Err(Error::from_reason("expected [key, value] pair"));
        }
        Ok((arr[0].clone(), arr[1].clone()))
    }

    /// The `[key, value]` pairs after a per-partition key-wise combine. In distributed mode the
    /// `wrapper` JS combiner is shipped and runs on workers, so only one partial per key per
    /// partition crosses the wire; in local mode the source rows are returned for the driver merge.
    fn map_side_combine(&mut self, wrapper: &str) -> Result<Vec<JsonValue>> {
        if self.context.is_distributed() {
            let saved_staged = self.staged.clone();
            self.stage_js_task(
                wrapper.to_string(),
                atomic_data::distributed::TaskAction::Map,
                None,
            )?;
            let partials = self.dispatch_and_collect();
            self.staged = saved_staged;
            partials
        } else {
            Ok(self.elements.clone())
        }
    }

    /// Merge `[key, value]` partials key-wise with `op` (driver side), preserving first-seen order.
    fn merge_partials(
        &self,
        partials: Vec<JsonValue>,
        op: impl Fn(JsonValue, JsonValue) -> JsonValue,
    ) -> Result<JsRdd> {
        let mut accum: HashMap<String, (JsonValue, JsonValue)> = HashMap::new();
        let mut order: Vec<String> = Vec::new();
        for pair in &partials {
            let (k, v) = Self::split_pair(pair)?;
            let key_str = Self::key_to_string(&k)?;
            match accum.get_mut(&key_str) {
                Some((_, acc)) => *acc = op(acc.clone(), v),
                None => {
                    order.push(key_str.clone());
                    accum.insert(key_str, (k, v));
                }
            }
        }
        let elements: Vec<JsonValue> = order
            .into_iter()
            .filter_map(|ks| accum.remove(&ks))
            .map(|(k, v)| JsonValue::Array(vec![k, v]))
            .collect();
        Ok(JsRdd::from_data(
            elements,
            self.num_partitions,
            Arc::clone(&self.context),
        ))
    }

    /// Ship `js_combiner` (`(a,b)=>…`) as a map-side per-key combine, then merge the partials on
    /// the driver with the equivalent `rust_op`.
    fn combine_values(
        &mut self,
        js_combiner: &str,
        rust_op: impl Fn(JsonValue, JsonValue) -> JsonValue,
    ) -> Result<JsRdd> {
        let wrapper = format!(
            "(partition) => {{ const g = new Map(), o = []; \
             for (const p of partition) {{ const k = JSON.stringify(p[0]); \
             if (g.has(k)) {{ g.set(k, [p[0], ({js_combiner})(g.get(k)[1], p[1])]); }} \
             else {{ g.set(k, [p[0], p[1]]); o.push(k); }} }} \
             return o.map(k => g.get(k)); }}"
        );
        let partials = self.map_side_combine(&wrapper)?;
        self.merge_partials(partials, rust_op)
    }
}

/// Wrap an `f64` as a JSON number, using an integer when the value is whole (so summing ints
/// yields an int, not `5.0`).
fn number_json(v: f64) -> JsonValue {
    if v.fract() == 0.0 && v.abs() < i64::MAX as f64 {
        JsonValue::from(v as i64)
    } else {
        JsonValue::from(v)
    }
}
