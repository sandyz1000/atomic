use std::sync::Arc;

use atomic_data::distributed::TaskAction;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde_json::Value as JsonValue;

use super::JsRdd;

#[napi]
impl JsRdd {
    /// Apply `f` to each element, returning a new RDD.
    #[napi]
    pub fn map(&mut self, f: Function<JsonValue, JsonValue>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!(
                    "(partition) => partition.map((x) => ({})(x))",
                    Self::fn_to_source(&f)?
                ),
                TaskAction::Map,
                None,
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
    pub fn filter(&mut self, f: Function<JsonValue, bool>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!(
                    "(partition) => partition.filter((x) => ({})(x))",
                    Self::fn_to_source(&f)?
                ),
                TaskAction::Filter,
                None,
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
    pub fn flat_map(&mut self, f: Function<JsonValue, Vec<JsonValue>>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!(
                    "(partition) => partition.flatMap((x) => ({})(x))",
                    Self::fn_to_source(&f)?
                ),
                TaskAction::FlatMap,
                None,
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
    pub fn map_values(&mut self, f: Function<JsonValue, JsonValue>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!(
                    "(partition) => partition.map((p) => [p[0], ({})(p[1])])",
                    Self::fn_to_source(&f)?
                ),
                TaskAction::Map,
                None,
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
    pub fn flat_map_values(&mut self, f: Function<JsonValue, Vec<JsonValue>>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!(
                    "(partition) => partition.flatMap((p) => ({})(p[1]).map((v) => [p[0], v]))",
                    Self::fn_to_source(&f)?
                ),
                TaskAction::FlatMap,
                None,
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
    pub fn key_by(&mut self, f: Function<JsonValue, JsonValue>) -> Result<JsRdd> {
        if self.context.is_distributed() {
            self.stage_js_udf(
                format!(
                    "(partition) => partition.map((x) => [({})(x), x])",
                    Self::fn_to_source(&f)?
                ),
                TaskAction::Map,
                None,
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
    pub fn group_by(&mut self, f: Function<JsonValue, JsonValue>) -> Result<JsRdd> {
        let mut keyed = self.key_by(f)?;
        keyed.group_by_key()
    }

    /// Apply `f` to each logical partition (array of elements), returning a flattened RDD.
    #[napi]
    pub fn map_partitions(&self, f: Function<Vec<JsonValue>, Vec<JsonValue>>) -> Result<JsRdd> {
        let np = self.num_partitions.max(1);
        let total = self.elements.len();
        let chunk_size = total.div_ceil(np).max(1);
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
}
