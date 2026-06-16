use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde_json::Value as JsonValue;

use super::JsRdd;

#[napi]
impl JsRdd {
    /// Return the top `n` elements (largest first). Optional comparator `f(a, b) => number`.
    #[napi]
    pub fn top(
        &self,
        n: u32,
        comparator: Option<Function<FnArgs<(JsonValue, JsonValue)>, f64>>,
    ) -> Result<Vec<JsonValue>> {
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
        comparator: Option<Function<FnArgs<(JsonValue, JsonValue)>, f64>>,
    ) -> Result<Vec<JsonValue>> {
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

    /// Sort elements by a key extracted via `key_fn(element)`.
    ///
    /// `ascending` defaults to `true`. The key function is called on each
    /// element; results are compared using JSON ordering (numbers numerically,
    /// strings lexicographically).
    #[napi]
    pub fn sort_by(
        &self,
        key_fn: Function<JsonValue, JsonValue>,
        ascending: Option<bool>,
    ) -> Result<JsRdd> {
        let asc = ascending.unwrap_or(true);
        let mut keyed: Vec<(JsonValue, JsonValue)> = self
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
}
