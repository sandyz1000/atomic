//! Interpreter for a [`JsDStreamInner`] tree: walks parent chains to materialize one
//! batch, applying each [`JsStreamTransform`] node along the way. Used by both
//! synchronous (`runOneBatch`) execution in
//! [`JsStreamingContext`](crate::streaming::context::JsStreamingContext).
//!
//! Elements are `serde_json::Value`. Pair elements are `[key, value]` JSON arrays.
//! State for `updateStateByKey` is stored as `serde_json::Value` per key.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use napi::bindgen_prelude::*;
use serde_json::Value as JV;

use super::dstream::{JsDStreamInner, JsStreamTransform};

pub(crate) fn compute_batch(
    env: &Env,
    inner: &JsDStreamInner,
    state_store: &mut HashMap<String, JV>,
) -> Result<Vec<JV>> {
    match inner {
        JsDStreamInner::Queue { queue } => {
            let mut q = queue.lock();
            Ok(q.pop_front().unwrap_or_default())
        }
        JsDStreamInner::Transform { parent, op } => {
            let parent_elems = compute_batch(env, parent, state_store)?;
            apply_transform(env, parent_elems, op, state_store)
        }
        JsDStreamInner::Windowed {
            parent,
            window_ms,
            buffer,
        } => {
            let batch = compute_batch(env, parent, state_store)?;
            let now = Instant::now();
            let cutoff = Duration::from_millis(*window_ms);
            let mut buf = buffer.lock();
            buf.push_back((now, batch));
            buf.retain(|(ts, _)| now.duration_since(*ts) <= cutoff);
            Ok(buf
                .iter()
                .flat_map(|(_, elems)| elems.iter().cloned())
                .collect())
        }
    }
}

fn apply_transform(
    env: &Env,
    elements: Vec<JV>,
    op: &JsStreamTransform,
    state_store: &mut HashMap<String, JV>,
) -> Result<Vec<JV>> {
    match op {
        JsStreamTransform::Map(f) => elements
            .into_iter()
            .map(|e| f.borrow_back(env)?.call(e))
            .collect(),

        JsStreamTransform::Filter(f) => elements
            .into_iter()
            .filter_map(
                |e| match f.borrow_back(env).and_then(|h| h.call(e.clone())) {
                    Ok(true) => Some(Ok(e)),
                    Ok(false) => None,
                    Err(err) => Some(Err(err)),
                },
            )
            .collect(),

        JsStreamTransform::FlatMap(f) => {
            let mut result = Vec::new();
            for e in elements {
                result.extend(f.borrow_back(env)?.call(e)?);
            }
            Ok(result)
        }

        JsStreamTransform::ReduceByKey(f) => {
            let mut groups: Vec<(JV, JV)> = Vec::new();
            for e in &elements {
                let pair = e
                    .as_array()
                    .ok_or_else(|| Error::from_reason("reduceByKey: element must be [k,v]"))?;
                if pair.len() != 2 {
                    return Err(Error::from_reason("reduceByKey: need 2-element array"));
                }
                let k = pair[0].clone();
                let v = pair[1].clone();
                let k_str = key_str(&k)?;
                let mut found = false;
                for (ek, acc) in &mut groups {
                    if key_str(ek)? == k_str {
                        *acc = f.borrow_back(env)?.call((acc.clone(), v.clone()).into())?;
                        found = true;
                        break;
                    }
                }
                if !found {
                    groups.push((k, v));
                }
            }
            Ok(groups
                .into_iter()
                .map(|(k, v)| serde_json::json!([k, v]))
                .collect())
        }

        JsStreamTransform::GroupByKey => {
            let mut groups: Vec<(JV, Vec<JV>)> = Vec::new();
            for e in &elements {
                let pair = e
                    .as_array()
                    .ok_or_else(|| Error::from_reason("groupByKey: element must be [k,v]"))?;
                if pair.len() != 2 {
                    return Err(Error::from_reason("groupByKey: need 2-element array"));
                }
                let k = pair[0].clone();
                let v = pair[1].clone();
                let k_str = key_str(&k)?;
                let mut found = false;
                for (ek, vals) in &mut groups {
                    if key_str(ek)? == k_str {
                        vals.push(v.clone());
                        found = true;
                        break;
                    }
                }
                if !found {
                    groups.push((k, vec![v]));
                }
            }
            Ok(groups
                .into_iter()
                .map(|(k, vals)| serde_json::json!([k, vals]))
                .collect())
        }

        JsStreamTransform::Join(right_inner) => {
            let mut dummy: HashMap<String, JV> = HashMap::new();
            let right_elems = compute_batch(env, right_inner, &mut dummy)?;
            let mut result = Vec::new();
            for le in &elements {
                let lp = le
                    .as_array()
                    .ok_or_else(|| Error::from_reason("join: left must be [k,v]"))?;
                let lk = &lp[0];
                let lv = &lp[1];
                let lk_str = key_str(lk)?;
                for re in &right_elems {
                    let rp = re
                        .as_array()
                        .ok_or_else(|| Error::from_reason("join: right must be [k,v]"))?;
                    let rk = &rp[0];
                    let rv = &rp[1];
                    if key_str(rk)? == lk_str {
                        result.push(serde_json::json!([lk, [lv, rv]]));
                    }
                }
            }
            Ok(result)
        }

        JsStreamTransform::LeftOuterJoin(right_inner) => {
            let mut dummy: HashMap<String, JV> = HashMap::new();
            let right_elems = compute_batch(env, right_inner, &mut dummy)?;
            let mut result = Vec::new();
            for le in &elements {
                let lp = le
                    .as_array()
                    .ok_or_else(|| Error::from_reason("leftOuterJoin: left must be [k,v]"))?;
                let lk = &lp[0];
                let lv = &lp[1];
                let lk_str = key_str(lk)?;
                let mut matched = false;
                for re in &right_elems {
                    let rp = re
                        .as_array()
                        .ok_or_else(|| Error::from_reason("leftOuterJoin: right must be [k,v]"))?;
                    let rk = &rp[0];
                    let rv = &rp[1];
                    if key_str(rk)? == lk_str {
                        result.push(serde_json::json!([lk, [lv, rv]]));
                        matched = true;
                    }
                }
                if !matched {
                    result.push(serde_json::json!([lk, [lv, JV::Null]]));
                }
            }
            Ok(result)
        }

        JsStreamTransform::UpdateStateByKey(f) => {
            let mut new_vals: Vec<(String, JV, Vec<JV>)> = Vec::new();
            for e in &elements {
                let pair = e
                    .as_array()
                    .ok_or_else(|| Error::from_reason("updateStateByKey: element must be [k,v]"))?;
                let k = &pair[0];
                let v = pair[1].clone();
                let k_str = key_str(k)?;
                let mut found = false;
                for (ks, _, vals) in &mut new_vals {
                    if *ks == k_str {
                        vals.push(v.clone());
                        found = true;
                        break;
                    }
                }
                if !found {
                    new_vals.push((k_str, k.clone(), vec![v]));
                }
            }

            let mut all_keys: Vec<(String, JV)> = new_vals
                .iter()
                .map(|(ks, k, _)| (ks.clone(), k.clone()))
                .collect();
            for k_str in state_store.keys() {
                if !new_vals.iter().any(|(ks, _, _)| ks == k_str) {
                    all_keys.push((k_str.clone(), JV::String(k_str.clone())));
                }
            }

            let mut result = Vec::new();
            for (key_str_val, key_obj) in &all_keys {
                let new_values_for_key: Vec<JV> = new_vals
                    .iter()
                    .find(|(ks, _, _)| ks == key_str_val)
                    .map(|(_, _, v)| v.clone())
                    .unwrap_or_default();
                let old_state = state_store.get(key_str_val).cloned();
                let new_state = f
                    .borrow_back(env)?
                    .call((new_values_for_key, old_state).into())?;
                match new_state {
                    None => {
                        state_store.remove(key_str_val);
                    }
                    Some(ns) => {
                        state_store.insert(key_str_val.clone(), ns.clone());
                        result.push(serde_json::json!([key_obj, ns]));
                    }
                }
            }
            Ok(result)
        }

        JsStreamTransform::MapValues(f) => elements
            .into_iter()
            .map(|e| {
                let pair = e
                    .as_array()
                    .ok_or_else(|| Error::from_reason("mapValues: element must be [k,v]"))?;
                if pair.len() != 2 {
                    return Err(Error::from_reason("mapValues: need 2-element array"));
                }
                let new_v = f.borrow_back(env)?.call(pair[1].clone())?;
                Ok(serde_json::json!([pair[0], new_v]))
            })
            .collect(),
    }
}

pub(crate) fn key_str(val: &JV) -> Result<String> {
    match val {
        JV::String(s) => Ok(s.clone()),
        JV::Number(n) => Ok(n.to_string()),
        JV::Bool(b) => Ok(b.to_string()),
        other => Ok(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::dstream::{JsDStreamInner, JsStreamTransform};
    use parking_lot::Mutex;
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::Arc;

    // Handle to a queue-backed DStream's shared batch buffer.
    type QueueHandle = Arc<Mutex<VecDeque<Vec<JV>>>>;

    // SAFETY: all tested variants (Queue/GroupByKey/Join/Windowed) are pure Rust
    // and never dereference `env`. The zeroed pointer is never passed to the JS runtime.
    fn compute_batch(
        inner: &Arc<JsDStreamInner>,
        state: &mut HashMap<String, JV>,
    ) -> napi::Result<Vec<JV>> {
        let env: napi::Env = unsafe { std::mem::zeroed() };
        super::compute_batch(&env, inner, state)
    }

    // Helper: create a Queue-backed DStreamInner directly.
    fn make_queue(batches: Vec<Vec<JV>>) -> (Arc<JsDStreamInner>, QueueHandle) {
        let q: Arc<Mutex<VecDeque<Vec<JV>>>> = Arc::new(Mutex::new(batches.into_iter().collect()));
        let inner = Arc::new(JsDStreamInner::Queue {
            queue: Arc::clone(&q),
        });
        (inner, q)
    }

    fn empty_state() -> HashMap<String, JV> {
        HashMap::new()
    }

    // --- key_str: covers all variant arms ---
    // key_str is pure Rust; it is called on every key in the pair transforms.

    #[test]
    fn key_str_string() {
        assert_eq!(key_str(&json!("hello")).unwrap(), "hello");
    }

    #[test]
    fn key_str_integer() {
        assert_eq!(key_str(&json!(42)).unwrap(), "42");
    }

    #[test]
    fn key_str_negative() {
        assert_eq!(key_str(&json!(-7)).unwrap(), "-7");
    }

    #[test]
    fn key_bool_true() {
        assert_eq!(key_str(&json!(true)).unwrap(), "true");
    }

    #[test]
    fn key_bool_false() {
        assert_eq!(key_str(&json!(false)).unwrap(), "false");
    }

    #[test]
    fn key_str_null() {
        // null falls into the `other => Ok(other.to_string())` arm.
        assert_eq!(key_str(&json!(null)).unwrap(), "null");
    }

    // --- Queue DStream ---

    #[test]
    fn queue_empty_batch() {
        let (inner, _q) = make_queue(vec![]);
        let batch = compute_batch(&inner, &mut empty_state()).unwrap();
        assert!(batch.is_empty());
    }

    #[test]
    fn queue_pops_batch() {
        let (inner, _q) = make_queue(vec![vec![json!(1), json!(2)], vec![json!(3)]]);
        let b1 = compute_batch(&inner, &mut empty_state()).unwrap();
        let b2 = compute_batch(&inner, &mut empty_state()).unwrap();
        let b3 = compute_batch(&inner, &mut empty_state()).unwrap();
        assert_eq!(b1, vec![json!(1), json!(2)]);
        assert_eq!(b2, vec![json!(3)]);
        assert!(b3.is_empty());
    }

    // --- GroupByKey transform (no JS callback — pure Rust) ---

    #[test]
    fn group_by_key() {
        let pairs = vec![json!(["a", 1]), json!(["b", 2]), json!(["a", 3])];
        let (inner, _q) = make_queue(vec![pairs]);
        let grouped_inner = Arc::new(JsDStreamInner::Transform {
            parent: inner,
            op: JsStreamTransform::GroupByKey,
        });
        let result = compute_batch(&grouped_inner, &mut empty_state()).unwrap();
        // Should have two keys: "a" and "b"
        assert_eq!(result.len(), 2);
        let a_entry = result.iter().find(|e| e[0] == json!("a")).unwrap();
        let b_entry = result.iter().find(|e| e[0] == json!("b")).unwrap();
        let a_vals: Vec<JV> = serde_json::from_value(a_entry[1].clone()).unwrap();
        let b_vals: Vec<JV> = serde_json::from_value(b_entry[1].clone()).unwrap();
        assert_eq!(a_vals, vec![json!(1), json!(3)]);
        assert_eq!(b_vals, vec![json!(2)]);
    }

    #[test]
    fn group_single_elem() {
        let pairs = vec![json!(["x", 99])];
        let (inner, _q) = make_queue(vec![pairs]);
        let grouped = Arc::new(JsDStreamInner::Transform {
            parent: inner,
            op: JsStreamTransform::GroupByKey,
        });
        let result = compute_batch(&grouped, &mut empty_state()).unwrap();
        assert_eq!(result, vec![json!(["x", [99]])]);
    }

    #[test]
    fn group_non_pair() {
        let pairs = vec![json!("not_a_pair")];
        let (inner, _q) = make_queue(vec![pairs]);
        let grouped = Arc::new(JsDStreamInner::Transform {
            parent: inner,
            op: JsStreamTransform::GroupByKey,
        });
        assert!(compute_batch(&grouped, &mut empty_state()).is_err());
    }

    // --- Join transform (no JS callback — pure Rust) ---

    #[test]
    fn join_matching_pairs() {
        let left = vec![json!(["k", "lv"])];
        let right = vec![json!(["k", "rv"])];
        let (left_inner, _) = make_queue(vec![left]);
        let (right_inner, _) = make_queue(vec![right]);
        let joined = Arc::new(JsDStreamInner::Transform {
            parent: left_inner,
            op: JsStreamTransform::Join(right_inner),
        });
        let result = compute_batch(&joined, &mut empty_state()).unwrap();
        assert_eq!(result, vec![json!(["k", ["lv", "rv"]])]);
    }

    #[test]
    fn join_no_match() {
        let left = vec![json!(["a", 1])];
        let right = vec![json!(["b", 2])];
        let (left_inner, _) = make_queue(vec![left]);
        let (right_inner, _) = make_queue(vec![right]);
        let joined = Arc::new(JsDStreamInner::Transform {
            parent: left_inner,
            op: JsStreamTransform::Join(right_inner),
        });
        let result = compute_batch(&joined, &mut empty_state()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn join_multi_right() {
        let left = vec![json!(["k", "lv"])];
        let right = vec![json!(["k", "r1"]), json!(["k", "r2"])];
        let (left_inner, _) = make_queue(vec![left]);
        let (right_inner, _) = make_queue(vec![right]);
        let joined = Arc::new(JsDStreamInner::Transform {
            parent: left_inner,
            op: JsStreamTransform::Join(right_inner),
        });
        let result = compute_batch(&joined, &mut empty_state()).unwrap();
        assert_eq!(result.len(), 2);
    }

    // --- LeftOuterJoin transform (no JS callback — pure Rust) ---

    #[test]
    fn loj_match_present() {
        let left = vec![json!(["k", "lv"])];
        let right = vec![json!(["k", "rv"])];
        let (left_inner, _) = make_queue(vec![left]);
        let (right_inner, _) = make_queue(vec![right]);
        let joined = Arc::new(JsDStreamInner::Transform {
            parent: left_inner,
            op: JsStreamTransform::LeftOuterJoin(right_inner),
        });
        let result = compute_batch(&joined, &mut empty_state()).unwrap();
        assert_eq!(result, vec![json!(["k", ["lv", "rv"]])]);
    }

    #[test]
    fn loj_match_null() {
        let left = vec![json!(["a", "lv"])];
        let right = vec![json!(["b", "rv"])];
        let (left_inner, _) = make_queue(vec![left]);
        let (right_inner, _) = make_queue(vec![right]);
        let joined = Arc::new(JsDStreamInner::Transform {
            parent: left_inner,
            op: JsStreamTransform::LeftOuterJoin(right_inner),
        });
        let result = compute_batch(&joined, &mut empty_state()).unwrap();
        assert_eq!(result, vec![json!(["a", ["lv", null]])]);
    }

    // --- Windowed DStream ---

    #[test]
    fn window_accumulates() {
        let (src, q) = make_queue(vec![]);
        let windowed = Arc::new(JsDStreamInner::Windowed {
            parent: src,
            window_ms: 60_000,
            buffer: Mutex::new(VecDeque::new()),
        });
        // Push two batches and check that both are included in the window.
        q.lock().push_back(vec![json!(1), json!(2)]);
        let b1 = compute_batch(&windowed, &mut empty_state()).unwrap();
        assert_eq!(b1, vec![json!(1), json!(2)]);

        q.lock().push_back(vec![json!(3)]);
        let b2 = compute_batch(&windowed, &mut empty_state()).unwrap();
        // Window is 60s; both batches are still fresh, so result includes all 3.
        assert_eq!(b2.len(), 3);
    }

    #[test]
    fn window_zero_evicts() {
        let (src, q) = make_queue(vec![]);
        let windowed = Arc::new(JsDStreamInner::Windowed {
            parent: src,
            window_ms: 0, // zero-length window: only items from this exact instant
            buffer: Mutex::new(VecDeque::new()),
        });
        q.lock().push_back(vec![json!(1)]);
        let _b1 = compute_batch(&windowed, &mut empty_state()).unwrap();

        q.lock().push_back(vec![json!(2)]);
        let b2 = compute_batch(&windowed, &mut empty_state()).unwrap();
        // With 0ms window, the previous batch was already older → evicted.
        // Result contains only items from the current tick.
        assert_eq!(b2, vec![json!(2)]);
    }

    // --- Confirm the stored callback refs are Send + Sync (they travel across
    // threads inside JsDStreamInner via Arc). ---
    fn _assert_send_sync<T: Send + Sync>() {}
    #[test]
    fn transform_send_sync() {
        _assert_send_sync::<JsStreamTransform>();
    }
}
