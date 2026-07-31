//! Map-side pre-combine dispatch: the worker handler logic plus the driver-side
//! `TypeId` reflection registry, owned together in one file — mirroring [`super::shuffle`],
//! which pairs the shuffle-map handler with its `SHUFFLE_KEY_REGISTRY`.
//!
//! A [`EngineAction::CombineByKey`](atomic_data::distributed::EngineAction::CombineByKey)
//! step runs on the worker immediately before `ShuffleMap` when a `_task` pipeline precedes
//! a keyed shuffle in distributed mode. It groups same-key values within one map partition
//! and folds each group down with the registered merge task, so fewer, pre-combined pairs
//! cross the network. It is strictly opt-in: nothing runs unless a combine handler was
//! registered for the concrete `(K, V)` (or `(K, V, C)`) type.
//!
//! # Two shapes, mirroring the RDD ops that emit them
//!
//! - **`C == V`** ([`combine_handler`], from `register_combine!(K, V)`) —
//!   `reduce_by_key_task` / `fold_by_key_task`. Each key's `Vec<V>` is folded to one `V` via
//!   the merge task's [`TaskAction::Reduce`]; the shuffle wire type stays `V`, so the reduce
//!   side is untouched (summing partial sums is transparent to `create_combiner`/`merge_value`).
//! - **`C != V`** ([`combine_lift_handler`], from `register_combine_lift!(K, V, C)`)
//!   — `aggregate_by_key_task`. The partition's values are lifted `V -> C` in one
//!   [`TaskAction::Map`] call, grouped by key, and each group folded to one `C`. The shuffle
//!   wire type becomes `(K, C)`, so the reduce side fetches `(K, C)` and merges via
//!   `merge_combiners` (see `ShuffledRdd::compute`'s `map_side_combined` branch).
//!
//! # Dispatch
//!
//! Both handlers are ordinary [`TaskEntry`]s in the shared [`TASK_REGISTRY`], keyed by the
//! `combine_key` string the driver embeds in the step. `NativeDispatcher` repacks the
//! per-call lift/merge task names into a [`CombineCtx`] and passes it as the handler's
//! `payload` — the same "dispatcher packs a small ctx into `payload`" pattern
//! [`ShuffleWriteCtx`](super::shuffle::ShuffleWriteCtx) uses, since `TaskHandlerFn`'s
//! signature has no dedicated slot for it.

use std::any::TypeId;
use std::collections::HashMap;
use std::hash::Hash;

use atomic_data::data::Data;
use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
use once_cell::sync::Lazy;

use crate::registry::TASK_REGISTRY;

/// Per-invocation lift/merge task names, packed by `NativeDispatcher` into the handler's
/// `payload` from the `CombineByKey` step (whose `combine_key` already selected the handler).
/// `lift_task_name` is `Some` only for the `C != V` path.
#[derive(bincode::Encode, bincode::Decode)]
pub struct CombineCtx {
    pub lift_task_name: Option<String>,
    pub merge_task_name: String,
}

/// Maps a concrete combine `TypeId` to its stable dispatch key — the driver-side reflection
/// table for the opt-in check, mirroring [`SHUFFLE_KEY_REGISTRY`](super::SHUFFLE_KEY_REGISTRY).
///
/// The `C == V` macro submits `TypeId::of::<(K, V)>()`; the `C != V` macro submits
/// `TypeId::of::<(K, V, C)>()`. Those are distinct `TypeId`s, so one registry serves both
/// without collision — the driver queries whichever tuple matches the op it is building.
pub struct CombineKeyEntry {
    /// Returns `TypeId::of::<(K, V)>()` (C == V) or `TypeId::of::<(K, V, C)>()` (C != V).
    pub type_id: fn() -> TypeId,
    /// The stable dispatch key, also the `TASK_REGISTRY` key of the combine handler.
    pub key: &'static str,
}

inventory::collect!(CombineKeyEntry);

/// Global map from a combine `TypeId` to its dispatch key string. Queried on the driver by
/// [`combine_handler_registered`] to decide whether to insert a `CombineByKey` step.
pub static COMBINE_KEY_REGISTRY: Lazy<HashMap<TypeId, &'static str>> = Lazy::new(|| {
    inventory::iter::<CombineKeyEntry>
        .into_iter()
        .map(|entry| ((entry.type_id)(), entry.key))
        .collect()
});

/// Return the registered combine dispatch key for `type_id`, or `None` if the current binary
/// did not register a combine handler for it. Since driver and worker are the same binary,
/// a hit here proves the worker can run the corresponding `CombineByKey` step.
///
/// Callers pass `TypeId::of::<(K, V)>()` for the `C == V` case, `TypeId::of::<(K, V, C)>()`
/// for the `C != V` case.
pub fn combine_handler_registered(type_id: TypeId) -> Option<&'static str> {
    COMBINE_KEY_REGISTRY.get(&type_id).copied()
}

fn decode_ctx(payload: &[u8]) -> Result<CombineCtx, String> {
    bincode::decode_from_slice(payload, bincode::config::standard())
        .map(|(ctx, _)| ctx)
        .map_err(|e| format!("combine ctx decode: {e}"))
}

/// Group `pairs` by key into `Vec<(K, Vec<T>)>`, preserving first-seen key order so the
/// output is deterministic regardless of `HashMap` iteration order.
fn group_by_key<K, T>(pairs: impl IntoIterator<Item = (K, T)>) -> Vec<(K, Vec<T>)>
where
    K: Clone + Eq + Hash,
{
    let mut order: Vec<(K, Vec<T>)> = Vec::new();
    let mut index: HashMap<K, usize> = HashMap::new();
    for (k, t) in pairs {
        match index.get(&k) {
            Some(&i) => order[i].1.push(t),
            None => {
                index.insert(k.clone(), order.len());
                order.push((k, vec![t]));
            }
        }
    }
    order
}

/// `TaskHandlerFn` for the `C == V` case (`reduce_by_key_task` / `fold_by_key_task`).
///
/// Decodes `Vec<(K, V)>`, groups by key, folds each key's `Vec<V>` to one `V` via the merge
/// task's [`TaskAction::Reduce`], and re-encodes the (fewer) `(K, V)` pairs. The wire type is
/// unchanged, so the reduce side needs no changes. Registered under the `"K::V"` combine key
/// by `register_combine!(K, V)`.
pub fn combine_handler<K, V>(
    _action: &TaskAction,
    payload: &[u8],
    data: &[u8],
) -> Result<Vec<u8>, String>
where
    K: Data + Clone + Eq + Hash,
    V: Data + Clone + WireDecode,
    Vec<(K, V)>: WireDecode + WireEncode,
    Vec<V>: WireEncode,
{
    let ctx = decode_ctx(payload)?;
    let merge = TASK_REGISTRY
        .get(ctx.merge_task_name.as_str())
        .ok_or_else(|| {
            format!(
                "combine_handler: merge task '{}' not registered",
                ctx.merge_task_name
            )
        })?;

    let pairs = Vec::<(K, V)>::decode_wire(data)
        .map_err(|e| format!("combine_handler: decode input: {e}"))?;

    let groups = group_by_key(pairs);
    let mut out: Vec<(K, V)> = Vec::with_capacity(groups.len());
    for (k, group) in groups {
        let encoded = group
            .encode_wire()
            .map_err(|e| format!("combine_handler: encode group: {e}"))?;
        let reduced = merge.call(&TaskAction::Reduce, &[], &encoded)?;
        let v = V::decode_wire(&reduced)
            .map_err(|e| format!("combine_handler: decode reduced value: {e}"))?;
        out.push((k, v));
    }
    out.encode_wire()
        .map_err(|e| format!("combine_handler: encode output: {e}"))
}

/// `TaskHandlerFn` for the `C != V` case (`aggregate_by_key_task`).
///
/// Decodes `Vec<(K, V)>`, lifts the whole partition's values `V -> C` in one
/// [`TaskAction::Map`] call, groups the resulting `C`s by key, and folds each group to one
/// `C` via the merge task's [`TaskAction::Reduce`]. Emits `Vec<(K, C)>` — the shuffle then
/// carries `(K, C)`, and the reduce side merges via `merge_combiners`. Registered under the
/// `"K::V::C"` combine key by `register_combine_lift!(K, V, C)`.
pub fn combine_lift_handler<K, V, C>(
    _action: &TaskAction,
    payload: &[u8],
    data: &[u8],
) -> Result<Vec<u8>, String>
where
    K: Data + Clone + Eq + Hash,
    V: Data + Clone,
    C: Data + Clone + WireDecode,
    Vec<(K, V)>: WireDecode,
    Vec<(K, C)>: WireEncode,
    Vec<V>: WireEncode,
    Vec<C>: WireEncode + WireDecode,
{
    let ctx = decode_ctx(payload)?;
    let lift_name = ctx
        .lift_task_name
        .as_deref()
        .ok_or_else(|| "combine_lift_handler: lift task name missing".to_string())?;
    let lift = TASK_REGISTRY
        .get(lift_name)
        .ok_or_else(|| format!("combine_lift_handler: lift task '{lift_name}' not registered"))?;
    let merge = TASK_REGISTRY
        .get(ctx.merge_task_name.as_str())
        .ok_or_else(|| {
            format!(
                "combine_lift_handler: merge task '{}' not registered",
                ctx.merge_task_name
            )
        })?;

    let pairs = Vec::<(K, V)>::decode_wire(data)
        .map_err(|e| format!("combine_lift_handler: decode input: {e}"))?;
    let (keys, values): (Vec<K>, Vec<V>) = pairs.into_iter().unzip();

    // Lift the whole partition's values V -> C in one Map call, then zip back with keys.
    let values_enc = values
        .encode_wire()
        .map_err(|e| format!("combine_lift_handler: encode values: {e}"))?;
    let lifted_bytes = lift.call(&TaskAction::Map, &[], &values_enc)?;
    let lifted = Vec::<C>::decode_wire(&lifted_bytes)
        .map_err(|e| format!("combine_lift_handler: decode lifted: {e}"))?;

    let groups = group_by_key(keys.into_iter().zip(lifted));
    let mut out: Vec<(K, C)> = Vec::with_capacity(groups.len());
    for (k, group) in groups {
        let encoded = group
            .encode_wire()
            .map_err(|e| format!("combine_lift_handler: encode group: {e}"))?;
        let reduced = merge.call(&TaskAction::Reduce, &[], &encoded)?;
        let c = C::decode_wire(&reduced)
            .map_err(|e| format!("combine_lift_handler: decode reduced combiner: {e}"))?;
        out.push((k, c));
    }
    out.encode_wire()
        .map_err(|e| format!("combine_lift_handler: encode output: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::TaskEntry;

    // Manual `TaskEntry`s stand in for `#[task]`-registered merge/lift tasks so the handler
    // logic can be exercised directly, without the full distributed stack.
    fn merge_i32(_a: &TaskAction, _p: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
        let items = Vec::<i32>::decode_wire(data).map_err(|e| e.to_string())?;
        let mut iter = items.into_iter();
        let first = iter.next().ok_or_else(|| "empty".to_string())?;
        iter.fold(first, |a, b| a + b)
            .encode_wire()
            .map_err(|e| e.to_string())
    }

    fn lift_sum_count(_a: &TaskAction, _p: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
        let items = Vec::<i32>::decode_wire(data).map_err(|e| e.to_string())?;
        let out: Vec<(i64, u64)> = items.into_iter().map(|v| (v as i64, 1u64)).collect();
        out.encode_wire().map_err(|e| e.to_string())
    }

    fn merge_sum_count(_a: &TaskAction, _p: &[u8], data: &[u8]) -> Result<Vec<u8>, String> {
        let items = Vec::<(i64, u64)>::decode_wire(data).map_err(|e| e.to_string())?;
        let acc = items
            .into_iter()
            .reduce(|a, b| (a.0 + b.0, a.1 + b.1))
            .ok_or_else(|| "empty".to_string())?;
        acc.encode_wire().map_err(|e| e.to_string())
    }

    inventory::submit! { TaskEntry { task_name: "test::combine::merge_i32", body_hash: 0, handler: merge_i32 } }
    inventory::submit! { TaskEntry { task_name: "test::combine::lift_sum_count", body_hash: 0, handler: lift_sum_count } }
    inventory::submit! { TaskEntry { task_name: "test::combine::merge_sum_count", body_hash: 0, handler: merge_sum_count } }

    fn ctx_payload(lift: Option<&str>, merge: &str) -> Vec<u8> {
        let ctx = CombineCtx {
            lift_task_name: lift.map(str::to_string),
            merge_task_name: merge.to_string(),
        };
        bincode::encode_to_vec(&ctx, bincode::config::standard()).unwrap()
    }

    #[test]
    fn combine_cv_ordered() {
        let pairs: Vec<(String, i32)> = vec![
            ("b".into(), 1),
            ("a".into(), 10),
            ("b".into(), 2),
            ("a".into(), 20),
            ("b".into(), 3),
        ];
        let data = pairs.encode_wire().unwrap();
        let payload = ctx_payload(None, "test::combine::merge_i32");

        let out_bytes = combine_handler::<String, i32>(&TaskAction::Map, &payload, &data).unwrap();
        let out = Vec::<(String, i32)>::decode_wire(&out_bytes).unwrap();

        // One pair per key; first-seen order ("b" before "a"); values summed.
        assert_eq!(out, vec![("b".into(), 6), ("a".into(), 30)]);
    }

    #[test]
    fn combine_cv_single() {
        let pairs: Vec<(String, i32)> = vec![("solo".into(), 42)];
        let data = pairs.encode_wire().unwrap();
        let payload = ctx_payload(None, "test::combine::merge_i32");

        let out_bytes = combine_handler::<String, i32>(&TaskAction::Map, &payload, &data).unwrap();
        let out = Vec::<(String, i32)>::decode_wire(&out_bytes).unwrap();
        assert_eq!(out, vec![("solo".into(), 42)]);
    }

    #[test]
    fn combine_lift_reduce() {
        let pairs: Vec<(String, i32)> = vec![
            ("a".into(), 4),
            ("b".into(), 100),
            ("a".into(), 6),
            ("a".into(), 10),
        ];
        let data = pairs.encode_wire().unwrap();
        let payload = ctx_payload(
            Some("test::combine::lift_sum_count"),
            "test::combine::merge_sum_count",
        );

        let out_bytes =
            combine_lift_handler::<String, i32, (i64, u64)>(&TaskAction::Map, &payload, &data)
                .unwrap();
        let out = Vec::<(String, (i64, u64))>::decode_wire(&out_bytes).unwrap();

        // "a": (4+6+10, 3 values); "b": (100, 1 value). First-seen order preserved.
        assert_eq!(out, vec![("a".into(), (20, 3)), ("b".into(), (100, 1))]);
    }

    #[test]
    fn combine_missing_merge() {
        let pairs: Vec<(String, i32)> = vec![("a".into(), 1)];
        let data = pairs.encode_wire().unwrap();
        let payload = ctx_payload(None, "test::combine::does_not_exist");
        let err = combine_handler::<String, i32>(&TaskAction::Map, &payload, &data).unwrap_err();
        assert!(err.contains("not registered"), "unexpected error: {err}");
    }
}
