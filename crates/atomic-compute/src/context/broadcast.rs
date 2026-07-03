use std::sync::Arc;

use atomic_data::accumulator::{Accumulator, MergeFn, make_merge_fn, next_accumulator_id};
use atomic_data::broadcast::{BroadcastVar, next_broadcast_id};

use super::Context;

impl Context {
    /// Create a broadcast variable.
    ///
    /// The value is rkyv-encoded on the driver and embedded in every `TaskEnvelope`
    /// dispatched to workers. Workers call `broadcast_var.value()` inside `#[task]`
    /// functions to read the data without re-serializing per element.
    ///
    /// In local mode the broadcast value is still loaded into the thread-local registry
    /// before each task, so the same `#[task]` code works in both modes.
    pub fn broadcast<T>(self: &Arc<Self>, value: T) -> BroadcastVar<T>
    where
        T: atomic_data::distributed::WireEncode + atomic_data::distributed::WireDecode,
    {
        let id = next_broadcast_id();
        let bytes = value.encode_wire().expect("broadcast: encode failed");
        self.broadcast_store.insert(id, bytes);
        BroadcastVar::new(id)
    }

    /// Return a snapshot of all broadcast values as `(id, bytes)` pairs.
    /// Used by `dispatch_pipeline` and `run_shuffle_map_stage` to attach broadcasts
    /// to outgoing `TaskEnvelope`s.
    pub fn broadcast_snapshot(&self) -> Vec<(usize, Vec<u8>)> {
        self.broadcast_store
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }

    /// Create an accumulator with an initial value and an associative merge function.
    ///
    /// Workers call `acc.add(delta)` inside `#[task]` functions. The driver merges
    /// per-task deltas by calling `merge(current, delta)` after every task result.
    /// Read the current driver-side value with `Context::accumulator_value(&acc)`.
    pub fn accumulator<T, F>(self: &Arc<Self>, init: T, merge: F) -> Accumulator<T>
    where
        T: atomic_data::distributed::WireEncode + atomic_data::distributed::WireDecode + 'static,
        F: Fn(T, T) -> T + Send + Sync + 'static,
    {
        let id = next_accumulator_id();
        let bytes = init.encode_wire().expect("accumulator: encode init failed");
        let merge_fn: Arc<MergeFn> = Arc::new(make_merge_fn::<T, F>(merge));
        self.accumulator_store.insert(id, (bytes, merge_fn));
        Accumulator::new(id)
    }

    /// Read the current driver-side value of an accumulator.
    pub fn accumulator_value<T>(&self, acc: &Accumulator<T>) -> T
    where
        T: atomic_data::distributed::WireDecode,
    {
        let entry = self.accumulator_store.get(&acc.id).unwrap_or_else(|| {
            panic!(
                "accumulator id={} is not registered on this context; \
                 ensure the accumulator was created from the same Context",
                acc.id
            )
        });
        T::decode_wire(&entry.value().0).unwrap_or_else(|e| {
            panic!(
                "accumulator id={}: failed to decode current value: {}",
                acc.id, e
            )
        })
    }

    /// Merge incoming accumulator deltas from a completed task result into driver-side values.
    pub fn merge_accumulator_deltas(&self, deltas: &[(usize, Vec<u8>)]) {
        merge_deltas_into(&self.accumulator_store, deltas);
    }
}

/// Store-level delta merge, shared by the context method and the
/// `DistributedScheduler`'s accumulator sink (which captures the store by `Arc`
/// before the `Context` exists).
pub(crate) fn merge_deltas_into(store: &super::AccumulatorStore, deltas: &[(usize, Vec<u8>)]) {
    for (id, delta_bytes) in deltas {
        if let Some(mut entry) = store.get_mut(id) {
            let merge_fn = entry.value().1.clone();
            match merge_fn(entry.value().0.clone(), delta_bytes.clone()) {
                Ok(new_bytes) => entry.value_mut().0 = new_bytes,
                Err(e) => log::error!("accumulator id={id}: dropping bad delta: {e}"),
            }
        }
    }
}
