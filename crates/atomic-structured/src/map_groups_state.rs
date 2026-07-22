//! Arbitrary per-group stateful processing — the engine core behind
//! `map_groups_with_state` / `flat_map_groups_with_state`.
//!
//! Unlike the mergeable [`AggState`](crate::state::AggState) store, this operator holds a
//! *user-defined* state value `S` per group and drives it with a user update function. Each
//! batch, the rows for a group plus its current state are handed to the callback, which mutates
//! the state (via [`GroupState`]) and returns zero or more output rows. Groups can arm a timeout
//! and are revisited — with no input rows and [`GroupState::has_timed_out`] set — once the
//! processing-time or event-time clock passes it.
//!
//! The operator is transport-agnostic (generic over the row and output types), so the query
//! layer decides how to extract group keys and rows from Arrow batches and how to encode the
//! outputs. State is checkpointable whenever `S: bincode::Encode + bincode::Decode`.

use std::collections::HashMap;

use crate::errors::{StructuredError, StructuredResult};
use crate::state::GroupVal;

/// When a group's armed timeout is measured against the wall clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupStateTimeout {
    /// Timeouts are disabled; `set_timeout_timestamp_ms` is ignored.
    None,
    /// Timeout timestamps are compared against processing time (wall clock).
    ProcessingTime,
    /// Timeout timestamps are compared against the event-time watermark.
    EventTime,
}

/// A mutable handle to one group's state, passed to the user update function.
///
/// The function inspects the current value ([`get`](Self::get) / [`exists`](Self::exists)),
/// then either [`update`](Self::update)s it, [`remove`](Self::remove)s it, or leaves it. It may
/// also arm or read a timeout. Whether this invocation is a timeout firing (rather than a
/// data batch) is reported by [`has_timed_out`](Self::has_timed_out).
pub struct GroupState<S> {
    value: Option<S>,
    timeout_ms: Option<u64>,
    timed_out: bool,
    removed: bool,
}

impl<S> GroupState<S> {
    fn new(value: Option<S>, timeout_ms: Option<u64>, timed_out: bool) -> Self {
        GroupState {
            value,
            timeout_ms,
            timed_out,
            removed: false,
        }
    }

    /// Whether the group currently has a state value.
    pub fn exists(&self) -> bool {
        self.value.is_some()
    }

    /// The current state value, if any.
    pub fn get(&self) -> Option<&S> {
        self.value.as_ref()
    }

    /// Replace the state value. Clears any pending removal.
    pub fn update(&mut self, value: S) {
        self.value = Some(value);
        self.removed = false;
    }

    /// Drop the state value; the group is removed from the store after this invocation.
    /// Also clears any armed timeout.
    pub fn remove(&mut self) {
        self.value = None;
        self.timeout_ms = None;
        self.removed = true;
    }

    /// Whether this invocation is a timeout firing (no input rows for the group this batch).
    pub fn has_timed_out(&self) -> bool {
        self.timed_out
    }

    /// Arm (or re-arm) the timeout at an absolute timestamp in milliseconds.
    pub fn set_timeout_timestamp_ms(&mut self, timestamp_ms: u64) {
        self.timeout_ms = Some(timestamp_ms);
    }

    /// Clear any armed timeout.
    pub fn clear_timeout(&mut self) {
        self.timeout_ms = None;
    }

    /// The currently armed timeout timestamp, if any.
    pub fn get_timeout_timestamp_ms(&self) -> Option<u64> {
        self.timeout_ms
    }
}

/// One group's persisted state: the value plus its armed timeout.
#[derive(Clone, bincode::Encode, bincode::Decode)]
struct StoredState<S> {
    value: S,
    timeout_ms: Option<u64>,
}

/// The arbitrary-per-group stateful operator.
///
/// `update` receives the group key, the rows for the group this batch (empty on a timeout
/// firing), and the [`GroupState`] handle; it returns the output rows for the group.
pub struct MapGroupsWithState<S, R, Out, F>
where
    F: Fn(&[GroupVal], Vec<R>, &mut GroupState<S>) -> Vec<Out>,
{
    states: HashMap<Vec<GroupVal>, StoredState<S>>,
    update: F,
    timeout: GroupStateTimeout,
    _row: std::marker::PhantomData<fn(R) -> Out>,
}

impl<S, R, Out, F> MapGroupsWithState<S, R, Out, F>
where
    F: Fn(&[GroupVal], Vec<R>, &mut GroupState<S>) -> Vec<Out>,
{
    /// Create an operator with the given per-group `update` function and timeout mode.
    pub fn new(update: F, timeout: GroupStateTimeout) -> Self {
        MapGroupsWithState {
            states: HashMap::new(),
            update,
            timeout,
            _row: std::marker::PhantomData,
        }
    }

    /// Number of groups currently holding state.
    pub fn num_groups(&self) -> usize {
        self.states.len()
    }

    /// Apply the update function to a batch of grouped rows, returning all output rows.
    ///
    /// Each key in `grouped` is visited once with its rows; groups with state but no rows this
    /// batch are left untouched (they only fire via [`evict_timed_out`](Self::evict_timed_out)).
    pub fn process_batch(&mut self, grouped: HashMap<Vec<GroupVal>, Vec<R>>) -> Vec<Out> {
        let mut out = Vec::new();
        for (key, rows) in grouped {
            out.extend(self.run_group(key, rows, false));
        }
        out
    }

    /// Fire the timeout callback for every group whose armed timestamp is `<= current_ms`,
    /// where `current_ms` is processing time or the event-time watermark depending on the
    /// configured [`GroupStateTimeout`]. No-op when timeouts are disabled.
    pub fn evict_timed_out(&mut self, current_ms: u64) -> Vec<Out> {
        if self.timeout == GroupStateTimeout::None {
            return Vec::new();
        }
        let due: Vec<Vec<GroupVal>> = self
            .states
            .iter()
            .filter(|(_, s)| s.timeout_ms.is_some_and(|t| t <= current_ms))
            .map(|(k, _)| k.clone())
            .collect();
        let mut out = Vec::new();
        for key in due {
            out.extend(self.run_group(key, Vec::new(), true));
        }
        out
    }

    /// Load the group's state, run the callback, then reconcile the store with the handle.
    fn run_group(&mut self, key: Vec<GroupVal>, rows: Vec<R>, timed_out: bool) -> Vec<Out> {
        // Take ownership of any existing state so the callback mutates it in place — no clone of
        // the user's `S`. Re-insert below only if the group survives.
        let (value, timeout_ms) = match self.states.remove(&key) {
            Some(s) => (Some(s.value), s.timeout_ms),
            None => (None, None),
        };
        let mut gs = GroupState::new(value, timeout_ms, timed_out);
        let out = (self.update)(&key, rows, &mut gs);
        let GroupState {
            value,
            timeout_ms,
            removed,
            ..
        } = gs;
        if let Some(value) = value
            && !removed
        {
            self.states.insert(key, StoredState { value, timeout_ms });
        }
        out
    }
}

impl<S, R, Out, F> MapGroupsWithState<S, R, Out, F>
where
    S: Clone + bincode::Encode + bincode::Decode<()>,
    F: Fn(&[GroupVal], Vec<R>, &mut GroupState<S>) -> Vec<Out>,
{
    /// Serialize the current per-group state for checkpointing.
    pub fn encode_state(&self) -> StructuredResult<Vec<u8>> {
        let snapshot: Vec<(&Vec<GroupVal>, &StoredState<S>)> = self.states.iter().collect();
        bincode::encode_to_vec(snapshot, bincode::config::standard())
            .map_err(|e| StructuredError::Checkpoint(e.to_string()))
    }

    /// Restore per-group state from a checkpoint blob, replacing any current state.
    pub fn restore_state(&mut self, bytes: &[u8]) -> StructuredResult<()> {
        let (snapshot, _): (Vec<(Vec<GroupVal>, StoredState<S>)>, _) =
            bincode::decode_from_slice(bytes, bincode::config::standard())
                .map_err(|e| StructuredError::Checkpoint(e.to_string()))?;
        self.states = snapshot.into_iter().collect();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(id: i64) -> Vec<GroupVal> {
        vec![GroupVal::Int(id)]
    }

    // Running per-key sum: state is the running total, output is (key_id, total).
    fn sum_update(k: &[GroupVal], rows: Vec<i64>, gs: &mut GroupState<i64>) -> Vec<(i64, i64)> {
        let GroupVal::Int(id) = k[0] else {
            return vec![];
        };
        if gs.has_timed_out() {
            let total = gs.get().copied().unwrap_or(0);
            gs.remove();
            return vec![(id, total)];
        }
        let mut total = gs.get().copied().unwrap_or(0);
        total += rows.iter().sum::<i64>();
        gs.update(total);
        gs.set_timeout_timestamp_ms(1_000);
        vec![(id, total)]
    }

    #[test]
    fn test_state_persists() {
        let mut op = MapGroupsWithState::new(sum_update, GroupStateTimeout::ProcessingTime);

        let mut b1 = HashMap::new();
        b1.insert(key(1), vec![1, 2]);
        b1.insert(key(2), vec![10]);
        let mut out1 = op.process_batch(b1);
        out1.sort();
        assert_eq!(out1, vec![(1, 3), (2, 10)]);

        let mut b2 = HashMap::new();
        b2.insert(key(1), vec![4]);
        let out2 = op.process_batch(b2);
        assert_eq!(out2, vec![(1, 7)]); // 3 + 4, state carried over
        assert_eq!(op.num_groups(), 2);
    }

    #[test]
    fn test_timeout_removes() {
        let mut op = MapGroupsWithState::new(sum_update, GroupStateTimeout::ProcessingTime);
        let mut b1 = HashMap::new();
        b1.insert(key(1), vec![5]);
        op.process_batch(b1);

        // Before the timeout, nothing fires.
        assert!(op.evict_timed_out(500).is_empty());
        // After it, the group emits its final total and is dropped.
        let fired = op.evict_timed_out(2_000);
        assert_eq!(fired, vec![(1, 5)]);
        assert_eq!(op.num_groups(), 0);
    }

    #[test]
    fn test_checkpoint_roundtrip() {
        let mut op = MapGroupsWithState::new(sum_update, GroupStateTimeout::ProcessingTime);
        let mut b1 = HashMap::new();
        b1.insert(key(7), vec![100]);
        op.process_batch(b1);

        let blob = op.encode_state().unwrap();
        let mut restored = MapGroupsWithState::new(sum_update, GroupStateTimeout::ProcessingTime);
        restored.restore_state(&blob).unwrap();
        assert_eq!(restored.num_groups(), 1);

        // Restored state continues accumulating.
        let mut b2 = HashMap::new();
        b2.insert(key(7), vec![1]);
        assert_eq!(restored.process_batch(b2), vec![(7, 101)]);
    }
}
