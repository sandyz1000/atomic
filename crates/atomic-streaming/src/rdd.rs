/// Streaming-specific RDD types.
///
/// The core streaming engine uses `atomic_compute` RDDs directly.
/// This module holds streaming-specific state management types.
use std::collections::HashMap;

// ─────────────────────────────────────────────────────────────────────────────
// StateMap — per-key state storage for updateStateByKey / mapWithState
// ─────────────────────────────────────────────────────────────────────────────

/// A map from key to optional state, used in stateful stream transformations.
pub struct StateMap<K, S>
where
    K: std::hash::Hash + Eq + Clone,
    S: Clone,
{
    map: HashMap<K, S>,
}

impl<K, S> StateMap<K, S>
where
    K: std::hash::Hash + Eq + Clone,
    S: Clone,
{
    pub fn new() -> Self {
        StateMap {
            map: HashMap::new(),
        }
    }

    pub fn get(&self, key: &K) -> Option<&S> {
        self.map.get(key)
    }

    pub fn put(&mut self, key: K, state: S) {
        self.map.insert(key, state);
    }

    pub fn remove(&mut self, key: &K) {
        self.map.remove(key);
    }

    pub fn contains(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.map.keys()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &S)> {
        self.map.iter()
    }
}

impl<K, S> Default for StateMap<K, S>
where
    K: std::hash::Hash + Eq + Clone,
    S: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MapWithStateRDDRecord — per-key record for mapWithState
// ─────────────────────────────────────────────────────────────────────────────

/// A single record held by the `MapWithStateRDD`.
#[derive(Debug, Clone)]
pub struct MapWithStateRDDRecord<K, S>
where
    K: Clone,
    S: Clone,
{
    pub key: K,
    pub state: Option<S>,
    pub last_updated_ms: u64,
}

impl<K, S> MapWithStateRDDRecord<K, S>
where
    K: Clone,
    S: Clone,
{
    pub fn new(key: K, state: Option<S>, last_updated_ms: u64) -> Self {
        MapWithStateRDDRecord { key, state, last_updated_ms }
    }

    pub fn update(&mut self, new_state: Option<S>, time_ms: u64) {
        self.state = new_state;
        self.last_updated_ms = time_ms;
    }
}
