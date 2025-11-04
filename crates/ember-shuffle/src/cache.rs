use std::fmt::Debug;

/// Trait for shuffle cache operations
///
/// This abstracts the storage mechanism for shuffle data,
/// allowing different implementations (in-memory, distributed, etc.)
pub trait ShuffleCache: Send + Sync + Debug {
    /// Insert shuffle data for a given (shuffle_id, map_id, reduce_id) tuple
    fn insert(&self, key: (usize, usize, usize), value: Vec<u8>);

    /// Get shuffle data for a given key
    fn get(&self, key: &(usize, usize, usize)) -> Option<Vec<u8>>;

    /// Remove shuffle data for a given key
    fn remove(&self, key: &(usize, usize, usize)) -> Option<Vec<u8>>;

    /// Clear all shuffle data
    fn clear(&self);
}
