use std::any::Any;

/// Marker trait for types that can be used in RDD operations.
/// This trait combines the common bounds required throughout the codebase:
/// - `Clone`: REQUIRED - RDD operations move/duplicate data across partitions and through
///   transformation pipelines. Without Clone, we cannot split data or cache intermediate results.
/// - `Send + Sync`: For safe concurrent access across threads
/// - `Debug`: For debugging and error messages
/// - `'static`: For stable references without lifetime constraints
///
/// Note: Serialization bounds (Encode/Decode) are NOT included here to allow
/// `dyn Data` trait objects. Add serialization bounds where needed in function signatures.
///
/// Why Clone is fundamental:
/// - RDDs are immutable - transformations create new RDDs that share data with parents
/// - Caching requires cloning to store results
/// - Shuffle operations distribute data across partitions (moved, not cloned in practice,
///   but Clone bound enables the type system to work)
/// - Tuple types like (K, V) need Clone to be Data
pub trait Data: Send + Sync + std::fmt::Debug + 'static {
    /// Cast trait object to `&dyn Any` for downcasting
    fn as_any(&self) -> &dyn Any;
}

/// Blanket implementation: any type that satisfies the bounds automatically implements Data
impl<T> Data for T
where
    T: Send + Sync + std::fmt::Debug + 'static + Clone,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

