/// Marker trait for types that can be used in RDD operations.
/// This trait combines the common bounds required throughout the codebase:
/// - `Clone`: For duplicating data across partitions
/// - `Send + Sync`: For safe concurrent access across threads
/// - `Debug`: For debugging and error messages
/// - `'static`: For stable references without lifetime constraints
///
/// Note: Serialization bounds (Encode/Decode) are NOT included here to allow
/// `dyn Data` trait objects. Add serialization bounds where needed in function signatures.
pub trait Data: Send + Sync + std::fmt::Debug + 'static {}

/// Blanket implementation: any type that satisfies the bounds automatically implements Data
impl<T> Data for T where T: Send + Sync + std::fmt::Debug + 'static + Clone {}
