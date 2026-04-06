/// Trait alias for functions used in RDD operations
/// Consolidates common bounds: Fn trait + Send + Sync + 'static
///
/// This eliminates the need to repeat these bounds in every impl block.
///
/// # Example
/// ```rust,ignore
/// // Before:
/// impl<T, U, F> RddBase for MapperRdd<T, U, F>
/// where
///     F: Fn(T) -> U + Send + Sync + 'static,
/// { }
///
/// // After:
/// impl<T, U, F: RddFn<T, U>> RddBase for MapperRdd<T, U, F> { }
/// ```

/// Function that transforms one value to another
pub trait RddFn<T, U>: Fn(T) -> U + Send + Sync + 'static {}

impl<T, U, F> RddFn<T, U> for F where F: Fn(T) -> U + Send + Sync + 'static {}

/// Function that transforms one value to an iterator
pub trait RddFlatMapFn<T, U>: Fn(T) -> Box<dyn Iterator<Item = U>> + Send + Sync + 'static {}

impl<T, U, F> RddFlatMapFn<T, U> for F where
    F: Fn(T) -> Box<dyn Iterator<Item = U>> + Send + Sync + 'static
{
}

/// Function that transforms a partition (with index) to an iterator
pub trait RddPartitionFn<T, U>:
    Fn(usize, Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = U>> + Send + Sync + 'static
{
}

impl<T, U, F> RddPartitionFn<T, U> for F where
    F: Fn(usize, Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = U>>
        + Send
        + Sync
        + 'static
{
}

/// Predicate function for filtering
pub trait RddPredicateFn<T>: Fn(&T) -> bool + Send + Sync + 'static {}

impl<T, F> RddPredicateFn<T> for F where F: Fn(&T) -> bool + Send + Sync + 'static {}
