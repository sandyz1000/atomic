pub trait RddFn<T, U>: Fn(T) -> U + Send + Sync + 'static {}

impl<T, U, F> RddFn<T, U> for F where F: Fn(T) -> U + Send + Sync + 'static {}

pub trait RddFlatMapFn<T, U>: Fn(T) -> Box<dyn Iterator<Item = U>> + Send + Sync + 'static {}

impl<T, U, F> RddFlatMapFn<T, U> for F where
    F: Fn(T) -> Box<dyn Iterator<Item = U>> + Send + Sync + 'static
{
}

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

pub trait RddPredicateFn<T>: Fn(&T) -> bool + Send + Sync + 'static {}

impl<T, F> RddPredicateFn<T> for F where F: Fn(&T) -> bool + Send + Sync + 'static {}
