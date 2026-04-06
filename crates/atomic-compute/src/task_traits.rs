/// Marker + dispatch trait for unary `#[task]` functions: `fn(T) -> U`.
///
/// Covers `Map` (T→U), `Filter` (T→bool), and `FlatMap` (T→Vec<U>) actions.
/// The `#[task]` proc-macro generates a zero-sized struct (e.g. `Double`) that
/// implements this trait, making the op-id available statically at the call site.
///
/// # Example
/// ```ignore
/// use atomic_compute::task;
///
/// #[task]
/// fn double(x: i32) -> i32 { x * 2 }
///
/// // Generated:
/// // pub struct Double;
/// // impl UnaryTask<i32, i32> for Double { const NAME = "…::double"; … }
///
/// ctx.parallelize_typed(data, 2).map_task(Double).collect()?;
/// ```
pub trait UnaryTask<T, U>: Send + Sync + 'static {
    /// Compile-time op_id — must match the `inventory` registration in the worker.
    const NAME: &'static str;
    /// Apply the task function to a single input element.
    fn call(input: T) -> U;
}

/// Marker + dispatch trait for binary `#[task]` functions: `fn(T, T) -> T`.
///
/// Covers `Fold` and `Reduce` actions where both arguments and the return type
/// are the same element type `T`.
///
/// # Example
/// ```ignore
/// #[task]
/// fn add(a: i32, b: i32) -> i32 { a + b }
///
/// // Generated:
/// // pub struct Add;
/// // impl BinaryTask<i32> for Add { const NAME = "…::add"; … }
///
/// ctx.parallelize_typed(data, 2).fold_task(0, Add)?;
/// ```
pub trait BinaryTask<T>: Send + Sync + 'static {
    /// Compile-time op_id — must match the `inventory` registration in the worker.
    const NAME: &'static str;
    /// Combine two values of type `T` into one.
    fn call(a: T, b: T) -> T;
}
