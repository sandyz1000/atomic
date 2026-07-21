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
pub trait UnaryTask<T, U>: Clone + Send + Sync + 'static {
    /// Compile-time op_id — must match the `inventory` registration in the worker.
    const NAME: &'static str;
    /// Apply the task function to a single input element. Takes `&self` so a task
    /// carrying captured/bound parameters can read them; a parameter-free task ignores it.
    fn call(&self, input: T) -> U;
    /// rkyv-encoded bound parameters shipped in `Step.payload` and decoded by the
    /// worker's dispatch handler. Empty for a zero-sized (parameter-free) task; a task
    /// carrying captured values (`task_fn!([cap] …)`) overrides this to encode its fields.
    fn encode_params(&self) -> Vec<u8> {
        Vec::new()
    }
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
pub trait BinaryTask<T>: Clone + Send + Sync + 'static {
    /// Compile-time op_id — must match the `inventory` registration in the worker.
    const NAME: &'static str;
    /// Combine two values of type `T` into one. Takes `&self` so a task carrying
    /// bound parameters can read them; a parameter-free task ignores it.
    fn call(&self, a: T, b: T) -> T;
    /// rkyv-encoded bound parameters shipped in `Step.payload`. Empty for a
    /// parameter-free task. Note: a `Fold`/`Aggregate` op already uses `payload` for its
    /// zero value, so a binary task cannot both carry params and be used as a fold.
    fn encode_params(&self) -> Vec<u8> {
        Vec::new()
    }
}
