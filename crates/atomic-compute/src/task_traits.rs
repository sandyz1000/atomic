use atomic_data::error::DataResult;

/// Marker + dispatch trait for unary `#[task]` functions: `fn(T) -> U`.
///
/// Covers `Map` (T→U), `Filter` (T→bool), and `FlatMap` (T→Vec<U>) actions.
/// The `#[task]` proc-macro generates a zero-sized struct (e.g. `Double`) that
/// implements this trait, making the `task_name` available statically at the call site.
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
    /// Compile-time task_name — must match the `inventory` registration in the worker.
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
    /// Compile-time task_name — must match the `inventory` registration in the worker.
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

/// Marker + dispatch trait for whole-partition transforms: `Vec<T> -> Vec<T>`.
///
/// Covers reductions that consume a partition all at once rather than element-by-element —
/// local top-k, bounded ordered take, dedup, in-partition sort. Unlike [`BinaryTask`] and
/// [`AggregateTask`] there is no element-level combine; the whole partition is transformed and
/// the cross-partition merge happens driver-side.
///
/// The `Map` / `Collect` dispatch handler decodes the partition into a `Vec<T>`, calls
/// [`transform`](Self::transform), and re-encodes. `payload` carries the op parameter (e.g. the
/// `k` for top-k), or is empty for a parameter-free transform.
pub trait PartitionTask<T>: Default + Send + Sync + 'static {
    /// Compile-time task_name — must match the `inventory` registration in the worker.
    const NAME: &'static str;
    /// Transform the partition. `payload` holds any wire-encoded op parameter.
    fn transform(&self, items: Vec<T>, payload: &[u8]) -> DataResult<Vec<T>>;
}

/// Marker + dispatch trait for accumulator-typed reductions: `seqOp: fn(A, T) -> A`
/// plus `combOp: fn(A, A) -> A`.
///
/// This is the general aggregation shape, where the accumulator type `A` differs from
/// the element type `T`. It subsumes [`BinaryTask`] — a `BinaryTask<T>` is the special
/// case `A == T` with `seq == comb`.
///
/// The two [`UnaryTask`]/[`BinaryTask`] shapes cannot express `seqOp`: `UnaryTask<T, U>`
/// takes one argument and `BinaryTask<T>` forces both arguments to the same type, whereas
/// `seqOp` folds an element of type `T` into an accumulator of a distinct type `A`. Tasks
/// that need a running accumulator (`count`: `A = u64`, `mean`: `A = (f64, u64)`,
/// `variance`: `A = (u64, f64, f64)`) implement this trait rather than being hand-written
/// dispatch handlers.
///
/// # Two-phase execution
///
/// - **Map side** (per partition): fold the partition's elements into one `A` via
///   [`seq`](Self::seq), starting from the wire-encoded zero in `Step.payload`.
/// - **Reduce side** (driver): combine the per-partition accumulators via
///   [`comb`](Self::comb) into the final `A`.
///
/// `comb` must be associative and commutative so partitions can merge in any order;
/// `seq` must be consistent with it (`comb(seq(z, x), seq(z, y))` == folding both elements).
///
/// # Example
/// ```ignore
/// // mean = sum / count, accumulator A = (f64, u64)
/// impl AggregateTask<(f64, u64), f64> for MeanTask<f64> {
///     const NAME: &'static str = "atomic::builtin::mean::f64";
///     fn seq(&self, (sum, n): (f64, u64), x: f64) -> (f64, u64) { (sum + x, n + 1) }
///     fn comb(&self, a: (f64, u64), b: (f64, u64)) -> (f64, u64) { (a.0 + b.0, a.1 + b.1) }
/// }
/// ```
pub trait AggregateTask<A, T>: Clone + Send + Sync + 'static {
    /// Compile-time task_name — must match the `inventory` registration in the worker.
    const NAME: &'static str;
    /// Fold one element into the accumulator (map side). `fn(A, T) -> A`.
    fn seq(&self, acc: A, elem: T) -> A;
    /// Merge two accumulators (reduce side). `fn(A, A) -> A`; must be associative
    /// and commutative.
    fn comb(&self, a: A, b: A) -> A;
    /// rkyv-encoded bound parameters shipped in `Step.payload`. Empty for a
    /// parameter-free task. Note: the zero accumulator travels separately in the
    /// op's own payload slot, so a param-carrying aggregate has the same slot
    /// collision that [`BinaryTask::encode_params`] describes for folds.
    fn encode_params(&self) -> Vec<u8> {
        Vec::new()
    }
}
