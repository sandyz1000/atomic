pub struct TaskContext {
    pub stage_id: usize,
    pub split_id: usize,
    pub attempt_id: usize,
}

impl TaskContext {
    pub fn new(stage_id: usize, split_id: usize, attempt_id: usize) -> Self {
        TaskContext {
            stage_id,
            split_id,
            attempt_id,
        }
    }
}

/// Per-partition compute closure shape: `(context, partition iterator) -> result`.
/// Bare bound, no thread/lifetime requirement — for signatures where `F` is a
/// phantom type parameter that is never invoked (e.g. `NativeScheduler::submit_task`).
///
/// Legacy closure-dispatch shape, kept only for `LocalScheduler`'s existing built-in RDD
/// actions (`.collect()`, `.reduce()`, `.max()`, ...) — do not use it for new code. The
/// project's task model is `#[task]`/`task_fn!` (`atomic_compute::task_traits`): a
/// zero-sized struct dispatched by compile-time-registered name, not a closure. Every new
/// driver-facing op should be `#[task]`/`task_fn!`-wrapped, even where it only ever runs
/// in-process (`LocalScheduler::supports_closure_tasks() == true` is what let this shape
/// survive here at all — `DistributedScheduler` already refuses it). Migrating the
/// built-in actions off this trait onto the task model is tracked as the scheduler
/// unification effort; do not add further closure-based call sites in the meantime.
pub trait PartitionFn<T, U>: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U {}
impl<T, U, F> PartitionFn<T, U> for F where F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U {}

/// [`PartitionFn`] additionally bounded to cross thread and task boundaries —
/// required wherever the closure is actually stored or invoked off the driver thread.
/// Same legacy status as [`PartitionFn`] — see its doc comment.
pub trait PartitionTask<T, U>: PartitionFn<T, U> + Send + Sync + 'static {}
impl<T, U, F> PartitionTask<T, U> for F where F: PartitionFn<T, U> + Send + Sync + 'static {}
