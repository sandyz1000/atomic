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
pub trait PartitionFn<T, U>: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U {}
impl<T, U, F> PartitionFn<T, U> for F where F: Fn((TaskContext, Box<dyn Iterator<Item = T>>)) -> U {}

/// [`PartitionFn`] additionally bounded to cross thread and task boundaries —
/// required wherever the closure is actually stored or invoked off the driver thread.
pub trait PartitionTask<T, U>: PartitionFn<T, U> + Send + Sync + 'static {}
impl<T, U, F> PartitionTask<T, U> for F where F: PartitionFn<T, U> + Send + Sync + 'static {}
