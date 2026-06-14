use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::distributed::{WireDecode, WireEncode};

static NEXT_ACCUMULATOR_ID: AtomicUsize = AtomicUsize::new(1);

pub fn next_accumulator_id() -> usize {
    NEXT_ACCUMULATOR_ID.fetch_add(1, Ordering::SeqCst)
}

// Workers accumulate deltas here during task execution. NativeBackend serialises
// and returns them in TaskResultEnvelope::accumulator_deltas after the op loop.
thread_local! {
    static ACCUMULATOR_DELTAS: RefCell<HashMap<usize, Vec<u8>>> = RefCell::new(HashMap::new());
}

/// Add a rkyv-encoded delta for accumulator `id` to the task-local registry.
/// Called by `Accumulator::add` inside `#[task]` functions.
pub fn add_delta(id: usize, delta_bytes: Vec<u8>) {
    ACCUMULATOR_DELTAS.with(|d| {
        d.borrow_mut().insert(id, delta_bytes);
    });
}

/// Drain and return all pending deltas accumulated during the current task.
/// Called by `NativeBackend::execute` after the op loop.
pub fn drain_deltas() -> Vec<(usize, Vec<u8>)> {
    ACCUMULATOR_DELTAS.with(|d| d.borrow_mut().drain().collect())
}

/// A distributed accumulator.
///
/// On the driver, created by `Context::accumulator(init)`. On workers, task code calls
/// `accumulator.add(delta)` which stores the delta in the task-local registry.
/// After the task finishes, `NativeBackend` serialises all deltas and returns them in
/// `TaskResultEnvelope::accumulator_deltas`. The driver scheduler merges them via
/// the registered merge function into the driver-side value.
///
/// Read the current (driver-side) value with `accumulator.value()`.
#[derive(Debug, Clone)]
pub struct Accumulator<T> {
    pub id: usize,
    _phantom: PhantomData<T>,
}

impl<T> Accumulator<T>
where
    T: WireEncode + WireDecode,
{
    pub fn new(id: usize) -> Self {
        Accumulator {
            id,
            _phantom: PhantomData,
        }
    }

    /// Accumulate a delta from inside a `#[task]` function (worker side).
    ///
    /// Only the last `add()` call per accumulator per task is retained — if you need to
    /// accumulate multiple values within a single task, fold them yourself before calling
    /// `add`.  The driver-side merge function combines per-task deltas.
    pub fn add(&self, delta: T) {
        let bytes = delta
            .encode_wire()
            .expect("Accumulator::add: encode failed");
        add_delta(self.id, bytes);
    }
}

/// A type-erased merge function: takes `(current_bytes, delta_bytes)` and returns
/// merged bytes.  Registered once per accumulator at `Context::accumulator()` time.
pub type MergeFn = Box<dyn Fn(Vec<u8>, Vec<u8>) -> Vec<u8> + Send + Sync>;

/// Type-safe helper to build a `MergeFn` from a user-supplied `Fn(T, T) -> T`.
pub fn make_merge_fn<T, F>(merge: F) -> MergeFn
where
    T: WireEncode + WireDecode,
    F: Fn(T, T) -> T + Send + Sync + 'static,
{
    Box::new(move |cur_bytes: Vec<u8>, delta_bytes: Vec<u8>| {
        let cur = T::decode_wire(&cur_bytes).expect("accumulator merge: decode current");
        let delta = T::decode_wire(&delta_bytes).expect("accumulator merge: decode delta");
        let merged = merge(cur, delta);
        merged.encode_wire().expect("accumulator merge: encode")
    })
}
