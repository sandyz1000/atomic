use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::distributed::WireDecode;

static NEXT_BROADCAST_ID: AtomicUsize = AtomicUsize::new(1);

pub fn next_broadcast_id() -> usize {
    NEXT_BROADCAST_ID.fetch_add(1, Ordering::SeqCst)
}

// Each worker task populates this before executing ops and clears it after.
thread_local! {
    static BROADCAST_CTX: RefCell<HashMap<usize, Vec<u8>>> = RefCell::new(HashMap::new());
}

/// Load broadcast values for the current task into the thread-local registry.
/// Called by `NativeBackend::execute` before the op loop.
pub fn load_broadcast_values(values: &[(usize, Vec<u8>)]) {
    BROADCAST_CTX.with(|ctx| {
        let mut map = ctx.borrow_mut();
        map.clear();
        for (id, bytes) in values {
            map.insert(*id, bytes.clone());
        }
    });
}

/// Clear the thread-local registry after task execution.
pub fn clear_broadcast_values() {
    BROADCAST_CTX.with(|ctx| ctx.borrow_mut().clear());
}

/// A handle to a broadcast variable.
///
/// On the driver, created by `Context::broadcast(value)`. On workers, populated
/// from `TaskEnvelope::broadcast_values` before the task's ops execute. Task code
/// calls `broadcast_var.value()` to read the data.
///
/// `T` must implement `WireEncode` (driver side) and `WireDecode` (worker side).
/// Both are blanket-implemented for types that satisfy the rkyv bounds.
#[derive(Debug, Clone)]
pub struct BroadcastVar<T> {
    pub id: usize,
    _phantom: PhantomData<T>,
}

impl<T> BroadcastVar<T>
where
    T: WireDecode,
{
    pub fn new(id: usize) -> Self {
        BroadcastVar {
            id,
            _phantom: PhantomData,
        }
    }

    /// Read the broadcast value. Panics if the broadcast was not loaded for the current task.
    pub fn value(&self) -> T {
        BROADCAST_CTX.with(|ctx| {
            let map = ctx.borrow();
            let bytes = map
                .get(&self.id)
                .unwrap_or_else(|| panic!("BroadcastVar {}: not loaded for this task", self.id));
            T::decode_wire(bytes).expect("BroadcastVar: decode failed")
        })
    }
}
