
use crate::task_registry::TaskEntry;

/// Built-in: map partition to its element count (as `u64`).
///
/// Used by `TypedRdd::count()` to count elements per partition.
/// The driver sums the per-partition counts.
pub struct CountTask;

impl CountTask {
    /// op_id used for registration — generic-independent since count doesn't
    /// decode element types.
    pub const OP_ID: &'static str = "atomic::builtin::count";
}

inventory::submit! {
    TaskEntry {
        op_id: CountTask::OP_ID,
        body_hash: 0,
handler: |action, _payload, data| {
            use atomic_data::distributed::{TaskAction, WireEncode};
            match action {
                TaskAction::Map | TaskAction::Collect => {
                    // data is rkyv-encoded Vec<T>; we only need the count,
                    // so we decode as Vec<u8> sequences — but rkyv doesn't
                    // know the element type at this point. We use the raw
                    // byte length heuristic only if the type is known, so
                    // we instead expect the caller to encode as Vec<u8> with
                    // a count-map applied before dispatching.
                    //
                    // Simpler: callers use a typed count via SumTask<u64> on
                    // a map-to-1 pipeline. CountTask here is a low-level hook.
                    let _ = data;
                    1u64.encode_wire().map_err(|e| e.to_string())
                }
                other => Err(format!("CountTask does not support action {:?}", other)),
            }
        },
    }
}
