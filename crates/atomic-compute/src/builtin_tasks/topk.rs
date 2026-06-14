use crate::task_registry::TaskEntry;

/// Built-in: return the top K elements in descending order from a partition.
///
/// Used by `TypedRdd::top(k)` in distributed mode. Each partition produces its
/// local top-K; the driver merges them and takes the global top-K.
///
/// `payload` must be a wire-encoded `u64` (the K value).
pub struct TopKTask<T>(std::marker::PhantomData<T>);

impl<T> Default for TopKTask<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

macro_rules! impl_top_k_task {
    ($ty:ty) => {
        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::top_k::", stringify!($ty)),
                body_hash: 0,
handler: |action, payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    match action {
                        TaskAction::Collect => {
                            let k = u64::decode_wire(payload)
                                .map_err(|e| e.to_string())? as usize;
                            let mut items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            items.sort_by(|a, b| b.partial_cmp(a).unwrap_or(::std::cmp::Ordering::Equal));
                            items.truncate(k);
                            items.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("TopKTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}

// Instantiate new built-in tasks for common primitive types.
impl_top_k_task!(i32);
impl_top_k_task!(i64);
impl_top_k_task!(u32);
impl_top_k_task!(u64);
impl_top_k_task!(f32);
impl_top_k_task!(f64);
