use crate::task_registry::TaskEntry;

/// Built-in: return the first K elements in ascending order from a partition.
///
/// Used by `TypedRdd::take_ordered(k)` in distributed mode. Each partition
/// produces its local first-K ascending; the driver merges and re-truncates.
///
/// `payload` must be a wire-encoded `u64` (the K value).
pub struct TakeOrderedTask<T>(std::marker::PhantomData<T>);

impl<T> Default for TakeOrderedTask<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

macro_rules! impl_take_ordered_task {
    ($ty:ty) => {
        inventory::submit! {
            TaskEntry {
                task_name: concat!("atomic::builtin::take_ordered::", stringify!($ty)),
                body_hash: 0,
handler: |action, payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    match action {
                        TaskAction::Collect => {
                            let k = u64::decode_wire(payload)
                                .map_err(|e| e.to_string())? as usize;
                            let mut items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            items.sort_by(|a, b| a.partial_cmp(b).unwrap_or(::std::cmp::Ordering::Equal));
                            items.truncate(k);
                            items.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("TakeOrderedTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}

impl_take_ordered_task!(i32);
impl_take_ordered_task!(i64);
impl_take_ordered_task!(u32);
impl_take_ordered_task!(u64);
impl_take_ordered_task!(f32);
impl_take_ordered_task!(f64);
