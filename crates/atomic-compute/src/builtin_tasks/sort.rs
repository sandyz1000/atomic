use crate::task_registry::TaskEntry;

/// Built-in: sort all elements within a partition in ascending order.
///
/// Used by `TypedRdd::sort_within_partitions()` and as the local sort step
/// before a merge-sort across partitions for `sort_by` / `sort_by_key`.
pub struct SortTask<T>(std::marker::PhantomData<T>);

impl<T> Default for SortTask<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

macro_rules! impl_sort_task {
    ($ty:ty) => {
        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::sort::", stringify!($ty)),
                body_hash: 0,
handler: |action, _payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    match action {
                        TaskAction::Map | TaskAction::Collect => {
                            let mut items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            items.sort_by(|a, b| a.partial_cmp(b).unwrap_or(::std::cmp::Ordering::Equal));
                            items.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("SortTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}

impl_sort_task!(i32);
impl_sort_task!(i64);
impl_sort_task!(u32);
impl_sort_task!(u64);
impl_sort_task!(f32);
impl_sort_task!(f64);
