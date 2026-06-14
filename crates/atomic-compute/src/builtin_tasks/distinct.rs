use crate::task_registry::TaskEntry;

/// Built-in: remove duplicate elements within a partition.
///
/// Used as the local combine step in `TypedRdd::distinct()`. After each
/// partition deduplicates locally, a shuffle groups all copies of each element
/// to one partition, where a final `DistinctTask` pass finishes the job.
pub struct DistinctTask<T>(std::marker::PhantomData<T>);

impl<T> Default for DistinctTask<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

macro_rules! impl_distinct_task {
    ($ty:ty) => {
        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::distinct::", stringify!($ty)),
                body_hash: 0,
handler: |action, _payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    use ::std::collections::HashSet;
                    match action {
                        TaskAction::Map | TaskAction::Collect => {
                            let items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let unique: Vec<$ty> = items
                                .into_iter()
                                .collect::<HashSet<_>>()
                                .into_iter()
                                .collect();
                            unique.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("DistinctTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}

impl_distinct_task!(i32);
impl_distinct_task!(i64);
impl_distinct_task!(u32);
impl_distinct_task!(u64);
impl_distinct_task!(String);
