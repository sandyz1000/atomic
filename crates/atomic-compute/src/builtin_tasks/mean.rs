use crate::task_registry::TaskEntry;

/// Built-in: compute per-partition `(f64_sum, u64_count)` for mean calculation.
///
/// Used by `TypedRdd::mean()`. Each worker returns `(sum, count)` for its
/// partition; the driver combines them: `total_sum / total_count as f64`.
///
/// The driver reduce step uses a `SumTask<f64>` for the sum component and
/// `SumTask<u64>` for the count component, or combines the tuple directly.
pub struct MeanTask<T>(std::marker::PhantomData<T>);

impl<T> Default for MeanTask<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

macro_rules! impl_mean_task {
    ($ty:ty) => {
        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::mean::", stringify!($ty)),
                body_hash: 0,
handler: |action, _payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    match action {
                        TaskAction::Aggregate | TaskAction::Collect => {
                            let items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let count = items.len() as u64;
                            let sum: f64 = items.into_iter().map(|x| x as f64).sum();
                            (sum, count).encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("MeanTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}



// MeanTask uses `x as f64` so only integer and float primitives apply.
impl_mean_task!(i32);
impl_mean_task!(i64);
impl_mean_task!(u32);
impl_mean_task!(u64);
impl_mean_task!(f32);
impl_mean_task!(f64);


