use crate::task_registry::TaskEntry;
use crate::task_traits::BinaryTask;

/// Built-in: add two values.
///
/// Used by `TypedRdd::count()` to sum per-partition counts.
///
/// Registered by hand rather than via `register_binary_task!`: `sum` folds from the zero in
/// `payload` (the fold identity) for `Fold`/`Aggregate` and reduces without one for `Reduce`,
/// whereas the shared macro seeds from the first element and ignores the payload.
#[derive(Clone, Copy)]
pub struct SumTask<T>(std::marker::PhantomData<T>);

impl<T> SumTask<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> Default for SumTask<T> {
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! impl_sum_task {
    ($ty:ty) => {
        impl BinaryTask<$ty> for SumTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::sum::", stringify!($ty));
            fn call(&self, a: $ty, b: $ty) -> $ty {
                a + b
            }
        }

        inventory::submit! {
            TaskEntry {
                task_name: concat!("atomic::builtin::sum::", stringify!($ty)),
                body_hash: 0,
handler: |action, payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    let task = SumTask::<$ty>::default();
                    match action {
                        TaskAction::Fold | TaskAction::Aggregate => {
                            let zero = <$ty>::decode_wire(payload).map_err(|e| e.to_string())?;
                            let items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let result: $ty = items.into_iter().fold(zero, |a, b| task.call(a, b));
                            result.encode_wire().map_err(|e| e.to_string())
                        }
                        TaskAction::Reduce => {
                            let items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let result: $ty =
                                items.into_iter().reduce(|a, b| task.call(a, b)).unwrap_or_default();
                            result.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("SumTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}

impl_sum_task!(i32);
impl_sum_task!(i64);
impl_sum_task!(u32);
impl_sum_task!(u64);
impl_sum_task!(f32);
impl_sum_task!(f64);
