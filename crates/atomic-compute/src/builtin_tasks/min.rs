use crate::task_registry::TaskEntry;
use crate::task_traits::BinaryTask;

/// Built-in: return the lesser of two `Ord` values.
pub struct MinTask<T>(std::marker::PhantomData<T>);

impl<T> MinTask<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> Default for MinTask<T> {
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! impl_min_task {
    ($ty:ty) => {
        impl BinaryTask<$ty> for MinTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::min::", stringify!($ty));
            fn call(a: $ty, b: $ty) -> $ty {
                if a <= b { a } else { b }
            }
        }

        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::min::", stringify!($ty)),
                body_hash: 0,
handler: |action, payload, data| {
                    use atomic_data::distributed::{TaskAction, WireDecode, WireEncode};
                    let _ = payload;
                    match action {
                        TaskAction::Fold | TaskAction::Aggregate | TaskAction::Reduce => {
                            let items = ::std::vec::Vec::<$ty>::decode_wire(data)
                                .map_err(|e| e.to_string())?;
                            let mut iter = items.into_iter();
                            let first = iter.next()
                                .ok_or_else(|| "min: empty partition".to_string())?;
                            let result = iter.fold(first, |a, b| if a <= b { a } else { b });
                            result.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("MinTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}


impl_min_task!(i32);
impl_min_task!(i64);
impl_min_task!(u32);
impl_min_task!(u64);
impl_min_task!(f32);
impl_min_task!(f64);
