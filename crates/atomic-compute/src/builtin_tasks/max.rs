use crate::task_traits::BinaryTask;
use crate::task_registry::TaskEntry;

/// Built-in: return the greater of two `Ord` values.
///
/// Used by `TypedRdd::max()` to reduce each partition to its local maximum,
/// then combine the per-partition maxima on the driver.
///
/// `const NAME` encodes the element type via `stringify!(T)` so different
/// instantiations (`MaxTask<i32>`, `MaxTask<String>`, …) each get a unique op_id.
pub struct MaxTask<T>(std::marker::PhantomData<T>);

impl<T> MaxTask<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> Default for MaxTask<T> {
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! impl_max_task {
    ($ty:ty) => {
        impl BinaryTask<$ty> for MaxTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::max::", stringify!($ty));
            fn call(a: $ty, b: $ty) -> $ty {
                if a >= b { a } else { b }
            }
        }

        inventory::submit! {
            TaskEntry {
                op_id: concat!("atomic::builtin::max::", stringify!($ty)),
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
                                .ok_or_else(|| "max: empty partition".to_string())?;
                            let result = iter.fold(first, |a, b| if a >= b { a } else { b });
                            result.encode_wire().map_err(|e| e.to_string())
                        }
                        other => Err(format!("MaxTask does not support action {:?}", other)),
                    }
                },
            }
        }
    };
}


impl_max_task!(i32);
impl_max_task!(i64);
impl_max_task!(u32);
impl_max_task!(u64);
impl_max_task!(f32);
impl_max_task!(f64);
