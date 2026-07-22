use crate::register_binary_task;
use crate::task_traits::BinaryTask;

/// Built-in: return the greater of two `Ord` values.
///
/// Used by `TypedRdd::max()` to reduce each partition to its local maximum,
/// then combine the per-partition maxima on the driver.
///
/// `const NAME` encodes the element type via `stringify!(T)` so different
/// instantiations (`MaxTask<i32>`, `MaxTask<String>`, …) each get a unique task_name.
#[derive(Clone, Copy)]
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
            fn call(&self, a: $ty, b: $ty) -> $ty {
                if a >= b { a } else { b }
            }
        }

        register_binary_task!(MaxTask<$ty>, $ty);
    };
}

impl_max_task!(i32);
impl_max_task!(i64);
impl_max_task!(u32);
impl_max_task!(u64);
impl_max_task!(f32);
impl_max_task!(f64);
impl_max_task!(String);
