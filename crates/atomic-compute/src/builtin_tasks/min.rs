use crate::register_binary_task;
use crate::task_traits::BinaryTask;

/// Built-in: return the lesser of two `Ord` values.
#[derive(Clone, Copy)]
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
            fn call(&self, a: $ty, b: $ty) -> $ty {
                if a <= b { a } else { b }
            }
        }

        register_binary_task!(MinTask<$ty>, $ty);
    };
}

impl_min_task!(i32);
impl_min_task!(i64);
impl_min_task!(u32);
impl_min_task!(u64);
impl_min_task!(f32);
impl_min_task!(f64);
impl_min_task!(String);
