use crate::register_aggregate_task;
use crate::task_traits::AggregateTask;

/// Built-in: accumulate `(f64_sum, u64_count)` for mean calculation.
///
/// The [`AggregateTask`] shape (`A = (f64, u64)`, `T = numeric`): each worker folds its
/// partition into one `(sum, count)`; the driver merges them and divides. Registered for the
/// numeric primitives via [`register_aggregate_task!`], keyed `atomic::builtin::mean::<ty>`.
#[derive(Clone, Copy, Default)]
pub struct MeanTask<T>(std::marker::PhantomData<T>);

macro_rules! impl_mean_task {
    ($ty:ty) => {
        impl AggregateTask<(f64, u64), $ty> for MeanTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::mean::", stringify!($ty));
            fn seq(&self, (sum, count): (f64, u64), x: $ty) -> (f64, u64) {
                (sum + x as f64, count + 1)
            }
            fn comb(&self, a: (f64, u64), b: (f64, u64)) -> (f64, u64) {
                (a.0 + b.0, a.1 + b.1)
            }
        }

        register_aggregate_task!(MeanTask<$ty>, (f64, u64), $ty);
    };
}

// MeanTask uses `x as f64`, so only integer and float primitives apply.
impl_mean_task!(i32);
impl_mean_task!(i64);
impl_mean_task!(u32);
impl_mean_task!(u64);
impl_mean_task!(f32);
impl_mean_task!(f64);
