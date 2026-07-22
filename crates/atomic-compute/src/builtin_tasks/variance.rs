use crate::register_aggregate_task;
use crate::task_traits::AggregateTask;

/// Built-in: Welford accumulator `(count, mean, m2)` for population variance / stdev.
///
/// The [`AggregateTask`] shape (`A = (u64, f64, f64)`, `T = numeric`): each worker folds its
/// partition into `(count, mean, m2)` via Welford's online update; the driver merges the
/// accumulators with Chan's parallel formula. `m2` is the sum of squared deviations, so
/// population variance is `m2 / count`. Registered for the numeric primitives via
/// [`register_aggregate_task!`], keyed `atomic::builtin::variance::<ty>`.
#[derive(Clone, Copy, Default)]
pub struct VarianceTask<T>(std::marker::PhantomData<T>);

macro_rules! impl_variance_task {
    ($ty:ty) => {
        impl AggregateTask<(u64, f64, f64), $ty> for VarianceTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::variance::", stringify!($ty));

            fn seq(&self, (count, mean, m2): (u64, f64, f64), x: $ty) -> (u64, f64, f64) {
                let x = x as f64;
                let count = count + 1;
                let delta = x - mean;
                let mean = mean + delta / count as f64;
                let delta2 = x - mean;
                (count, mean, m2 + delta * delta2)
            }

            fn comb(&self, a: (u64, f64, f64), b: (u64, f64, f64)) -> (u64, f64, f64) {
                let (na, mean_a, m2a) = a;
                let (nb, mean_b, m2b) = b;
                let n = na + nb;
                if n == 0 {
                    return (0, 0.0, 0.0);
                }
                let delta = mean_b - mean_a;
                let mean = mean_a + delta * nb as f64 / n as f64;
                let m2 = m2a + m2b + delta * delta * (na as f64) * (nb as f64) / n as f64;
                (n, mean, m2)
            }
        }

        register_aggregate_task!(VarianceTask<$ty>, (u64, f64, f64), $ty);
    };
}

// VarianceTask uses `x as f64`, so only integer and float primitives apply.
impl_variance_task!(i32);
impl_variance_task!(i64);
impl_variance_task!(u32);
impl_variance_task!(u64);
impl_variance_task!(f32);
impl_variance_task!(f64);
