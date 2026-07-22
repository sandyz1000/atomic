use crate::register_aggregate_task;
use crate::task_traits::AggregateTask;

/// Summary statistics over a numeric RDD, computed in a single pass.
///
/// Returned by [`TypedRdd::stats`](crate::rdd::TypedRdd::stats). Holds the same accumulator
/// the worker builds — count, running mean, sum of squared deviations (`m2`), min and max —
/// and derives sum/variance/stdev from them on demand.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StatCounter {
    count: u64,
    mean: f64,
    m2: f64,
    min: f64,
    max: f64,
}

impl StatCounter {
    /// The wire accumulator tuple `(count, mean, m2, min, max)` this counter wraps.
    pub(crate) fn from_tuple((count, mean, m2, min, max): (u64, f64, f64, f64, f64)) -> Self {
        Self {
            count,
            mean,
            m2,
            min,
            max,
        }
    }

    /// Number of elements.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Arithmetic mean, or `NaN` for an empty RDD.
    pub fn mean(&self) -> f64 {
        if self.count == 0 { f64::NAN } else { self.mean }
    }

    /// Sum of all elements.
    pub fn sum(&self) -> f64 {
        self.mean * self.count as f64
    }

    /// Minimum element, or `NaN` for an empty RDD.
    pub fn min(&self) -> f64 {
        if self.count == 0 { f64::NAN } else { self.min }
    }

    /// Maximum element, or `NaN` for an empty RDD.
    pub fn max(&self) -> f64 {
        if self.count == 0 { f64::NAN } else { self.max }
    }

    /// Population variance (`m2 / count`), or `NaN` for an empty RDD.
    pub fn variance(&self) -> f64 {
        if self.count == 0 {
            f64::NAN
        } else {
            self.m2 / self.count as f64
        }
    }

    /// Sample variance (`m2 / (count - 1)`), or `NaN` for fewer than two elements.
    pub fn sample_variance(&self) -> f64 {
        if self.count < 2 {
            f64::NAN
        } else {
            self.m2 / (self.count - 1) as f64
        }
    }

    /// Population standard deviation — `sqrt(variance())`.
    pub fn stdev(&self) -> f64 {
        self.variance().sqrt()
    }

    /// Sample standard deviation — `sqrt(sample_variance())`.
    pub fn sample_stdev(&self) -> f64 {
        self.sample_variance().sqrt()
    }
}

/// The identity accumulator for [`StatCounterTask`] — an empty counter.
pub const STAT_ZERO: (u64, f64, f64, f64, f64) = (0, 0.0, 0.0, f64::INFINITY, f64::NEG_INFINITY);

/// Built-in: single-pass `(count, mean, m2, min, max)` accumulator behind `stats()`.
///
/// The [`AggregateTask`] shape combines Welford's online `(count, mean, m2)` (as in
/// [`VarianceTask`](crate::builtin_tasks::variance::VarianceTask)) with running min/max, so one
/// worker pass yields every summary statistic. Registered for the numeric primitives via
/// [`register_aggregate_task!`], keyed `atomic::builtin::stats::<ty>`.
#[derive(Clone, Copy, Default)]
pub struct StatCounterTask<T>(std::marker::PhantomData<T>);

macro_rules! impl_stat_counter_task {
    ($ty:ty) => {
        impl AggregateTask<(u64, f64, f64, f64, f64), $ty> for StatCounterTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::stats::", stringify!($ty));

            fn seq(
                &self,
                (count, mean, m2, min, max): (u64, f64, f64, f64, f64),
                x: $ty,
            ) -> (u64, f64, f64, f64, f64) {
                let x = x as f64;
                let count = count + 1;
                let delta = x - mean;
                let mean = mean + delta / count as f64;
                let delta2 = x - mean;
                (count, mean, m2 + delta * delta2, min.min(x), max.max(x))
            }

            fn comb(
                &self,
                a: (u64, f64, f64, f64, f64),
                b: (u64, f64, f64, f64, f64),
            ) -> (u64, f64, f64, f64, f64) {
                let (na, mean_a, m2a, min_a, max_a) = a;
                let (nb, mean_b, m2b, min_b, max_b) = b;
                let n = na + nb;
                if n == 0 {
                    return STAT_ZERO;
                }
                let delta = mean_b - mean_a;
                let mean = mean_a + delta * nb as f64 / n as f64;
                let m2 = m2a + m2b + delta * delta * (na as f64) * (nb as f64) / n as f64;
                (n, mean, m2, min_a.min(min_b), max_a.max(max_b))
            }
        }

        register_aggregate_task!(StatCounterTask<$ty>, (u64, f64, f64, f64, f64), $ty);
    };
}

impl_stat_counter_task!(i32);
impl_stat_counter_task!(i64);
impl_stat_counter_task!(u32);
impl_stat_counter_task!(u64);
impl_stat_counter_task!(f32);
impl_stat_counter_task!(f64);
