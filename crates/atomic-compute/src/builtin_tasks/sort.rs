use atomic_data::error::DataResult;

use crate::register_partition_task;
use crate::task_traits::PartitionTask;

/// Built-in: sort all elements within a partition in ascending order.
///
/// Used by `TypedRdd::sort_within_partitions()` and as the local sort step
/// before a merge-sort across partitions for `sort_by` / `sort_by_key`.
#[derive(Default)]
pub struct SortTask<T>(std::marker::PhantomData<T>);

macro_rules! impl_sort_task {
    ($ty:ty) => {
        impl PartitionTask<$ty> for SortTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::sort::", stringify!($ty));
            fn transform(&self, mut items: Vec<$ty>, _payload: &[u8]) -> DataResult<Vec<$ty>> {
                items.sort_by(|a, b| a.partial_cmp(b).unwrap_or(::std::cmp::Ordering::Equal));
                Ok(items)
            }
        }

        register_partition_task!(SortTask<$ty>, $ty);
    };
}

impl_sort_task!(i32);
impl_sort_task!(i64);
impl_sort_task!(u32);
impl_sort_task!(u64);
impl_sort_task!(f32);
impl_sort_task!(f64);
