use std::collections::HashSet;

use atomic_data::error::DataResult;

use crate::register_partition_task;
use crate::task_traits::PartitionTask;

/// Built-in: remove duplicate elements within a partition.
///
/// Used as the local combine step in `TypedRdd::distinct()`. After each
/// partition deduplicates locally, a shuffle groups all copies of each element
/// to one partition, where a final `DistinctTask` pass finishes the job.
#[derive(Default)]
pub struct DistinctTask<T>(std::marker::PhantomData<T>);

macro_rules! impl_distinct_task {
    ($ty:ty) => {
        impl PartitionTask<$ty> for DistinctTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::distinct::", stringify!($ty));
            fn transform(&self, items: Vec<$ty>, _payload: &[u8]) -> DataResult<Vec<$ty>> {
                Ok(items
                    .into_iter()
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect())
            }
        }

        register_partition_task!(DistinctTask<$ty>, $ty);
    };
}

impl_distinct_task!(i32);
impl_distinct_task!(i64);
impl_distinct_task!(u32);
impl_distinct_task!(u64);
impl_distinct_task!(String);
