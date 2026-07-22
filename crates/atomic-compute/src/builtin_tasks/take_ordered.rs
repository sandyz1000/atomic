use atomic_data::distributed::WireDecode;
use atomic_data::error::DataResult;

use crate::register_partition_task;
use crate::task_traits::PartitionTask;

/// Built-in: return the first K elements in ascending order from a partition.
///
/// Used by `TypedRdd::take_ordered(k)` in distributed mode. Each partition
/// produces its local first-K ascending; the driver merges and re-truncates.
///
/// `payload` must be a wire-encoded `u64` (the K value).
#[derive(Default)]
pub struct TakeOrderedTask<T>(std::marker::PhantomData<T>);

macro_rules! impl_take_ordered_task {
    ($ty:ty) => {
        impl PartitionTask<$ty> for TakeOrderedTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::take_ordered::", stringify!($ty));
            fn transform(&self, mut items: Vec<$ty>, payload: &[u8]) -> DataResult<Vec<$ty>> {
                let k = u64::decode_wire(payload)? as usize;
                items.sort_by(|a, b| a.partial_cmp(b).unwrap_or(::std::cmp::Ordering::Equal));
                items.truncate(k);
                Ok(items)
            }
        }

        register_partition_task!(TakeOrderedTask<$ty>, $ty);
    };
}

impl_take_ordered_task!(i32);
impl_take_ordered_task!(i64);
impl_take_ordered_task!(u32);
impl_take_ordered_task!(u64);
impl_take_ordered_task!(f32);
impl_take_ordered_task!(f64);
impl_take_ordered_task!(String);
