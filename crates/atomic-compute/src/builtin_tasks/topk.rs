use atomic_data::distributed::WireDecode;
use atomic_data::error::DataResult;

use crate::register_partition_task;
use crate::task_traits::PartitionTask;

/// Built-in: return the top K elements in descending order from a partition.
///
/// Used by `TypedRdd::top(k)` in distributed mode. Each partition produces its
/// local top-K; the driver merges them and takes the global top-K.
///
/// `payload` must be a wire-encoded `u64` (the K value).
#[derive(Default)]
pub struct TopKTask<T>(std::marker::PhantomData<T>);

macro_rules! impl_top_k_task {
    ($ty:ty) => {
        impl PartitionTask<$ty> for TopKTask<$ty> {
            const NAME: &'static str = concat!("atomic::builtin::top_k::", stringify!($ty));
            fn transform(&self, mut items: Vec<$ty>, payload: &[u8]) -> DataResult<Vec<$ty>> {
                let k = u64::decode_wire(payload)? as usize;
                items.sort_by(|a, b| b.partial_cmp(a).unwrap_or(::std::cmp::Ordering::Equal));
                items.truncate(k);
                Ok(items)
            }
        }

        register_partition_task!(TopKTask<$ty>, $ty);
    };
}

impl_top_k_task!(i32);
impl_top_k_task!(i64);
impl_top_k_task!(u32);
impl_top_k_task!(u64);
impl_top_k_task!(f32);
impl_top_k_task!(f64);
impl_top_k_task!(String);
