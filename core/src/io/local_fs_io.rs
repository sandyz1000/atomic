use crate::dependency::{NarrowDependencyTrait, ShuffleDependencyTrait};
use crate::io::*;
use crate::rdd::local_fs_read_rdd::LocalFsReadRdd;

pub struct LocalFsIO {}

impl LocalFsIO {

    pub fn read_to_rdd<ND: NarrowDependencyTrait, SD: ShuffleDependencyTrait>(
        path: &str,
        context: &Arc<Context>,
        num_slices: usize,
    ) -> LocalFsReadRdd<ND, SD>
    {
        let rdd = LocalFsReadRdd::new(context.clone(), path.to_string(), num_slices);
        rdd
    }

    pub fn read_to_rdd_and_decode<U, F>(
        path: &str,
        context: &Arc<Context>,
        num_slices: usize,
        decoder: F,
    ) -> Arc<impl Rdd<Item = U>>
    where
        F: SerFunc<Vec<u8>, Output = U>,
        U: Data,
    {
        let rdd = LocalFsReadRdd::new(context.clone(), path.to_string(), num_slices);
        let rdd = rdd.map(decoder);
        rdd
    }
}
