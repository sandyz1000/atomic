use std::sync::Arc;

use crate::context::Context;
use crate::rdd::rdd::Rdd;
use crate::serializable_traits::Data;
use core::ops::Fn as SerFunc;

pub mod local_file_reader;
pub mod decoders;
pub mod local_fs_io;
pub mod hdfs_io;
pub use local_file_reader::{LocalFsReader, LocalFsReaderConfig};


pub trait ReaderConfiguration<I: Data> {
    fn make_reader<F, O>(self, context: Arc<Context>, decoder: F) -> Arc<dyn Rdd<Item = O>>
    where
        O: Data,
        F: SerFunc(I) -> O;
}
