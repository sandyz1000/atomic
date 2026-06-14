use crate::context::Context;
use atomic_data::{data::Data, rdd::Rdd};
use std::sync::Arc;

pub mod local_file;
pub mod text_file_rdd;

#[cfg(feature = "s3")]
pub mod s3;

pub use local_file::reader::{LocalFsReader, LocalFsReaderConfig};
pub use text_file_rdd::{TextFileRdd, TextFileSource};

pub trait ReaderConfiguration<I: Data> {
    fn make_reader<F, O>(self, context: Arc<Context>, decoder: F) -> Arc<dyn Rdd<Item = O>>
    where
        O: Data,
        F: Fn(I) -> O + Send + Sync + 'static;
}
