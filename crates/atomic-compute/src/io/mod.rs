use std::sync::Arc;
use crate::context::Context;
use atomic_data::{data::Data, rdd::Rdd};



pub mod local_file;
pub use local_file::reader::{LocalFsReader, LocalFsReaderConfig};

pub trait ReaderConfiguration<I: Data> {
    fn make_reader<F, O>(self, context: Arc<Context>, decoder: F) -> Arc<dyn Rdd<Item = O>>
    where
        O: Data,
        F: Fn(I) -> O + Send + Sync + 'static;
}
