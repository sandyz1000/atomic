use std::sync::Arc;

use crate::{context::Context, rdd::AnyData};
use crate::rdd::Rdd;



pub mod local_file;
pub use local_file::reader::{LocalFsReader, LocalFsReaderConfig};

pub trait ReaderConfiguration<I: AnyData> {
    fn make_reader<F, O>(self, context: Arc<Context>, decoder: F) -> Arc<dyn Rdd<Item = O>>
    where
        O: AnyData,
        F: Fn(I) -> O;
}
