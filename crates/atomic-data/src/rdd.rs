use crate::{
    data::Data, dependency::Dependency, error::BaseResult, partitioner::Partitioner, split::Split,
};

use std::sync::Arc;

pub trait RddBase: Send + Sync {
    fn get_rdd_id(&self) -> usize;

    fn get_op_name(&self) -> String {
        "unknown".to_owned()
    }

    fn register_op_name(&self, _name: &str) {
        log::debug!("couldn't register op name")
    }
    fn get_dependencies(&self) -> Vec<Dependency>;

    fn preferred_locations(&self, _split: Box<dyn Split>) -> Vec<std::net::Ipv4Addr> {
        Vec::new()
    }
    fn partitioner(&self) -> Option<Partitioner> {
        None
    }
    fn splits(&self) -> Vec<Box<dyn Split>>;

    fn number_of_splits(&self) -> usize {
        self.splits().len()
    }

    // Analyse whether this is required or not. It requires downcasting while executing tasks which could hurt performance.
    // TODO: Rename the below method to use only the prefix noun and remove any suffix _any
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> BaseResult<Box<dyn Iterator<Item = Box<dyn Data>>>>;

    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> BaseResult<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        self.iterator_any(split)
    }

    fn is_pinned(&self) -> bool {
        false
    }

    fn wasm_bytes(&self, _partition: usize) -> Option<BaseResult<Vec<u8>>> {
        None
    }
}

// Rdd containing methods associated with processing
pub trait Rdd: RddBase + 'static {
    type Item: Data;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>>;

    fn get_rdd_base(&self) -> Arc<dyn RddBase>;

    fn compute(&self, split: Box<dyn Split>) -> BaseResult<Box<dyn Iterator<Item = Self::Item>>>;

    fn iterator(&self, split: Box<dyn Split>) -> BaseResult<Box<dyn Iterator<Item = Self::Item>>> {
        self.compute(split)
    }
}

pub trait Reduce<T> {
    fn reduce<F>(self, f: F) -> Option<T>
    where
        F: FnMut(T, T) -> T;
}
