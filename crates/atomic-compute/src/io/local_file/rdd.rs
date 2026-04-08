use std::net::Ipv4Addr;
use std::sync::Arc;

use atomic_data::data::Data;
use atomic_data::dependency::Dependency;
use atomic_data::error::BaseError;
use atomic_data::rdd::{Rdd, RddBase};
use atomic_data::split::{BytesReader, FileReader, Split};

use super::reader::LocalFsReader;

type Result<T> = std::result::Result<T, BaseError>;

impl RddBase for LocalFsReader<BytesReader> {
    fn get_rdd_id(&self) -> usize {
        self.id
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        vec![]
    }

    fn is_pinned(&self) -> bool {
        true
    }

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        // for a given split there is only one preferred location because this is pinned,
        // the preferred location is the host at which this split will be executed;
        let split = split.as_any().downcast_ref::<BytesReader>().unwrap();
        vec![split.host]
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        let mut splits = Vec::with_capacity(self.splits.len());
        for (idx, host) in self.splits.iter().enumerate() {
            splits.push(Box::new(BytesReader {
                idx,
                host: *host.ip(),
                files: Vec::new(),
            }) as Box<dyn Split>)
        }
        splits
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        Ok(Box::new(
            self.compute(split)?
                .map(|reader| Box::new(reader) as Box<dyn Data>),
        ))
    }
}

impl RddBase for LocalFsReader<FileReader> {
    fn get_rdd_id(&self) -> usize {
        self.id
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        vec![]
    }

    fn is_pinned(&self) -> bool {
        true
    }

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        let split = split.as_any().downcast_ref::<FileReader>().unwrap();
        vec![split.host]
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        let mut splits = Vec::with_capacity(self.splits.len());
        for (idx, host) in self.splits.iter().enumerate() {
            splits.push(Box::new(FileReader {
                idx,
                host: *host.ip(),
                files: Vec::new(),
            }) as Box<dyn Split>)
        }
        splits
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>> {
        Ok(Box::new(
            self.compute(split)?
                .map(|reader| Box::new(reader) as Box<dyn Data>),
        ))
    }
}

impl Rdd for LocalFsReader<BytesReader> {
    type Item = BytesReader;

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let split = split.as_any().downcast_ref::<BytesReader>().unwrap();
        let files_by_part = self.load_local_files()?;
        let idx = split.idx;
        let host = split.host;
        Ok(Box::new(
            files_by_part
                .into_iter()
                .map(move |files| BytesReader { files, host, idx }),
        ) as Box<dyn Iterator<Item = Self::Item>>)
    }
}

impl Rdd for LocalFsReader<FileReader> {
    type Item = FileReader;

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let split = split.as_any().downcast_ref::<FileReader>().unwrap();
        let files_by_part = self.load_local_files()?;
        let idx = split.idx;
        let host = split.host;
        Ok(Box::new(
            files_by_part
                .into_iter()
                .map(move |files| FileReader { files, host, idx }),
        ) as Box<dyn Iterator<Item = Self::Item>>)
    }
}
