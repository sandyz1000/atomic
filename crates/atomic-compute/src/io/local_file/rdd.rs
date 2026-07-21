use std::net::Ipv4Addr;
use std::sync::Arc;

use atomic_data::data::Data;
use atomic_data::dependency::Dependency;
use atomic_data::error::DataError;
use atomic_data::rdd::{Rdd, RddBase};
use atomic_data::split::{BytesReader, FileReader, Split};

use super::reader::LocalFsReader;

type Result<T> = std::result::Result<T, DataError>;

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
        // Pinned RDD: the preferred location is the host embedded in the split.
        if let Some(split) = split.as_any().downcast_ref::<BytesReader>() {
            vec![split.host]
        } else {
            vec![]
        }
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
        if let Some(split) = split.as_any().downcast_ref::<FileReader>() {
            vec![split.host]
        } else {
            vec![]
        }
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
        let split = split
            .as_any()
            .downcast_ref::<BytesReader>()
            .ok_or_else(|| {
                DataError::DowncastFailure("expected BytesReader split for LocalFsReader".into())
            })?;
        let files_by_part = self.load_local_files()?;
        let idx = split.idx;
        let host = split.host;
        let byte_iter =
            files_by_part
                .into_iter()
                .map(move |files| BytesReader { files, host, idx });

        Ok(Box::new(byte_iter) as Box<dyn Iterator<Item = Self::Item>>)
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
        let split = split.as_any().downcast_ref::<FileReader>().ok_or_else(|| {
            DataError::DowncastFailure("expected FileReader split for LocalFsReader".into())
        })?;
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
