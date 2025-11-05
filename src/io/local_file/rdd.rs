// macro_rules! impl_common_lfs_rddb_funcs {
//     () => {
//         fn get_rdd_id(&self) -> usize {
//             self.id
//         }

//         fn get_context(&self) -> Arc<Context> {
//             self.context.clone()
//         }

//         fn get_dependencies(&self) -> Vec<Dependency> {
//             vec![]
//         }

//         fn is_pinned(&self) -> bool {
//             true
//         }
//     };
// }

impl RddBase for LocalFsReader<BytesReader> {
    // impl_common_lfs_rddb_funcs!();

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        // for a given split there is only one preferred location because this is pinned,
        // the preferred location is the host at which this split will be executed;
        let split = split.downcast_ref::<BytesReader>().unwrap();
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
}

impl RddBase for LocalFsReader<FileReader> {
    // impl_common_lfs_rddb_funcs!();

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        let split = split.downcast_ref::<FileReader>().unwrap();
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
}

// macro_rules! impl_common_lfs_rdd_funcs {
//     () => {
//         fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>>
//         where
//             Self: Sized,
//         {
//             Arc::new(self.clone()) as Arc<dyn Rdd<Item = Self::Item>>
//         }

//         fn get_rdd_base(&self) -> Arc<dyn RddBase> {
//             Arc::new(self.clone()) as Arc<dyn RddBase>
//         }
//     };
// }

impl Rdd for LocalFsReader<BytesReader> {
    type Item = BytesReader;

    // impl_common_lfs_rdd_funcs!();

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let split = split.downcast_ref::<BytesReader>().unwrap();
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

    // impl_common_lfs_rdd_funcs!();

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let split = split.downcast_ref::<FileReader>().unwrap();
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
