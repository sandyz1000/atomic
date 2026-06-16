use std::sync::Arc;

use atomic_data::data::Data;
use atomic_data::rdd::Rdd;

use crate::io::ReaderConfiguration;
use crate::rdd::typed::TypedRdd;
use crate::rdd::{ParallelCollection, UnionRdd};

use super::Context;

impl Context {
    pub fn make_rdd<T: Data + Clone, I>(
        self: &Arc<Self>,
        seq: I,
        num_slices: usize,
    ) -> Arc<dyn Rdd<Item = T>>
    where
        I: IntoIterator<Item = T>,
    {
        let rdd = self.parallelize(seq, num_slices);
        rdd.register_op_name("make_rdd");
        rdd
    }

    pub fn range(
        self: &Arc<Self>,
        start: u64,
        end: u64,
        step: usize,
        num_slices: usize,
    ) -> Arc<dyn Rdd<Item = u64>> {
        let seq = (start..=end).step_by(step);
        let rdd = self.parallelize(seq, num_slices);
        rdd.register_op_name("range");
        rdd
    }

    pub fn parallelize<T: Data + Clone, I>(
        self: &Arc<Self>,
        seq: I,
        num_slices: usize,
    ) -> Arc<dyn Rdd<Item = T>>
    where
        I: IntoIterator<Item = T>,
    {
        let id = self.new_rdd_id();
        Arc::new(ParallelCollection::new(id, seq, num_slices))
    }

    /// Create a TypedRdd from a collection with explicit typing.
    pub fn parallelize_typed<T: Data + Clone, I>(
        self: &Arc<Self>,
        seq: I,
        num_slices: usize,
    ) -> TypedRdd<T>
    where
        I: IntoIterator<Item = T>,
    {
        let id = self.new_rdd_id();
        let rdd = Arc::new(ParallelCollection::new(id, seq, num_slices));
        TypedRdd::new(rdd, self.clone())
    }

    pub fn read_source<F, C, I: Data, O: Data>(
        self: &Arc<Self>,
        config: C,
        func: F,
    ) -> Arc<dyn Rdd<Item = O>>
    where
        F: Fn(I) -> O + Send + Sync + 'static,
        C: ReaderConfiguration<I>,
    {
        config.make_reader(self.clone(), func)
    }

    /// Read a text file (or directory of files, or S3 prefix) as a `TypedRdd<String>`.
    ///
    /// URI schemes:
    /// - `s3://bucket/prefix` — lists all objects under the prefix; each key is one partition.
    ///   Requires the `s3` feature flag.
    /// - `file:///absolute/path` or `/absolute/path` or `relative/path` — reads a local file
    ///   (single partition) or, if the path is a directory, all files in the directory (one
    ///   partition per file).
    ///
    /// Each partition yields one line per element.
    pub fn text_file(self: &Arc<Self>, uri: &str) -> TypedRdd<String> {
        use crate::io::{TextFileRdd, TextFileSource};

        let sources: Vec<TextFileSource> = if uri.starts_with("s3://") {
            #[cfg(feature = "s3")]
            {
                use crate::io::s3::s3_impl::S3Uri;
                if let Some(s3uri) = S3Uri::parse(uri) {
                    let keys = crate::io::s3::s3_impl::list_keys(&s3uri.bucket, &s3uri.key);
                    if keys.is_empty() {
                        vec![TextFileSource::S3 {
                            bucket: s3uri.bucket,
                            key: s3uri.key,
                        }]
                    } else {
                        keys.into_iter()
                            .map(|k| TextFileSource::S3 {
                                bucket: s3uri.bucket.clone(),
                                key: k,
                            })
                            .collect()
                    }
                } else {
                    vec![]
                }
            }
            #[cfg(not(feature = "s3"))]
            {
                log::warn!("text_file: s3:// URI requested but 's3' feature is disabled");
                vec![]
            }
        } else {
            let path = std::path::Path::new(uri.strip_prefix("file://").unwrap_or(uri));
            if path.is_dir() {
                std::fs::read_dir(path)
                    .into_iter()
                    .flatten()
                    .filter_map(|entry| entry.ok())
                    .map(|entry| TextFileSource::Local(entry.path()))
                    .collect()
            } else {
                vec![TextFileSource::Local(path.to_path_buf())]
            }
        };

        let id = self.new_rdd_id();
        let rdd = Arc::new(TextFileRdd::new(id, sources));
        TypedRdd::new(rdd, self.clone())
    }

    pub fn union<T: Data + Clone>(
        self: &Arc<Self>,
        rdds: &[Arc<dyn Rdd<Item = T>>],
    ) -> crate::error::ComputeResult<UnionRdd<T>> {
        UnionRdd::new(self.new_rdd_id(), rdds)
    }
}
