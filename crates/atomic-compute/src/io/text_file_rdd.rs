/// A lazy RDD over text lines from local files or S3 objects.
///
/// Each partition corresponds to one source (file path or S3 key). Lines are read
/// lazily in `compute()` — one partition per `context.parallelize_typed` call is
/// never made so the driver does not read all files before the job starts.
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use atomic_data::data::Data;
use atomic_data::dependency::Dependency;
use atomic_data::error::BaseError;
use atomic_data::rdd::{Rdd, RddBase};
use atomic_data::split::Split;

use crate::rdd::rdd_val::RddVals;

// ── TextFileSource ─────────────────────────────────────────────────────────────

/// One partition source — either a local file or an S3 object key.
#[derive(Debug, Clone)]
pub enum TextFileSource {
    Local(PathBuf),
    #[cfg(feature = "s3")]
    S3 { bucket: String, key: String },
}

// ── SimpleSplit ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct TextFileSplit {
    index: usize,
}

impl Split for TextFileSplit {
    fn get_index(&self) -> usize {
        self.index
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// ── TextFileRdd ────────────────────────────────────────────────────────────────

/// Lazy text-line RDD. Each element is one line (`String`) from the source.
pub struct TextFileRdd {
    vals: Arc<RddVals>,
    sources: Vec<TextFileSource>,
}

impl TextFileRdd {
    pub fn new(id: usize, sources: Vec<TextFileSource>) -> Self {
        TextFileRdd { vals: Arc::new(RddVals::new(id)), sources }
    }
}

impl Clone for TextFileRdd {
    fn clone(&self) -> Self {
        TextFileRdd { vals: self.vals.clone(), sources: self.sources.clone() }
    }
}

impl RddBase for TextFileRdd {
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_op_name(&self) -> String {
        "text_file".to_owned()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        vec![]
    }

    fn preferred_locations(&self, _split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        vec![]
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        (0..self.sources.len())
            .map(|i| Box::new(TextFileSplit { index: i }) as Box<dyn Split>)
            .collect()
    }

    fn number_of_splits(&self) -> usize {
        self.sources.len()
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        Ok(Box::new(
            self.compute(split)?.map(|s| Box::new(s) as Box<dyn Data>),
        ))
    }
}

impl Rdd for TextFileRdd {
    type Item = String;

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = String>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = String>>, BaseError> {
        let idx = split.get_index();
        let source = self.sources.get(idx).ok_or_else(|| {
            BaseError::Other(format!("TextFileRdd: partition {idx} out of range"))
        })?;

        let lines: Vec<String> = match source {
            TextFileSource::Local(path) => {
                use std::io::BufRead;
                let file = std::fs::File::open(path).map_err(|e| {
                    BaseError::Other(format!("text_file: cannot open {}: {e}", path.display()))
                })?;
                std::io::BufReader::new(file).lines().filter_map(|l| l.ok()).collect()
            }

            #[cfg(feature = "s3")]
            TextFileSource::S3 { bucket, key } => {
                crate::io::s3::s3_impl::read_lines(bucket, key)
            }
        };

        Ok(Box::new(lines.into_iter()))
    }
}
