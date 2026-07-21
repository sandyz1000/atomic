//! A lazy RDD over text lines from pluggable sources (local files, S3 objects, …).
//!
//! Each partition is one [`TextFileSource`]. The RDD is source-agnostic: it calls
//! [`TextFileSource::read_lines`] and never matches on a concrete scheme. A new backend is a
//! new `impl TextFileSource` plus one scheme branch in [`resolve`].
use std::fmt::Debug;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use atomic_data::data::Data;
use atomic_data::dependency::Dependency;
use atomic_data::error::DataError;
use atomic_data::rdd::{Rdd, RddBase};
use atomic_data::split::Split;

use crate::rdd::rdd_val::RddVals;

/// One partition's text source. Object-safe so the RDD holds a heterogeneous set
/// (`Arc<dyn TextFileSource>`) without knowing any concrete scheme.
pub trait TextFileSource: Send + Sync + Debug {
    /// Read the whole source and return its content as lines.
    fn read_lines(&self) -> Result<Vec<String>, DataError>;
}

/// A local filesystem file.
#[derive(Debug)]
pub struct LocalTextFile(pub PathBuf);

impl TextFileSource for LocalTextFile {
    fn read_lines(&self) -> Result<Vec<String>, DataError> {
        use std::io::BufRead;
        let file = std::fs::File::open(&self.0).map_err(|e| {
            DataError::Other(format!("text_file: cannot open {}: {e}", self.0.display()))
        })?;
        Ok(std::io::BufReader::new(file)
            .lines()
            .map_while(Result::ok)
            .collect())
    }
}

/// A single S3 object (`s3://bucket/key`).
#[derive(Debug)]
pub struct S3TextFile {
    pub bucket: String,
    pub key: String,
}

impl TextFileSource for S3TextFile {
    fn read_lines(&self) -> Result<Vec<String>, DataError> {
        Ok(crate::io::s3::read_lines(&self.bucket, &self.key))
    }
}

/// Parse a URI into one or more partition sources by scheme.
///
/// - `s3://bucket/prefix` — lists objects under the prefix (one source per key).
/// - `file:///path` / `/path` / `relative/path` — a local file, or every file in a directory
///   (one source per file).
pub fn resolve(uri: &str) -> Vec<Arc<dyn TextFileSource>> {
    if let Some(s3uri) = crate::io::s3::S3Uri::parse(uri) {
        let keys = crate::io::s3::list_keys(&s3uri.bucket, &s3uri.key);
        if keys.is_empty() {
            return vec![Arc::new(S3TextFile {
                bucket: s3uri.bucket,
                key: s3uri.key,
            })];
        }
        return keys
            .into_iter()
            .map(|key| {
                Arc::new(S3TextFile {
                    bucket: s3uri.bucket.clone(),
                    key,
                }) as Arc<dyn TextFileSource>
            })
            .collect();
    }

    let path = std::path::Path::new(uri.strip_prefix("file://").unwrap_or(uri));
    if path.is_dir() {
        std::fs::read_dir(path)
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|entry| Arc::new(LocalTextFile(entry.path())) as Arc<dyn TextFileSource>)
            .collect()
    } else {
        vec![Arc::new(LocalTextFile(path.to_path_buf()))]
    }
}

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

/// Lazy text-line RDD. Each element is one line (`String`) from the source.
pub struct TextFileRdd {
    vals: Arc<RddVals>,
    sources: Vec<Arc<dyn TextFileSource>>,
}

impl TextFileRdd {
    pub fn new(id: usize, sources: Vec<Arc<dyn TextFileSource>>) -> Self {
        TextFileRdd {
            vals: Arc::new(RddVals::new(id)),
            sources,
        }
    }
}

impl Clone for TextFileRdd {
    fn clone(&self) -> Self {
        TextFileRdd {
            vals: self.vals.clone(),
            sources: self.sources.clone(),
        }
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
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, DataError> {
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

    fn compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = String>>, DataError> {
        let idx = split.get_index();
        let source = self.sources.get(idx).ok_or_else(|| {
            DataError::Other(format!("TextFileRdd: partition {idx} out of range"))
        })?;
        let lines = source.read_lines()?;
        Ok(Box::new(lines.into_iter()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_file(name: &str, contents: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("atomic-tf-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn local_reads_lines() {
        let path = temp_file("a.txt", "l1\nl2\nl3\n");
        let lines = LocalTextFile(path.clone()).read_lines().unwrap();
        assert_eq!(lines, vec!["l1", "l2", "l3"]);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn resolve_local_file() {
        let path = temp_file("b.txt", "x\ny\n");
        let sources = resolve(path.to_str().unwrap());
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].read_lines().unwrap(), vec!["x", "y"]);
        std::fs::remove_file(path).ok();
    }
}
