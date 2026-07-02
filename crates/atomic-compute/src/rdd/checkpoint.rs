/// RDD checkpointing — lineage truncation.
///
/// `CheckpointRdd<T>` is a leaf RDD that reads pre-materialized partitions from disk
/// (local path or `s3://` when the `s3` feature is enabled). It has no parent
/// dependencies, so the entire upstream DAG is truncated.
///
/// Create via `TypedRdd::checkpoint(dir)` which materialises the current RDD, writes
/// each partition to `{dir}/{rdd_id}/{partition}.bin`, and returns a new `TypedRdd`
/// backed by `CheckpointRdd`.
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use atomic_data::data::Data;
use atomic_data::dependency::Dependency;
use atomic_data::error::BaseError;
use atomic_data::rdd::{Rdd, RddBase};
use atomic_data::split::Split;

use crate::rdd::rdd_val::RddVals;

#[derive(Debug, Clone)]
struct CheckpointSplit {
    index: usize,
}

impl Split for CheckpointSplit {
    fn get_index(&self) -> usize {
        self.index
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Where checkpoint data lives — local directory or S3 prefix.
#[derive(Debug, Clone)]
pub enum CheckpointStore {
    Local(PathBuf),
    #[cfg(feature = "s3")]
    S3 {
        bucket: String,
        prefix: String,
    },
}

impl CheckpointStore {
    /// Parse a URI into a `CheckpointStore`.
    pub fn from_uri(uri: &str) -> Self {
        if uri.starts_with("s3://") {
            if let Some(store) = s3_checkpoint_store(uri) {
                return store;
            }
            log::warn!("CheckpointStore: s3:// URI requires 's3' feature; falling back to /tmp");
            CheckpointStore::Local(std::env::temp_dir())
        } else {
            CheckpointStore::Local(PathBuf::from(uri.strip_prefix("file://").unwrap_or(uri)))
        }
    }

    /// File path for the given rdd_id and partition index (local only).
    pub fn local_partition_path(&self, rdd_id: usize, partition: usize) -> Option<PathBuf> {
        match self {
            CheckpointStore::Local(base) => Some(
                base.join(format!("{rdd_id}"))
                    .join(format!("{partition}.bin")),
            ),
            #[cfg(feature = "s3")]
            CheckpointStore::S3 { .. } => None,
        }
    }

    crate::cfg_s3! {
    /// S3 key for a given rdd_id / partition (S3 only).
    pub fn s3_partition_key(&self, rdd_id: usize, partition: usize) -> Option<(&str, String)> {
        match self {
            CheckpointStore::S3 { bucket, prefix } => {
                Some((bucket, format!("{prefix}/{rdd_id}/{partition}.bin")))
            }
            CheckpointStore::Local(_) => None,
        }
    }
    } // cfg_s3!
}

crate::cfg_s3! {
    fn s3_checkpoint_store(uri: &str) -> Option<CheckpointStore> {
        use crate::io::s3::s3_impl::S3Uri;
        let s3 = S3Uri::parse(uri)?;
        Some(CheckpointStore::S3 {
            bucket: s3.bucket,
            prefix: s3.key,
        })
    }
}
crate::cfg_not_s3! {
    fn s3_checkpoint_store(_uri: &str) -> Option<CheckpointStore> {
        None
    }
}

/// Leaf RDD that reads pre-checkpointed partitions from a `CheckpointStore`.
///
/// Has no parent dependencies — lineage is fully truncated.
pub struct CheckpointRdd<T> {
    vals: Arc<RddVals>,
    store: CheckpointStore,
    num_partitions: usize,
    _phantom: PhantomData<T>,
}

impl<T> CheckpointRdd<T> {
    pub fn new(id: usize, store: CheckpointStore, num_partitions: usize) -> Self {
        CheckpointRdd {
            vals: Arc::new(RddVals::new(id)),
            store,
            num_partitions,
            _phantom: PhantomData,
        }
    }
}

impl<T> Clone for CheckpointRdd<T> {
    fn clone(&self) -> Self {
        CheckpointRdd {
            vals: self.vals.clone(),
            store: self.store.clone(),
            num_partitions: self.num_partitions,
            _phantom: PhantomData,
        }
    }
}

impl<T: Data + Clone + bincode::Decode<()> + 'static> RddBase for CheckpointRdd<T> {
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_op_name(&self) -> String {
        "checkpoint".to_owned()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        vec![] // lineage is truncated
    }

    fn preferred_locations(&self, _split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        vec![]
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        (0..self.num_partitions)
            .map(|i| Box::new(CheckpointSplit { index: i }) as Box<dyn Split>)
            .collect()
    }

    fn number_of_splits(&self) -> usize {
        self.num_partitions
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn Data>>>, BaseError> {
        Ok(Box::new(
            self.compute(split)?.map(|x| Box::new(x) as Box<dyn Data>),
        ))
    }
}

impl<T> Rdd for CheckpointRdd<T>
where
    T: Data + Clone + bincode::Decode<()> + 'static,
{
    type Item = T;

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = T>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = T>>, BaseError> {
        let idx = split.get_index();

        match &self.store {
            CheckpointStore::Local(base) => {
                let path = base
                    .join(format!("{}", self.vals.id))
                    .join(format!("{idx}.bin"));
                use crate::rdd::cached::disk_read_partition;
                let items = disk_read_partition::<T>(&path).map_err(|e| {
                    BaseError::Other(format!("checkpoint read failed at {}: {e}", path.display()))
                })?;
                Ok(Box::new(items.into_iter()))
            }

            #[cfg(feature = "s3")]
            CheckpointStore::S3 { bucket, prefix } => {
                use crate::io::s3::s3_impl::read_lines;
                // For S3 we store bincode-encoded bytes as a base64 object.
                // Read the object, base64-decode, then bincode-decode.
                let key = format!("{prefix}/{}/{idx}.bin", self.vals.id);
                let lines = read_lines(bucket, &key);
                let b64 = lines.into_iter().collect::<Vec<_>>().join("");
                let bytes =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64.trim())
                        .map_err(|e| {
                            BaseError::Other(format!("checkpoint s3 base64 decode: {e}"))
                        })?;
                let (items, _) =
                    bincode::decode_from_slice::<Vec<T>, _>(&bytes, bincode::config::standard())
                        .map_err(|e| {
                            BaseError::Other(format!("checkpoint s3 bincode decode: {e}"))
                        })?;
                Ok(Box::new(items.into_iter()))
            }
        }
    }
}
