use super::*;

impl<T: Data + Clone + 'static> TypedRdd<T> {
    /// Persist this RDD's partitions in memory so they are not recomputed
    /// across multiple actions.
    ///
    /// Equivalent to `persist(StorageLevel::MemoryOnly)`.
    pub fn cache(self) -> Self {
        self.persist(StorageLevel::MemoryOnly)
    }

    /// Persist this RDD's partitions using the given storage level.
    ///
    /// - `MemoryOnly` / `MemoryOnlySer`: memoises in the global `PartitionStore` (LRU-bounded).
    /// - `MemoryAndDisk` / `DiskOnly`: accepted but fall back to memory semantics unless `T`
    ///   implements `bincode::Encode + bincode::Decode<()>`. For actual disk spill, call
    ///   `persist_with_disk(level)` instead.
    pub fn persist(self, level: StorageLevel) -> Self {
        let ctx = self.context.clone();
        let cached = Arc::new(CachedRdd::new_with_level(self.into_rdd(), level));
        TypedRdd::new(cached as RddRef<T>, ctx)
    }

    /// Persist with real disk spill for `MemoryAndDisk` and `DiskOnly` levels.
    ///
    /// Requires `T: bincode::Encode + bincode::Decode<()>` so partitions can be
    /// serialized to `{work_dir}/rdd-cache/{rdd_id}/{partition}.bin`.
    ///
    /// - `MemoryAndDisk`: memory-first; on LRU eviction falls back to disk; on miss reads disk.
    /// - `DiskOnly`: always reads from disk; never occupies `PartitionStore` memory.
    /// - Other levels: identical to `persist(level)`.
    pub fn persist_with_disk(self, level: StorageLevel) -> Self
    where
        T: bincode::Encode + bincode::Decode<()>,
    {
        use atomic_data::cache::{PARTITION_CACHE, disk_write_partition};

        match level {
            StorageLevel::MemoryAndDisk => {
                let ctx = self.context.clone();
                let num_parts = self.rdd.number_of_splits();
                // Wrap lazily — spill registration replaces eager materialisation.
                let cached = Arc::new(CachedRdd::new_with_level(
                    self.rdd.clone(),
                    StorageLevel::MemoryAndDisk,
                ));
                let rdd_id = cached.rdd_id();
                // Pre-register per-partition spill handlers so that:
                //   * put() writes to disk on LRU eviction
                //   * get() reads from disk on a memory miss
                if let Some(store) = PARTITION_CACHE.get() {
                    for part_idx in 0..num_parts {
                        if let Some(path) = cached.spill_path(part_idx) {
                            store.register_spill_path::<T>(rdd_id, part_idx, path);
                        }
                    }
                }
                TypedRdd::new(cached as RddRef<T>, ctx)
            }

            StorageLevel::DiskOnly => {
                let ctx = self.context.clone();
                let num_parts = self.rdd.number_of_splits();
                let cached = Arc::new(CachedRdd::new_with_level(
                    self.rdd.clone(),
                    StorageLevel::DiskOnly,
                ));
                // Eagerly write all partitions to disk (DiskOnly skips memory cache).
                for part_idx in 0..num_parts {
                    let splits = self.rdd.splits();
                    if let Ok(items) = self.rdd.compute(splits[part_idx].clone()) {
                        let data: Vec<T> = items.collect();
                        if let Some(path) = cached.spill_path(part_idx) {
                            let _ = disk_write_partition(&path, &data);
                        }
                    }
                }
                TypedRdd::new(cached as RddRef<T>, ctx)
            }

            other => self.persist(other),
        }
    }

    /// Remove all cached partitions for this RDD from the global `PartitionStore`.
    ///
    /// After `unpersist()` the next action on the returned RDD will recompute all
    /// partitions from scratch.  Mirrors a conventional `unpersist()` operation.
    pub fn unpersist(self) -> Self {
        if let Some(store) = atomic_data::cache::PARTITION_CACHE.get() {
            let rdd_id = self.rdd.get_rdd_id();
            let n = self.rdd.number_of_splits();
            store.remove_rdd(rdd_id, n);
        }
        self
    }

    /// Returns `true` if at least one partition of this RDD is currently held in
    /// the global `PartitionStore` (i.e., the RDD has been cached and not evicted).
    pub fn is_cached(&self) -> bool {
        atomic_data::cache::PARTITION_CACHE
            .get()
            .map(|store| {
                let rdd_id = self.rdd.get_rdd_id();
                let n = self.rdd.number_of_splits();
                (0..n).any(|p| store.contains(rdd_id, p))
            })
            .unwrap_or(false)
    }

    /// Materialise this RDD, write each partition to `dir` (local path or `s3://`),
    /// and return a new `TypedRdd` backed by a `CheckpointRdd` — fully truncating
    /// the upstream lineage.
    ///
    /// Partitions are written to `{dir}/{rdd_id}/{partition}.bin` (bincode-encoded).
    ///
    /// Requires `T: bincode::Encode + bincode::Decode<()>`.
    pub fn checkpoint(self, dir: impl AsRef<str>) -> Result<TypedRdd<T>, BaseError>
    where
        T: bincode::Encode + bincode::Decode<()>,
    {
        use crate::rdd::checkpoint::{CheckpointRdd, CheckpointStore};
        use atomic_data::cache::disk_write_partition;

        let store = CheckpointStore::from_uri(dir.as_ref());
        let ctx = self.context.clone();
        let rdd_id = self.rdd.get_rdd_id();
        let partitions = self.collect_partitions()?;
        let num_partitions = partitions.len();

        for (idx, data) in partitions.iter().enumerate() {
            match &store {
                CheckpointStore::Local(base) => {
                    let path = base.join(format!("{rdd_id}")).join(format!("{idx}.bin"));
                    disk_write_partition(&path, data)
                        .map_err(|e| BaseError::Other(format!("checkpoint write failed: {e}")))?;
                }

                #[cfg(feature = "s3")]
                CheckpointStore::S3 { bucket, prefix } => {
                    use crate::io::s3::s3_impl::write_text;
                    let bytes = bincode::encode_to_vec(data, bincode::config::standard())
                        .map_err(|e| BaseError::Other(format!("checkpoint encode: {e}")))?;
                    let b64 =
                        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
                    let key = format!("{prefix}/{rdd_id}/{idx}.bin");
                    write_text(bucket, &key, b64).map_err(|e| BaseError::Other(e))?;
                }
            }
        }

        let checkpoint_rdd = Arc::new(CheckpointRdd::new(ctx.new_rdd_id(), store, num_partitions));
        Ok(TypedRdd::new(checkpoint_rdd as RddRef<T>, ctx))
    }
}
