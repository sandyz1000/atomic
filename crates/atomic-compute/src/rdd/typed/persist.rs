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
    pub fn persist(mut self, level: StorageLevel) -> Self {
        let ctx = self.context.clone();
        let distributed = ctx.is_distributed();
        // Capture the staged pipeline before consuming `self` so distributed caching
        // can append a terminal `Cache` op to it (local mode keeps using CachedRdd).
        let staged = self.staged.take();
        let cached = Arc::new(CachedRdd::new_with_level(self.into_rdd(), level));
        let cache_id = cached.rdd_id();
        let mut out = TypedRdd::new(cached as RddRef<T>, ctx);
        if distributed && let Some(mut sp) = staged {
            // The first action over this RDD runs the pipeline and caches each
            // partition's bytes on its worker under `cache_id` (see runtimes/native.rs).
            sp.steps.push(Step {
                task_name: String::new(),
                kind: StepKind::Engine(EngineStep::Cache { rdd_id: cache_id }),
                runtime: TaskRuntime::Native,
                payload: vec![],
            });
            out.staged = Some(sp);
        }
        out
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
                // A failure here leaves the partition unspilled — it will be recomputed
                // on read rather than served from disk, so warn instead of failing the
                // builder (persist returns `Self`, not `Result`).
                for part_idx in 0..num_parts {
                    let splits = self.rdd.splits();
                    match self.rdd.compute(splits[part_idx].clone()) {
                        Ok(items) => {
                            let data: Vec<T> = items.collect();
                            if let Some(path) = cached.spill_path(part_idx)
                                && let Err(e) = disk_write_partition(&path, &data)
                            {
                                log::warn!(
                                    "persist(DiskOnly): failed to spill partition {part_idx} \
                                     to {path:?}: {e}"
                                );
                            }
                        }
                        Err(e) => log::warn!(
                            "persist(DiskOnly): failed to compute partition {part_idx}: {e}"
                        ),
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
        let rdd_id = self.rdd.get_rdd_id();
        if let Some(store) = atomic_data::cache::PARTITION_CACHE.get() {
            let n = self.rdd.number_of_splits();
            store.remove_rdd(rdd_id, n);
        }
        // Distributed: drop cache locations so future jobs recompute (worker byte
        // caches reclaim memory via LRU).
        self.context.invalidate_distributed_cache(rdd_id);
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
    pub fn checkpoint(self, dir: impl AsRef<str>) -> Result<TypedRdd<T>, DataError>
    where
        T: bincode::Encode + bincode::Decode<()> + WireEncode + WireDecode,
        Vec<T>: WireEncode + WireDecode,
    {
        use crate::rdd::checkpoint::{CheckpointRdd, CheckpointStore};
        use atomic_data::cache::disk_write_partition;

        let store = CheckpointStore::from_uri(dir.as_ref());
        let ctx = self.context.clone();
        // Allocate the CheckpointRdd's id up front and key the written partitions
        // by it — `CheckpointRdd::compute` reads back under this same id. (Using
        // the source RDD's id here would write to a path the reader never looks
        // at, so every read-back would miss its file.)
        let checkpoint_id = ctx.new_rdd_id();
        let partitions = self.collect_partitions()?;
        let num_partitions = partitions.len();

        for (idx, data) in partitions.iter().enumerate() {
            match &store {
                CheckpointStore::Local(base) => {
                    let path = base
                        .join(format!("{checkpoint_id}"))
                        .join(format!("{idx}.bin"));
                    disk_write_partition(&path, data)
                        .map_err(|e| DataError::Other(format!("checkpoint write failed: {e}")))?;
                }

                CheckpointStore::S3 { bucket, prefix } => {
                    use crate::io::s3::write_text;
                    let bytes = bincode::encode_to_vec(data, bincode::config::standard())
                        .map_err(|e| DataError::Other(format!("checkpoint encode: {e}")))?;
                    let b64 =
                        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
                    let key = format!("{prefix}/{checkpoint_id}/{idx}.bin");
                    write_text(bucket, &key, b64).map_err(|e| DataError::Other(e.to_string()))?;
                }
            }
        }

        let checkpoint_rdd = Arc::new(CheckpointRdd::new(checkpoint_id, store, num_partitions));
        Ok(TypedRdd::new(checkpoint_rdd as RddRef<T>, ctx))
    }
}
