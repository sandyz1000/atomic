/// Runtime configuration for atomic-sql queries.
#[derive(Debug, Clone)]
pub struct AtomicSqlConfig {
    /// Number of partitions used when reading data sources without explicit partitioning.
    pub default_parallelism: usize,

    /// Number of rows per Arrow RecordBatch during execution.
    pub batch_size: usize,

    /// Enable Parquet row-group pruning using column statistics.
    pub enable_parquet_pruning: bool,

    /// Enable predicate push-down into data source scans.
    pub enable_predicate_pushdown: bool,
}

impl Default for AtomicSqlConfig {
    fn default() -> Self {
        Self {
            default_parallelism: num_cpus(),
            batch_size: 8192,
            enable_parquet_pruning: true,
            enable_predicate_pushdown: true,
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}
