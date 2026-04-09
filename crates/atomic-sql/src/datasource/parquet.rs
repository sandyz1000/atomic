// Parquet data source support is delegated entirely to DataFusion.
//
// Use `AtomicSqlContext::register_parquet` which calls DataFusion's built-in
// `SessionContext::register_parquet` internally.
//
// Re-export common Parquet types so callers don't need a direct `datafusion`
// dependency for typical use-cases.

pub use datafusion::prelude::ParquetReadOptions;
pub use datafusion::parquet::file::properties::WriterProperties;
