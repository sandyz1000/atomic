// CSV data source support is delegated entirely to DataFusion.
//
// Use `AtomicSqlContext::register_csv` which calls DataFusion's built-in
// `SessionContext::register_csv` internally.
//
// The types below re-export DataFusion's CSV read options so callers don't
// need a direct dependency on the `datafusion` crate for common CSV settings.

pub use datafusion::prelude::CsvReadOptions;
