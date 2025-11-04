pub mod cache;
pub mod config;
pub mod error;
pub mod fetcher;
pub mod manager;
pub mod map_task;
pub mod tracker;

use http_body_util::Full;
use hyper::body::Bytes;

pub type Body = Full<Bytes>;

// Re-export commonly used types
pub use cache::ShuffleCache;
pub use config::ShuffleConfig;
pub use manager::ShuffleManager;
pub use tracker::MapOutputTracker;
