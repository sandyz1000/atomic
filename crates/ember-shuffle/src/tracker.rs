use std::fmt::Debug;
use std::future::Future;

pub type PinFuture<'a, T> = std::pin::Pin<
    Box<dyn Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>,
>;

/// Trait for map output tracking
///
/// This abstracts the mechanism for tracking where shuffle outputs are located,
/// allowing the fetcher to find and retrieve shuffle data from various sources.
pub trait MapOutputTracker: Send + Sync + Debug {
    /// Get server URIs for all mappers of a given shuffle operation
    /// Returns a vector where index i contains the URI for mapper i
    fn get_server_uris(&self, shuffle_id: usize) -> PinFuture<'_, Vec<Option<String>>>;
}
