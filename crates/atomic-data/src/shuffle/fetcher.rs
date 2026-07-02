use super::error::ShuffleError;
use crate::data::Data;
use crate::shuffle::map_output::MapOutputTracker;
use futures::future;
use http_body_util::{BodyExt, Full};
use hyper::Uri;
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::sync::{Arc, atomic, atomic::AtomicBool};
use std::time::Duration;
use tokio::sync::Mutex;

/// Maximum number of per-chunk fetch attempts before giving up.
const MAX_FETCH_RETRIES: usize = 3;
/// Initial backoff before the first retry (doubles on each attempt).
const INITIAL_RETRY_MS: u64 = 100;
/// TCP connection timeout per attempt.
const CONNECT_TIMEOUT_SECS: u64 = 5;

type Body = Full<Bytes>;
type LibResult<T> = Result<T, ShuffleError>;

/// Parallel shuffle fetcher.
#[derive(Debug, Clone)]
pub struct ShuffleFetcher {
    tracker: Arc<MapOutputTracker>,
}

impl ShuffleFetcher {
    /// Create a new ShuffleFetcher with dependency injection
    pub fn new(tracker: Arc<MapOutputTracker>) -> Self {
        Self { tracker }
    }

    /// Fetch each map task's output for `reduce_id` as a separate run (not flattened).
    /// Sort-shuffle reduce uses this to k-way merge sorted runs; `fetch` flattens it.
    pub async fn fetch_runs<K, V>(
        &self,
        shuffle_id: usize,
        reduce_id: usize,
    ) -> LibResult<Vec<Vec<(K, V)>>>
    where
        K: Data + bincode::Decode<()>,
        V: Data + bincode::Decode<()>,
    {
        log::debug!("inside fetch_runs function");
        let mut inputs_by_uri = HashMap::new();
        let server_uris: Vec<String> =
            self.tracker
                .get_server_uris(shuffle_id)
                .await
                .map_err(|err| ShuffleError::FailFetchingShuffleUris {
                    source: Box::new(err),
                })?;
        log::debug!(
            "server uris for shuffle id #{}: {:?}",
            shuffle_id,
            server_uris
        );
        for (index, server_uri) in server_uris.into_iter().enumerate() {
            inputs_by_uri
                .entry(server_uri)
                .or_insert_with(Vec::new)
                .push(index);
        }
        let mut server_queue = Vec::new();
        let mut total_results = 0;
        for (key, value) in inputs_by_uri {
            total_results += value.len();
            server_queue.push((key, value));
        }
        log::debug!(
            "servers for shuffle id #{:?} & reduce id #{}: {:?}",
            shuffle_id,
            reduce_id,
            server_queue
        );
        let num_tasks = server_queue.len();
        let server_queue = Arc::new(Mutex::new(server_queue));
        let failure = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::with_capacity(num_tasks);
        for _ in 0..num_tasks {
            let server_queue = server_queue.clone();
            let failure = failure.clone();
            // spawn a future for each expected result set
            let task = async move {
                let mut lock = server_queue.lock().await;
                if let Some((base_server_uri, input_ids)) = lock.pop() {
                    drop(lock); // Release lock early
                    let server_uri = format!("{}/shuffle/{}", base_server_uri, shuffle_id);
                    let mut chunk_uri_str = String::with_capacity(server_uri.len() + 12);
                    chunk_uri_str.push_str(&server_uri);
                    let mut shuffle_chunks = Vec::with_capacity(input_ids.len());
                    // Reused across every chunk from this server instead of dialing fresh
                    // per chunk (that pattern exhausted local ephemeral ports under load).
                    let mut conn = None;
                    for input_id in input_ids {
                        if failure.load(atomic::Ordering::Acquire) {
                            // Abort early since the work failed in an other future
                            return Err(ShuffleError::Other);
                        }
                        log::debug!("inside parallel fetch {}", input_id);
                        let chunk_uri = Self::make_chunk_uri(
                            &server_uri,
                            &mut chunk_uri_str,
                            input_id,
                            reduce_id,
                        )?;

                        // Fetch with retry + exponential backoff.
                        let data_bytes =
                            match Self::fetch_chunk_with_retry(&mut conn, chunk_uri).await {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    log::error!("Failed to fetch chunk: {:?}", e);
                                    failure.store(true, atomic::Ordering::Release);
                                    // Host unreachable after retries → the map output is
                                    // lost; surface its identity so the scheduler recomputes it.
                                    return Err(ShuffleError::FetchFailed {
                                        shuffle_id,
                                        map_id: input_id,
                                        server_uri: base_server_uri.clone(),
                                    });
                                }
                            };

                        // bincode 2.0 API
                        let config = bincode::config::standard();
                        match bincode::decode_from_slice::<Vec<(K, V)>, _>(&data_bytes, config) {
                            Ok((deser_data, _)) => {
                                shuffle_chunks.push(deser_data);
                            }
                            Err(e) => {
                                log::error!("Failed to deserialize data: {:?}", e);
                                failure.store(true, atomic::Ordering::Release);
                                return Err(ShuffleError::FailedFetchOp);
                            }
                        }
                    }
                    Ok::<Vec<Vec<(K, V)>>, _>(shuffle_chunks)
                } else {
                    Ok::<Vec<Vec<(K, V)>>, _>(Vec::new())
                }
            };
            tasks.push(tokio::spawn(task));
        }
        log::debug!("total_results fetch results: {}", total_results);
        let task_results = future::join_all(tasks).await;
        let mut runs: Vec<Vec<(K, V)>> = Vec::with_capacity(total_results);
        for res in task_results {
            match res {
                Ok(Ok(chunks)) => runs.extend(chunks),
                _ => return Err(ShuffleError::FailedFetchOp),
            }
        }
        Ok(runs)
    }

    /// Like [`fetch_runs`](Self::fetch_runs), but each fetched run is streamed to
    /// an anonymous temp file and returned as a lazy [`SpilledRunIter`] that
    /// decodes `(K, V)` pairs on demand. The reduce side uses this for wide
    /// shuffles so the full set of input runs is never resident at once — peak
    /// memory is one run during its fetch plus the k-way-merge working set (one
    /// element per run) instead of every run decoded into a `Vec`.
    pub async fn fetch_runs_spilled<K, V>(
        &self,
        shuffle_id: usize,
        reduce_id: usize,
    ) -> LibResult<Vec<SpilledRunIter<K, V>>>
    where
        K: Data + bincode::Decode<()>,
        V: Data + bincode::Decode<()>,
    {
        let server_uris: Vec<String> =
            self.tracker
                .get_server_uris(shuffle_id)
                .await
                .map_err(|err| ShuffleError::FailFetchingShuffleUris {
                    source: Box::new(err),
                })?;
        let mut inputs_by_uri = HashMap::new();
        for (index, server_uri) in server_uris.into_iter().enumerate() {
            inputs_by_uri
                .entry(server_uri)
                .or_insert_with(Vec::new)
                .push(index);
        }
        let mut server_queue = Vec::new();
        for (key, value) in inputs_by_uri {
            server_queue.push((key, value));
        }
        let num_tasks = server_queue.len();
        let server_queue = Arc::new(Mutex::new(server_queue));
        let failure = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::with_capacity(num_tasks);
        for _ in 0..num_tasks {
            let server_queue = server_queue.clone();
            let failure = failure.clone();
            let task = async move {
                let mut lock = server_queue.lock().await;
                if let Some((base_server_uri, input_ids)) = lock.pop() {
                    drop(lock);
                    let server_uri = format!("{}/shuffle/{}", base_server_uri, shuffle_id);
                    let mut chunk_uri_str = String::with_capacity(server_uri.len() + 12);
                    chunk_uri_str.push_str(&server_uri);
                    let mut runs = Vec::with_capacity(input_ids.len());
                    let mut conn = None;
                    for input_id in input_ids {
                        if failure.load(atomic::Ordering::Acquire) {
                            return Err(ShuffleError::Other);
                        }
                        let chunk_uri = Self::make_chunk_uri(
                            &server_uri,
                            &mut chunk_uri_str,
                            input_id,
                            reduce_id,
                        )?;
                        let data_bytes =
                            match Self::fetch_chunk_with_retry(&mut conn, chunk_uri).await {
                                Ok(bytes) => bytes,
                                Err(e) => {
                                    log::error!("Failed to fetch chunk: {:?}", e);
                                    failure.store(true, atomic::Ordering::Release);
                                    // Same lineage-recovery contract as `fetch_runs`:
                                    // surface the lost map output so the scheduler
                                    // recomputes it.
                                    return Err(ShuffleError::FetchFailed {
                                        shuffle_id,
                                        map_id: input_id,
                                        server_uri: base_server_uri.clone(),
                                    });
                                }
                            };
                        match SpilledRunIter::<K, V>::spill(&data_bytes) {
                            Ok(run) => runs.push(run),
                            Err(e) => {
                                failure.store(true, atomic::Ordering::Release);
                                return Err(e);
                            }
                        }
                    }
                    Ok::<Vec<SpilledRunIter<K, V>>, _>(runs)
                } else {
                    Ok::<Vec<SpilledRunIter<K, V>>, _>(Vec::new())
                }
            };
            tasks.push(tokio::spawn(task));
        }
        let task_results = future::join_all(tasks).await;
        let mut runs: Vec<SpilledRunIter<K, V>> = Vec::new();
        for res in task_results {
            match res {
                Ok(Ok(chunk_runs)) => runs.extend(chunk_runs),
                _ => return Err(ShuffleError::FailedFetchOp),
            }
        }
        Ok(runs)
    }

    /// Fetch and flatten all map outputs for `reduce_id` into a single iterator.
    pub async fn fetch<K, V>(
        &self,
        shuffle_id: usize,
        reduce_id: usize,
    ) -> LibResult<impl Iterator<Item = (K, V)>>
    where
        K: Data + bincode::Decode<()>,
        V: Data + bincode::Decode<()>,
    {
        Ok(self
            .fetch_runs::<K, V>(shuffle_id, reduce_id)
            .await?
            .into_iter()
            .flatten())
    }

    fn make_chunk_uri(
        base: &str,
        chunk: &mut String,
        input_id: usize,
        reduce_id: usize,
    ) -> LibResult<Uri> {
        let input_id = input_id.to_string();
        let reduce_id = reduce_id.to_string();
        let path_tail = ["/".to_string(), input_id, "/".to_string(), reduce_id].concat();
        if chunk.len() == base.len() {
            chunk.push_str(&path_tail);
        } else {
            chunk.replace_range(base.len().., &path_tail);
        }
        Ok(Uri::try_from(chunk.as_str())?)
    }

    /// Retry wrapper. Reuses `*sender` (a keep-alive connection) across calls instead
    /// of dialing fresh per chunk; `None` (first call, or after a failed send) reconnects.
    async fn fetch_chunk_with_retry(
        sender: &mut Option<hyper::client::conn::http1::SendRequest<Body>>,
        uri: Uri,
    ) -> LibResult<Vec<u8>> {
        let mut delay_ms = INITIAL_RETRY_MS;
        for attempt in 0..MAX_FETCH_RETRIES {
            let result = match sender {
                Some(s) => Self::send_on(s, uri.clone()).await,
                None => match Self::open_connection(&uri).await {
                    Ok(s) => {
                        *sender = Some(s);
                        Self::send_on(sender.as_mut().unwrap(), uri.clone()).await
                    }
                    Err(e) => Err(e),
                },
            };
            match result {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    *sender = None; // drop a bad connection so the next attempt reconnects
                    if attempt + 1 < MAX_FETCH_RETRIES {
                        log::warn!(
                            "shuffle fetch attempt {}/{} failed: {:?}; retrying in {}ms",
                            attempt + 1,
                            MAX_FETCH_RETRIES,
                            e,
                            delay_ms,
                        );
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        delay_ms = delay_ms.saturating_mul(2);
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        unreachable!()
    }

    /// Dial `uri`'s host and complete the HTTP/1.1 handshake, returning a sender that
    /// can issue multiple requests over the one connection (keep-alive).
    async fn open_connection(
        uri: &Uri,
    ) -> LibResult<hyper::client::conn::http1::SendRequest<Body>> {
        let host = uri.host().ok_or(ShuffleError::FailedFetchOp)?;
        let is_https = uri.scheme_str() == Some("https");
        let port = uri.port_u16().unwrap_or(if is_https { 443 } else { 80 });

        let stream = tokio::time::timeout(
            Duration::from_secs(CONNECT_TIMEOUT_SECS),
            tokio::net::TcpStream::connect((host, port)),
        )
        .await
        .map_err(|e| {
            log::error!("connect timeout to {host}:{port}: {e}");
            ShuffleError::FailedFetchOp
        })?
        .map_err(|e| {
            log::error!("connect error to {host}:{port}: {e} (kind={:?})", e.kind());
            ShuffleError::FailedFetchOp
        })?;

        // When TLS is compiled in and the URI scheme is https, use the global
        // TLS connector (set by init_shuffle when TLS is configured).
        let stream = match Self::try_tls_open(is_https, host, stream).await {
            Ok(result) => return result,
            Err(stream) => stream,
        };
        Self::handshake(TokioIo::new(stream)).await
    }

    crate::cfg_tls! {
    /// Attempt the handshake over TLS when `is_https` and a TLS connector is configured.
    /// Returns `Err(stream)` (still holding the connection) so the caller falls
    /// through to plain HTTP; `Ok(result)` if the TLS path was taken.
    async fn try_tls_open(
        is_https: bool,
        host: &str,
        stream: tokio::net::TcpStream,
    ) -> Result<LibResult<hyper::client::conn::http1::SendRequest<Body>>, tokio::net::TcpStream> {
        let Some(connector) = is_https.then(crate::env::get_shuffle_tls_connector).flatten()
        else {
            return Err(stream);
        };
        let domain = match rustls::pki_types::ServerName::try_from(host.to_owned()) {
            Ok(d) => d,
            Err(_) => return Ok(Err(ShuffleError::FailedFetchOp)),
        };
        let tls_stream = match connector.connect(domain, stream).await {
            Ok(s) => s,
            Err(_) => return Ok(Err(ShuffleError::FailedFetchOp)),
        };
        Ok(Self::handshake(TokioIo::new(tls_stream)).await)
    }
    }
    crate::cfg_not_tls! {
    async fn try_tls_open(
        _is_https: bool,
        _host: &str,
        stream: tokio::net::TcpStream,
    ) -> Result<LibResult<hyper::client::conn::http1::SendRequest<Body>>, tokio::net::TcpStream> {
        Err(stream)
    }
    }

    async fn handshake<IO>(
        io: TokioIo<IO>,
    ) -> LibResult<hyper::client::conn::http1::SendRequest<Body>>
    where
        IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (sender, conn) = hyper::client::conn::http1::handshake(io)
            .await
            .map_err(|e| {
                log::error!("http1 handshake failed: {e}");
                ShuffleError::FailedFetchOp
            })?;

        tokio::spawn(async move {
            if let Err(err) = conn.await {
                log::error!("Connection failed: {:?}", err);
            }
        });

        Ok(sender)
    }

    /// Issue one request over an already-open keep-alive connection.
    async fn send_on(
        sender: &mut hyper::client::conn::http1::SendRequest<Body>,
        uri: Uri,
    ) -> LibResult<Vec<u8>> {
        let request = hyper::Request::builder()
            .uri(uri)
            .body(Body::default())
            .map_err(|e| {
                log::error!("request build failed: {e}");
                ShuffleError::FailedFetchOp
            })?;

        let response = sender.send_request(request).await.map_err(|e| {
            log::error!("send_request failed: {e}");
            ShuffleError::FailedFetchOp
        })?;

        let body_bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|e| {
                log::error!("response body collect failed: {e}");
                ShuffleError::FailedFetchOp
            })?
            .to_bytes();

        Ok(body_bytes.to_vec())
    }
}

/// A lazily-decoded shuffle run backed by an anonymous temp file. Holds the file
/// open (so it is auto-removed when the iterator drops) and decodes one `(K, V)`
/// pair per `next()`, so the run's elements are never all resident at once.
pub struct SpilledRunIter<K, V> {
    reader: BufReader<std::fs::File>,
    remaining: u64,
    _pd: PhantomData<(K, V)>,
}

impl<K, V> SpilledRunIter<K, V> {
    /// Spill a bincode-encoded `Vec<(K, V)>` to an anonymous temp file and return
    /// an iterator positioned just after the length prefix.
    fn spill(bytes: &[u8]) -> LibResult<Self> {
        let mut file = tempfile::tempfile().map_err(|_| ShuffleError::FailedFetchOp)?;
        file.write_all(bytes)
            .map_err(|_| ShuffleError::FailedFetchOp)?;
        file.seek(SeekFrom::Start(0))
            .map_err(|_| ShuffleError::FailedFetchOp)?;
        let mut reader = BufReader::new(file);
        let config = bincode::config::standard();
        // A `Vec`'s length prefix uses the same `u64` varint encoding a standalone
        // `u64` decodes with, so this consumes exactly the header the map side
        // wrote and leaves the reader at the first element.
        let remaining: u64 = bincode::decode_from_std_read(&mut reader, config)
            .map_err(|_| ShuffleError::FailedFetchOp)?;
        Ok(SpilledRunIter {
            reader,
            remaining,
            _pd: PhantomData,
        })
    }
}

impl<K, V> Iterator for SpilledRunIter<K, V>
where
    K: Data + bincode::Decode<()>,
    V: Data + bincode::Decode<()>,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let config = bincode::config::standard();
        match bincode::decode_from_std_read::<(K, V), _, _>(&mut self.reader, config) {
            Ok(pair) => {
                self.remaining -= 1;
                Some(pair)
            }
            Err(_) => {
                self.remaining = 0;
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spilled_run_roundtrips() {
        let data: Vec<(i32, String)> = vec![(1, "a".into()), (2, "bb".into()), (3, "ccc".into())];
        let bytes = bincode::encode_to_vec(&data, bincode::config::standard()).unwrap();
        let got: Vec<(i32, String)> = SpilledRunIter::<i32, String>::spill(&bytes)
            .unwrap()
            .collect();
        assert_eq!(got, data);
    }

    #[test]
    fn spilled_run_empty() {
        let data: Vec<(i32, String)> = vec![];
        let bytes = bincode::encode_to_vec(&data, bincode::config::standard()).unwrap();
        let n = SpilledRunIter::<i32, String>::spill(&bytes)
            .unwrap()
            .count();
        assert_eq!(n, 0);
    }
}

// TODO: Tests have been commented out because they depend on env:: module which is not available
// in the atomic-shuffle crate. These tests need to be rewritten in the main atomic crate where
// env:: is available, creating concrete implementations of MapOutputTracker and passing them to
// ShuffleFetcher::new().
//
// #[cfg(test)]
// mod tests {
//
//     use super::*;
//
//     #[tokio::test(flavor = "multi_thread")]
//     async fn fetch_ok() -> Result<(), Box<dyn std::error::Error + 'static>> {
//         {
//             let addr = format!(
//                 "http://127.0.0.1:{}",
//                 env::Env::get().shuffle_manager.server_port
//             );
//             let servers = &env::Env::get().map_output_tracker.server_uris;
//             servers.insert(11000, vec![Some(addr)]);
//
//             let data = vec![(0i32, "example data".to_string())];
//             let config = bincode::config::standard();
//             let serialized_data = bincode::encode_to_vec(&data, config).unwrap();
//             env::SHUFFLE_CACHE.insert((11000, 0, 11001), serialized_data);
//         }
//
//         let result: Vec<(i32, String)> = ShuffleFetcher::fetch(11000, 11001)
//             .await?
//             .into_iter()
//             .collect();
//         assert_eq!(result[0].0, 0);
//         assert_eq!(result[0].1, "example data");
//
//         Ok(())
//     }
//
//     #[tokio::test(flavor = "multi_thread")]
//     async fn fetch_failure() -> Result<(), Box<dyn std::error::Error + 'static>> {
//         {
//             let addr = format!(
//                 "http://127.0.0.1:{}",
//                 env::Env::get().shuffle_manager.server_port
//             );
//             let servers = &env::Env::get().map_output_tracker.server_uris;
//             servers.insert(10000, vec![Some(addr)]);
//
//             let data = "corrupted data";
//             let config = bincode::config::standard();
//             let serialized_data = bincode::encode_to_vec(&data, config).unwrap();
//             env::SHUFFLE_CACHE.insert((10000, 0, 10001), serialized_data);
//         }
//
//         let err = ShuffleFetcher::fetch::<i32, String>(10000, 10001).await;
//         assert!(err.is_err());
//
//         Ok(())
//     }
//
//     #[test]
//     fn build_shuffle_id_uri() -> Result<(), Box<dyn std::error::Error + 'static>> {
//         let base = "http://127.0.0.1/shuffle";
//         let mut chunk = base.to_owned();
//
//         let uri0 = ShuffleFetcher::make_chunk_uri(base, &mut chunk, 0, 1)?;
//         let expected = format!("{}/0/1", base);
//         assert_eq!(expected.as_str(), uri0);
//
//         let uri1 = ShuffleFetcher::make_chunk_uri(base, &mut chunk, 123, 123)?;
//         let expected = format!("{}/123/123", base);
//         assert_eq!(expected.as_str(), uri1);
//
//         Ok(())
//     }
// }
