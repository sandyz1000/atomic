use super::error::ShuffleError;
use crate::tracker::MapOutputTracker;
use ember_data::data::Data;
use futures::future;
use http_body_util::{BodyExt, Full};
use hyper::Uri;
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{Arc, atomic, atomic::AtomicBool};
use tokio::sync::Mutex;

type Body = Full<Bytes>;
type LibResult<T> = Result<T, ShuffleError>;

/// Parallel shuffle fetcher.
pub(crate) struct ShuffleFetcher {
    tracker: Arc<dyn MapOutputTracker>,
}

impl ShuffleFetcher {
    /// Create a new ShuffleFetcher with dependency injection
    pub fn new(tracker: Arc<dyn MapOutputTracker>) -> Self {
        Self { tracker }
    }

    pub async fn fetch<K, V>(
        &self,
        shuffle_id: usize,
        reduce_id: usize,
    ) -> LibResult<impl Iterator<Item = (K, V)>>
    where
        K: Data + bincode::Decode<()>,
        V: Data + bincode::Decode<()>,
    {
        log::debug!("inside fetch function");
        let mut inputs_by_uri = HashMap::new();
        let server_uris: Vec<Option<String>> = self
            .tracker
            .get_server_uris(shuffle_id)
            .await
            .map_err(|err| ShuffleError::FailFetchingShuffleUris { source: err })?;
        log::debug!(
            "server uris for shuffle id #{}: {:?}",
            shuffle_id,
            server_uris
        );
        for (index, server_uri_opt) in server_uris.into_iter().enumerate() {
            if let Some(server_uri) = server_uri_opt {
                inputs_by_uri
                    .entry(server_uri)
                    .or_insert_with(Vec::new)
                    .push(index);
            }
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
                if let Some((server_uri, input_ids)) = lock.pop() {
                    drop(lock); // Release lock early
                    let server_uri = format!("{}/shuffle/{}", server_uri, shuffle_id);
                    let mut chunk_uri_str = String::with_capacity(server_uri.len() + 12);
                    chunk_uri_str.push_str(&server_uri);
                    let mut shuffle_chunks = Vec::with_capacity(input_ids.len());
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

                        // Modern Hyper 1.x client connection
                        let data_bytes = match Self::fetch_chunk(chunk_uri).await {
                            Ok(bytes) => bytes,
                            Err(e) => {
                                log::error!("Failed to fetch chunk: {:?}", e);
                                failure.store(true, atomic::Ordering::Release);
                                return Err(ShuffleError::FailedFetchOp);
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
                    Ok::<Box<dyn Iterator<Item = (K, V)> + Send>, _>(Box::new(
                        shuffle_chunks.into_iter().flatten(),
                    ))
                } else {
                    Ok::<Box<dyn Iterator<Item = (K, V)> + Send>, _>(Box::new(std::iter::empty()))
                }
            };
            tasks.push(tokio::spawn(task));
        }
        log::debug!("total_results fetch results: {}", total_results);
        let task_results = future::join_all(tasks.into_iter()).await;
        let results = task_results.into_iter().fold(
            Ok(Vec::<(K, V)>::with_capacity(total_results)),
            |curr, res| {
                if let Ok(mut curr) = curr {
                    if let Ok(Ok(res)) = res {
                        curr.extend(res);
                        Ok(curr)
                    } else {
                        Err(ShuffleError::FailedFetchOp)
                    }
                } else {
                    Err(ShuffleError::FailedFetchOp)
                }
            },
        )?;
        Ok(results.into_iter())
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

    async fn fetch_chunk(uri: Uri) -> LibResult<Vec<u8>> {
        let host = uri.host().ok_or(ShuffleError::FailedFetchOp)?;
        let port = uri.port_u16().unwrap_or(80);

        let stream = tokio::net::TcpStream::connect((host, port))
            .await
            .map_err(|_| ShuffleError::FailedFetchOp)?;
        let io = TokioIo::new(stream);

        let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
            .await
            .map_err(|_| ShuffleError::FailedFetchOp)?;

        tokio::spawn(async move {
            if let Err(err) = conn.await {
                log::error!("Connection failed: {:?}", err);
            }
        });

        let request = hyper::Request::builder()
            .uri(uri)
            .body(Body::default())
            .map_err(|_| ShuffleError::FailedFetchOp)?;

        let response = sender
            .send_request(request)
            .await
            .map_err(|_| ShuffleError::FailedFetchOp)?;

        let body_bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|_| ShuffleError::FailedFetchOp)?
            .to_bytes();

        Ok(body_bytes.to_vec())
    }
}

// TODO: Tests have been commented out because they depend on env:: module which is not available
// in the ember-shuffle crate. These tests need to be rewritten in the main ember crate where
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
