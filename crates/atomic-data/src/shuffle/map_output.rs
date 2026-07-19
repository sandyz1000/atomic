use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use crate::shuffle::error::NetworkError;
use crate::shuffle::map_output_server::{MapOutputServerHandle, spawn_shuffling_server};
use dashmap::{DashMap, DashSet};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use thiserror::Error;
use tokio::net::TcpStream;

type Result<T> = std::result::Result<T, MapOutputError>;

// const CAPNP_BUF_READ_OPTS: ReaderOptions = ReaderOptions {
//     traversal_limit_in_words: std::u64::MAX,
//     nesting_limit: 64,
// };

pub enum MapOutputTrackerMessage {
    // Contains shuffle_id
    GetMapOutputLocations(i64),
    StopMapOutputTracker,
}

/// The key is the shuffle_id
pub type ServerUris = Arc<DashMap<usize, Vec<Option<String>>>>;

/// Trait for map output tracking
///
/// This abstracts the mechanism for tracking where shuffle outputs are located,
/// allowing the fetcher to find and retrieve shuffle data from various sources.
// Starts the server in master node and client in slave nodes. Similar to cache tracker.
#[derive(Clone, Debug)]
pub struct MapOutputTracker {
    is_master: bool,
    pub map_output_uris: ServerUris,
    fetching: Arc<DashSet<usize>>,
    generation: Arc<Mutex<i64>>,
    master_addr: SocketAddr,
    /// Adaptive coalescing result: shuffle_id → coalesced reduce partition count.
    /// Set by the scheduler after the map stage completes (if coalescing is configured).
    /// Queried by `ShuffledRdd::number_of_splits()` and `ShuffleFetcher::fetch`.
    pub coalesced_parts: Arc<DashMap<usize, usize>>,
    server_handle: Arc<Mutex<Option<MapOutputServerHandle>>>,
}

// Only master_addr doesn't have a default.
impl Default for MapOutputTracker {
    fn default() -> Self {
        MapOutputTracker {
            is_master: Default::default(),
            map_output_uris: Default::default(),
            fetching: Default::default(),
            generation: Default::default(),
            master_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
            coalesced_parts: Arc::new(DashMap::new()),
            server_handle: Arc::new(Mutex::new(None)),
        }
    }
}

impl MapOutputTracker {
    pub fn new(is_master: bool, master_addr: SocketAddr) -> Self {
        let output_tracker = MapOutputTracker {
            is_master,
            map_output_uris: Arc::new(DashMap::new()),
            fetching: Arc::new(DashSet::new()),
            generation: Arc::new(Mutex::new(0)),
            master_addr,
            coalesced_parts: Arc::new(DashMap::new()),
            server_handle: Arc::new(Mutex::new(None)),
        };
        output_tracker.server();
        output_tracker
    }

    /// Record the coalesced reduce partition count for a shuffle stage.
    /// Called by the scheduler after all shuffle-map tasks complete.
    pub fn set_coalesced_partitions(&self, shuffle_id: usize, n: usize) {
        self.coalesced_parts.insert(shuffle_id, n);
    }

    /// Return the coalesced reduce partition count, if adaptive coalescing ran for this shuffle.
    pub fn get_coalesced_partitions(&self, shuffle_id: usize) -> Option<usize> {
        self.coalesced_parts.get(&shuffle_id).map(|v| *v)
    }

    async fn client(&self, shuffle_id: usize) -> Result<Vec<String>> {
        log::debug!(
            "connecting to master to fetch shuffle task #{} data hosts",
            shuffle_id
        );

        // Serialize the shuffle_id request
        let shuffle_id_bytes = bincode::encode_to_vec(shuffle_id, bincode::config::standard())
            .map_err(NetworkError::from)?;

        // Connect to master with retry
        let stream = loop {
            match TcpStream::connect(self.master_addr).await {
                Ok(s) => break s,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        };

        let io = TokioIo::new(stream);

        // Handshake and establish HTTP connection
        let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
            .await
            .map_err(NetworkError::from)?;

        // Spawn connection task
        tokio::spawn(async move {
            if let Err(err) = conn.await {
                log::error!("Connection failed: {:?}", err);
            }
        });

        // Build HTTP request
        let uri: hyper::Uri = format!("http://{}/shuffle", self.master_addr)
            .parse()
            .map_err(NetworkError::from)?;

        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(uri)
            .header("content-type", "application/octet-stream")
            .body(Full::new(Bytes::from(shuffle_id_bytes)))
            .map_err(NetworkError::from)?;

        // Send request
        let resp = sender.send_request(req).await.map_err(NetworkError::from)?;

        // Check status
        if resp.status() != hyper::StatusCode::OK {
            return Err(MapOutputError::NetworkError(NetworkError::HttpError(
                format!("Server returned error: {}", resp.status()),
            )));
        }

        // Read response body
        let body_bytes = resp
            .into_body()
            .collect()
            .await
            .map_err(NetworkError::from)?
            .to_bytes();

        // Deserialize the Vec<String> response
        let (locs, _): (Vec<String>, _) =
            bincode::decode_from_slice(&body_bytes, bincode::config::standard())
                .map_err(NetworkError::from)?;

        log::debug!(
            "received {} locations for shuffle task #{}",
            locs.len(),
            shuffle_id
        );
        Ok(locs)
    }

    fn server(&self) {
        if !self.is_master {
            return;
        }
        log::debug!("map output tracker server starting");
        let handle = spawn_shuffling_server(self.master_addr, self.map_output_uris.clone());
        *self.server_handle.lock() = Some(handle);
    }

    pub async fn shutdown_server(&self) {
        let handle = self.server_handle.lock().take();
        if let Some(handle) = handle {
            handle.shutdown().await;
        }
    }

    pub fn register_shuffle(&self, shuffle_id: usize, num_maps: usize) {
        log::debug!("inside register shuffle");
        if self.map_output_uris.get(&shuffle_id).is_some() {
            // TODO: error handling
            log::debug!("map tracker register shuffle none");
            return;
        }
        self.map_output_uris
            .insert(shuffle_id, vec![None; num_maps]);
        log::debug!(
            "map_output_uris after register_shuffle {:?}",
            self.map_output_uris
        );
    }

    pub fn register_map_output(
        &self,
        shuffle_id: usize,
        map_id: usize,
        server_uri: String,
    ) -> Result<()> {
        log::debug!(
            "registering map output from shuffle task #{} with map id #{} at server: {}",
            shuffle_id,
            map_id,
            server_uri
        );
        let mut entry = self
            .map_output_uris
            .get_mut(&shuffle_id)
            .ok_or(MapOutputError::ShuffleIdNotFound(shuffle_id))?;
        let num_maps = entry.len();
        let slot = entry
            .get_mut(map_id)
            .ok_or(MapOutputError::MapIdOutOfBounds {
                shuffle_id,
                map_id,
                num_maps,
            })?;
        *slot = Some(server_uri);
        Ok(())
    }

    pub fn register_map_outputs(&self, shuffle_id: usize, locs: Vec<Option<String>>) {
        log::debug!(
            "registering map outputs inside map output tracker for shuffle id #{}: {:?}",
            shuffle_id,
            locs
        );
        self.map_output_uris.insert(shuffle_id, locs);
    }

    /// Remove all map output URIs for `shuffle_id`.
    ///
    /// Called when a shuffle-map stage fails fatally so the scheduler can re-run
    /// the full map stage and re-register fresh URIs.
    pub fn unregister_shuffle(&self, shuffle_id: usize) {
        self.map_output_uris.remove(&shuffle_id);
        self.increment_generation();
        log::debug!("MapOutputTracker: unregistered shuffle_id={}", shuffle_id);
    }

    pub fn unregister_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String) {
        if let Some(arr) = self.map_output_uris.get(&shuffle_id) {
            // Bounds-safe: out-of-range map_id is treated as a no-op.
            let should_clear = arr
                .get(map_id)
                .is_some_and(|slot| *slot == Some(server_uri));
            drop(arr); // release read guard before acquiring write guard
            if should_clear
                && let Some(mut entry) = self.map_output_uris.get_mut(&shuffle_id)
                && let Some(slot) = entry.get_mut(map_id)
            {
                *slot = None;
            }
            self.increment_generation();
        }
    }

    /// Invalidate every map output served by `host` (an IP string) across all
    /// shuffles — used when a worker dies so its lost outputs are recomputed.
    /// Matches by host substring because the registered shuffle URI uses the
    /// worker's shuffle-server port, not its task port. Returns the number of
    /// outputs cleared.
    pub fn unregister_outputs_on_host(&self, host: &str) -> usize {
        let mut cleared = 0;
        for mut entry in self.map_output_uris.iter_mut() {
            for slot in entry.value_mut().iter_mut() {
                if slot.as_deref().is_some_and(|uri| uri.contains(host)) {
                    *slot = None;
                    cleared += 1;
                }
            }
        }
        if cleared > 0 {
            self.increment_generation();
        }
        cleared
    }

    pub async fn get_server_uris(&self, shuffle_id: usize) -> Result<Vec<String>> {
        log::debug!(
            "trying to get uri for shuffle task #{}, current server uris: {:?}",
            shuffle_id,
            self.map_output_uris
        );

        if self
            .map_output_uris
            .get(&shuffle_id)
            .and_then(|some| some.iter().find_map(|x| x.clone()))
            .is_none()
        {
            if self.fetching.contains(&shuffle_id) {
                while self.fetching.contains(&shuffle_id) {
                    // TODO: check whether this will hurt the performance or not
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                let servers = self
                    .map_output_uris
                    .get(&shuffle_id)
                    .ok_or_else(|| MapOutputError::ShuffleIdNotFound(shuffle_id))?
                    .iter()
                    .filter_map(|x| x.clone())
                    .collect::<Vec<_>>();
                log::debug!("returning after fetching done, return: {:?}", servers);
                return Ok(servers);
            } else {
                log::debug!("adding to fetching queue");
                self.fetching.insert(shuffle_id);
            }
            let fetched = self.client(shuffle_id).await?;
            log::debug!("fetched locs from client: {:?}", fetched);
            self.map_output_uris.insert(
                shuffle_id,
                fetched.iter().map(|x| Some(x.clone())).collect(),
            );
            log::debug!("added locs to server uris after fetching");
            self.fetching.remove(&shuffle_id);
            Ok(fetched)
        } else {
            Ok(self
                .map_output_uris
                .get(&shuffle_id)
                .ok_or_else(|| MapOutputError::ShuffleIdNotFound(shuffle_id))?
                .iter()
                .filter_map(|x| x.clone())
                .collect())
        }
    }

    pub fn increment_generation(&self) {
        *self.generation.lock() += 1;
    }

    pub fn get_generation(&self) -> i64 {
        *self.generation.lock()
    }

    pub fn update_generation(&mut self, new_gen: i64) {
        if new_gen > *self.generation.lock() {
            self.map_output_uris = Arc::new(DashMap::new());
            *self.generation.lock() = new_gen;
        }
    }
}

#[derive(Debug, Error)]
pub enum MapOutputError {
    #[error("Shuffle id output #{0} not found in the map")]
    ShuffleIdNotFound(usize),

    #[error("map_id {map_id} out of bounds for shuffle_id {shuffle_id} (num_maps={num_maps})")]
    MapIdOutOfBounds {
        shuffle_id: usize,
        map_id: usize,
        num_maps: usize,
    },

    #[error(transparent)]
    NetworkError(#[from] NetworkError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tracker() -> MapOutputTracker {
        // Default avoids starting the master status server (no runtime in unit tests).
        MapOutputTracker::default()
    }

    #[test]
    fn host_outputs_invalidated() {
        let t = tracker();
        // shuffle 0: map 0 on host A, map 1 on host B.
        t.register_map_outputs(
            0,
            vec![
                Some("http://10.0.0.1:9000".to_string()),
                Some("http://10.0.0.2:9000".to_string()),
            ],
        );
        // shuffle 1: both maps on host A.
        t.register_map_outputs(
            1,
            vec![
                Some("http://10.0.0.1:9000".to_string()),
                Some("http://10.0.0.1:9000".to_string()),
            ],
        );

        let cleared = t.unregister_outputs_on_host("10.0.0.1");
        assert_eq!(cleared, 3, "all three outputs on host A must be cleared");

        // Host A slots are now empty; host B is untouched.
        assert_eq!(
            t.map_output_uris.get(&0).unwrap().clone(),
            vec![None, Some("http://10.0.0.2:9000".to_string())]
        );
        assert_eq!(t.map_output_uris.get(&1).unwrap().clone(), vec![None, None]);
    }

    #[test]
    fn unknown_host_noop() {
        let t = tracker();
        t.register_map_outputs(0, vec![Some("http://10.0.0.2:9000".to_string())]);
        assert_eq!(t.unregister_outputs_on_host("10.9.9.9"), 0);
        assert_eq!(
            t.map_output_uris.get(&0).unwrap().clone(),
            vec![Some("http://10.0.0.2:9000".to_string())]
        );
    }
}
