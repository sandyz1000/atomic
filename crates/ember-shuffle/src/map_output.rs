use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use crate::error::NetworkError;
use dashmap::{DashMap, DashSet};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};

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
    pub server_uris: ServerUris,
    fetching: Arc<DashSet<usize>>,
    generation: Arc<Mutex<i64>>,
    master_addr: SocketAddr,
}

// Only master_addr doesn't have a default.
impl Default for MapOutputTracker {
    fn default() -> Self {
        MapOutputTracker {
            is_master: Default::default(),
            server_uris: Default::default(),
            fetching: Default::default(),
            generation: Default::default(),
            master_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
        }
    }
}

impl MapOutputTracker {
    pub fn new(is_master: bool, master_addr: SocketAddr) -> Self {
        let output_tracker = MapOutputTracker {
            is_master,
            server_uris: Arc::new(DashMap::new()),
            fetching: Arc::new(DashSet::new()),
            generation: Arc::new(Mutex::new(0)),
            master_addr,
        };
        output_tracker.server();
        output_tracker
    }

    async fn client(&self, shuffle_id: usize) -> Result<Vec<String>> {
        log::debug!(
            "connecting to master to fetch shuffle task #{} data hosts",
            shuffle_id
        );

        // Serialize the shuffle_id request
        let shuffle_id_bytes = bincode::encode_to_vec(&shuffle_id, bincode::config::standard())
            .map_err(|e| {
                NetworkError::BincodeError(format!("Failed to serialize shuffle_id: {}", e))
            })?;

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
            .map_err(|e| NetworkError::HttpError(format!("Handshake failed: {}", e)))?;

        // Spawn connection task
        tokio::spawn(async move {
            if let Err(err) = conn.await {
                log::error!("Connection failed: {:?}", err);
            }
        });

        // Build HTTP request
        let uri: hyper::Uri = format!("http://{}/shuffle", self.master_addr)
            .parse()
            .map_err(|e| {
                MapOutputError::NetworkError(NetworkError::InvalidUri(format!("{}", e)))
            })?;

        let req = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(uri)
            .header("content-type", "application/octet-stream")
            .body(Full::new(Bytes::from(shuffle_id_bytes)))
            .map_err(|e| {
                MapOutputError::NetworkError(NetworkError::HttpError(format!(
                    "Failed to build request: {}",
                    e
                )))
            })?;

        // Send request
        let resp = sender.send_request(req).await.map_err(|e| {
            MapOutputError::NetworkError(NetworkError::HttpError(format!("Request failed: {}", e)))
        })?;

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
            .map_err(|e| {
                MapOutputError::NetworkError(NetworkError::HttpError(format!(
                    "Failed to read response: {}",
                    e
                )))
            })?
            .to_bytes();

        // Deserialize the Vec<String> response
        let (locs, _): (Vec<String>, _) =
            bincode::decode_from_slice(&body_bytes, bincode::config::standard()).map_err(|e| {
                MapOutputError::NetworkError(NetworkError::BincodeError(format!(
                    "Failed to deserialize response: {}",
                    e
                )))
            })?;

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
        let master_addr = self.master_addr;
        let server_uris = self.server_uris.clone();

        tokio::spawn(async move {
            let listener = match TcpListener::bind(master_addr).await {
                Ok(l) => l,
                Err(e) => {
                    log::error!("Failed to bind listener: {}", e);
                    return;
                }
            };
            log::debug!("map output tracker server started on {}", master_addr);

            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        log::error!("Failed to accept connection: {}", e);
                        continue;
                    }
                };

                let server_uris_clone = server_uris.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);

                    let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                        let server_uris = server_uris_clone.clone();
                        async move {
                            // Read request body (shuffle_id)
                            let body_bytes = match req.collect().await {
                                Ok(collected) => collected.to_bytes(),
                                Err(e) => {
                                    log::error!("Failed to read request body: {}", e);
                                    return Ok::<_, hyper::Error>(
                                        Response::builder()
                                            .status(StatusCode::BAD_REQUEST)
                                            .body(Full::new(Bytes::from("Failed to read request")))
                                            .unwrap(),
                                    );
                                }
                            };

                            // Deserialize shuffle_id
                            let shuffle_id: usize = match bincode::decode_from_slice(
                                &body_bytes,
                                bincode::config::standard(),
                            ) {
                                Ok((id, _)) => id,
                                Err(e) => {
                                    log::error!("Failed to deserialize shuffle_id: {}", e);
                                    return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Full::new(Bytes::from("Invalid shuffle_id")))
                                        .unwrap());
                                }
                            };

                            log::debug!("received request for shuffle id #{}", shuffle_id);

                            // Wait until shuffle data is available
                            loop {
                                match server_uris.get(&shuffle_id) {
                                    Some(uris) => {
                                        let ready_count =
                                            uris.iter().filter(|x| x.is_some()).count();
                                        if ready_count > 0 {
                                            break;
                                        }
                                    }
                                    None => {
                                        log::error!("shuffle id #{} not found", shuffle_id);
                                        return Ok(Response::builder()
                                            .status(StatusCode::NOT_FOUND)
                                            .body(Full::new(Bytes::from("Shuffle ID not found")))
                                            .unwrap());
                                    }
                                }
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }

                            // Get locations
                            let locs = server_uris
                                .get(&shuffle_id)
                                .map(|kv| {
                                    kv.value()
                                        .iter()
                                        .cloned()
                                        .filter_map(|x| x)
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default();

                            log::debug!(
                                "locs inside map output tracker server for shuffle id #{}: {:?}",
                                shuffle_id,
                                locs
                            );

                            // Serialize response
                            let response_bytes =
                                match bincode::encode_to_vec(&locs, bincode::config::standard()) {
                                    Ok(bytes) => bytes,
                                    Err(e) => {
                                        log::error!("Failed to serialize response: {}", e);
                                        return Ok(Response::builder()
                                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                                            .body(Full::new(Bytes::from("Serialization failed")))
                                            .unwrap());
                                    }
                                };

                            Ok(Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/octet-stream")
                                .body(Full::new(Bytes::from(response_bytes)))
                                .unwrap())
                        }
                    });

                    if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                        log::error!("Error serving connection: {:?}", err);
                    }
                });
            }
        });
    }

    pub fn register_shuffle(&self, shuffle_id: usize, num_maps: usize) {
        log::debug!("inside register shuffle");
        if self.server_uris.get(&shuffle_id).is_some() {
            // TODO: error handling
            log::debug!("map tracker register shuffle none");
            return;
        }
        self.server_uris.insert(shuffle_id, vec![None; num_maps]);
        log::debug!("server_uris after register_shuffle {:?}", self.server_uris);
    }

    pub fn register_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String) {
        log::debug!(
            "registering map output from shuffle task #{} with map id #{} at server: {}",
            shuffle_id,
            map_id,
            server_uri
        );
        self.server_uris.get_mut(&shuffle_id).unwrap()[map_id] = Some(server_uri);
    }

    pub fn register_map_outputs(&self, shuffle_id: usize, locs: Vec<Option<String>>) {
        log::debug!(
            "registering map outputs inside map output tracker for shuffle id #{}: {:?}",
            shuffle_id,
            locs
        );
        self.server_uris.insert(shuffle_id, locs);
    }

    pub fn unregister_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String) {
        let array = self.server_uris.get(&shuffle_id);
        if let Some(arr) = array {
            if arr.get(map_id).unwrap() == &Some(server_uri) {
                self.server_uris
                    .get_mut(&shuffle_id)
                    .unwrap()
                    .insert(map_id, None)
            }
            self.increment_generation();
        } else {
            // TODO: error logging
        }
    }

    pub async fn get_server_uris(&self, shuffle_id: usize) -> Result<Vec<String>> {
        log::debug!(
            "trying to get uri for shuffle task #{}, current server uris: {:?}",
            shuffle_id,
            self.server_uris
        );

        if self
            .server_uris
            .get(&shuffle_id)
            .map(|some| some.iter().filter_map(|x| x.clone()).next())
            .flatten()
            .is_none()
        {
            if self.fetching.contains(&shuffle_id) {
                while self.fetching.contains(&shuffle_id) {
                    // TODO: check whether this will hurt the performance or not
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                let servers = self
                    .server_uris
                    .get(&shuffle_id)
                    .ok_or_else(|| MapOutputError::ShuffleIdNotFound(shuffle_id))?
                    .iter()
                    .filter(|x| !x.is_none())
                    .map(|x| x.clone().unwrap())
                    .collect::<Vec<_>>();
                log::debug!("returning after fetching done, return: {:?}", servers);
                return Ok(servers);
            } else {
                log::debug!("adding to fetching queue");
                self.fetching.insert(shuffle_id);
            }
            let fetched = self.client(shuffle_id).await?;
            log::debug!("fetched locs from client: {:?}", fetched);
            self.server_uris.insert(
                shuffle_id,
                fetched.iter().map(|x| Some(x.clone())).collect(),
            );
            log::debug!("added locs to server uris after fetching");
            self.fetching.remove(&shuffle_id);
            Ok(fetched)
        } else {
            Ok(self
                .server_uris
                .get(&shuffle_id)
                .ok_or_else(|| MapOutputError::ShuffleIdNotFound(shuffle_id))?
                .iter()
                .filter(|x| !x.is_none())
                .map(|x| x.clone().unwrap())
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
            self.server_uris = Arc::new(DashMap::new());
            *self.generation.lock() = new_gen;
        }
    }
}

#[derive(Debug, Error)]
pub enum MapOutputError {
    #[error("Shuffle id output #{0} not found in the map")]
    ShuffleIdNotFound(usize),

    #[error(transparent)]
    NetworkError(#[from] NetworkError),
}
