use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, broadcast, mpsc, watch};

pub(crate) type ServerUris = Arc<DashMap<usize, Vec<Option<String>>>>;

const MAX_CONNECTIONS: usize = 500;
const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_SECS: u64 = 64;

/// Spawn the map-output tracking HTTP server.
///
/// Design mirrors mini-redis server architecture:
/// - dedicated `Listener` state
/// - per-connection `Handler`
/// - semaphore connection limit
/// - broadcast shutdown channel for graceful stop path
#[derive(Debug)]
pub(crate) struct MapOutputServerHandle {
    shutdown_tx: watch::Sender<bool>,
    join: tokio::task::JoinHandle<()>,
}

impl MapOutputServerHandle {
    pub(crate) async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.join.await;
    }
}

pub(crate) fn spawn_shuffling_server(
    master_addr: SocketAddr,
    map_output_uris: ServerUris,
) -> MapOutputServerHandle {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let join = tokio::spawn(async move {
        let listener = match bind_with_exponential_backoff(master_addr).await {
            Ok(listener) => listener,
            Err(e) => {
                log::error!("failed to bind map output tracker server: {e}");
                return;
            }
        };

        let (notify_shutdown, _) = broadcast::channel::<()>(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel::<()>(1);

        let mut server = Listener {
            listener,
            map_output_uris,
            limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
            notify_shutdown,
            shutdown_complete_tx,
        };

        tokio::select! {
            res = server.run() => {
                if let Err(err) = res {
                    log::error!("map output tracker accept loop failed: {err}");
                }
            }
            changed = shutdown_rx.changed() => {
                if changed.is_ok() {
                    log::info!("map output tracker shutdown signal received");
                }
            }
        }

        // graceful completion wiring: signal handlers and wait them
        let Listener {
            shutdown_complete_tx,
            notify_shutdown,
            ..
        } = server;
        drop(notify_shutdown);
        drop(shutdown_complete_tx);
        let _ = shutdown_complete_rx.recv().await;
    });

    MapOutputServerHandle { shutdown_tx, join }
}

#[derive(Debug)]
struct Listener {
    listener: TcpListener,
    map_output_uris: ServerUris,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    async fn run(&mut self) -> std::io::Result<()> {
        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .expect("connection semaphore closed unexpectedly");

            let stream = self.accept().await?;

            let mut handler = Handler {
                stream,
                map_output_uris: self.map_output_uris.clone(),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    log::error!("map output handler error: {err}");
                }
                drop(permit);
            });
        }
    }

    async fn accept(&mut self) -> std::io::Result<TcpStream> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => return Ok(stream),
                Err(err) => {
                    if backoff > MAX_BACKOFF_SECS {
                        return Err(err);
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
        }
    }
}

#[derive(Debug)]
struct Shutdown {
    shutdown: bool,
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            shutdown: false,
            notify,
        }
    }

    async fn recv(&mut self) {
        if self.shutdown {
            return;
        }

        let _ = self.notify.recv().await;
        self.shutdown = true;
    }
}

#[derive(Debug)]
struct Handler {
    stream: TcpStream,
    map_output_uris: ServerUris,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let io = TokioIo::new(&mut self.stream);
        let uris = self.map_output_uris.clone();

        let service = service_fn(move |req: Request<hyper::body::Incoming>| {
            let uris = uris.clone();
            async move { Ok::<_, Infallible>(handle_request(req, uris).await) }
        });

        tokio::select! {
            res = http1::Builder::new().serve_connection(io, service) => {
                res.map_err(|e| Box::new(e) as _)
            }
            _ = self.shutdown.recv() => {
                Ok(())
            }
        }
    }
}

async fn bind_with_exponential_backoff(master_addr: SocketAddr) -> std::io::Result<TcpListener> {
    let mut backoff = Duration::from_millis(INITIAL_BACKOFF_MS);

    loop {
        match TcpListener::bind(master_addr).await {
            Ok(listener) => {
                log::debug!(
                    "map output tracker server started on {} (max connections: {})",
                    master_addr,
                    MAX_CONNECTIONS
                );
                return Ok(listener);
            }
            Err(e) => {
                log::warn!(
                    "failed to bind map output server on {}: {} (retrying in {:?})",
                    master_addr,
                    e,
                    backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(MAX_BACKOFF_SECS));
            }
        }
    }
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    map_output_uris: ServerUris,
) -> Response<Full<Bytes>> {
    if req.method() != Method::POST {
        return response(
            StatusCode::METHOD_NOT_ALLOWED,
            Bytes::from("method not allowed"),
        );
    }

    let body_bytes = match req.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            log::error!("failed to read request body: {e}");
            return response(
                StatusCode::BAD_REQUEST,
                Bytes::from("failed to read request body"),
            );
        }
    };

    let shuffle_id = match decode_shuffle_id(&body_bytes) {
        Ok(id) => id,
        Err(e) => {
            log::error!("failed to decode shuffle_id: {e}");
            return response(StatusCode::BAD_REQUEST, Bytes::from("invalid shuffle_id"));
        }
    };

    let locs = match wait_and_collect_locations(&map_output_uris, shuffle_id).await {
        Ok(locs) => locs,
        Err(status) => return response(status, Bytes::from("shuffle id not found")),
    };

    match bincode::encode_to_vec(&locs, bincode::config::standard()) {
        Ok(bytes) => {
            let mut resp = response(StatusCode::OK, Bytes::from(bytes));
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("application/octet-stream"),
            );
            resp
        }
        Err(e) => {
            log::error!("failed to serialize map output locations: {e}");
            response(
                StatusCode::INTERNAL_SERVER_ERROR,
                Bytes::from("serialization failed"),
            )
        }
    }
}

async fn wait_and_collect_locations(
    map_output_uris: &ServerUris,
    shuffle_id: usize,
) -> Result<Vec<String>, StatusCode> {
    loop {
        match collect_locations(map_output_uris, shuffle_id) {
            Ok(locs) if !locs.is_empty() => return Ok(locs),
            Ok(_) => tokio::time::sleep(Duration::from_millis(1)).await,
            Err(status) => return Err(status),
        }
    }
}

fn collect_locations(
    map_output_uris: &ServerUris,
    shuffle_id: usize,
) -> Result<Vec<String>, StatusCode> {
    map_output_uris
        .get(&shuffle_id)
        .map(|kv| kv.value().iter().flatten().cloned().collect::<Vec<_>>())
        .ok_or(StatusCode::NOT_FOUND)
}

fn decode_shuffle_id(body: &[u8]) -> Result<usize, bincode::error::DecodeError> {
    bincode::decode_from_slice(body, bincode::config::standard()).map(|(id, _)| id)
}

fn response(status: StatusCode, body: Bytes) -> Response<Full<Bytes>> {
    match Response::builder().status(status).body(Full::new(body)) {
        Ok(resp) => resp,
        Err(_) => Response::new(Full::new(Bytes::from("internal response build error"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_shuffle_id_success() {
        let bytes = bincode::encode_to_vec(42usize, bincode::config::standard()).unwrap();
        assert_eq!(decode_shuffle_id(&bytes).unwrap(), 42);
    }

    #[test]
    fn decode_shuffle_id_invalid_bytes() {
        let bytes = vec![];
        assert!(decode_shuffle_id(&bytes).is_err());
    }

    #[test]
    fn collect_locations_returns_not_found_for_unknown_shuffle() {
        let uris: ServerUris = Arc::new(DashMap::new());
        assert_eq!(collect_locations(&uris, 7), Err(StatusCode::NOT_FOUND));
    }

    #[test]
    fn collect_locations_returns_flattened_locations() {
        let uris: ServerUris = Arc::new(DashMap::new());
        uris.insert(
            3,
            vec![
                Some("http://10.0.0.1:9000".to_string()),
                None,
                Some("http://10.0.0.2:9000".to_string()),
            ],
        );

        let got = collect_locations(&uris, 3).unwrap();
        assert_eq!(
            got,
            vec![
                "http://10.0.0.1:9000".to_string(),
                "http://10.0.0.2:9000".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn wait_and_collect_locations_waits_until_ready() {
        let uris: ServerUris = Arc::new(DashMap::new());
        uris.insert(5, vec![None, None]);

        let uris_bg = uris.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            if let Some(mut entry) = uris_bg.get_mut(&5) {
                entry[1] = Some("http://10.0.0.3:9000".to_string());
            }
        });

        let got = wait_and_collect_locations(&uris, 5).await.unwrap();
        assert_eq!(got, vec!["http://10.0.0.3:9000".to_string()]);
    }
}
