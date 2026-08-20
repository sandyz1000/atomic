use std::convert::TryFrom;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::shuffle::cache::ShuffleCache;
use crate::shuffle::config::ShuffleConfig;
use crate::shuffle::error::{NetworkError, ShuffleError};
use atomic_utils::get_dynamic_port;
use crossbeam::channel as cb_channel;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Request, Response, StatusCode, Uri, body::Incoming, service::Service};
use hyper_util::rt::TokioIo;
use uuid::Uuid;

pub type LibResult<T> = Result<T, ShuffleError>;
pub type Body = Full<Bytes>;

fn get_free_connection(ip: Ipv4Addr) -> LibResult<(TcpListener, u16)> {
    let mut port = 0;
    for _ in 0..100 {
        port = get_dynamic_port();
        let bind_addr = SocketAddr::from((ip, port));
        if let Ok(conn) = TcpListener::bind(bind_addr) {
            return Ok((conn, port));
        }
    }
    Err(ShuffleError::NetworkError(NetworkError::FreePortNotFound(
        port, 100,
    )))
}

/// Creates directories and files required for storing shuffle data.
/// It also creates the file server required for serving files via HTTP request.
pub struct ShuffleManager {
    config: ShuffleConfig,
    shuffle_dir: PathBuf,
    server_uri: String,
    pub server_port: u16,
    ask_status: cb_channel::Sender<()>,
    rcv_status: cb_channel::Receiver<LibResult<StatusCode>>,
}

impl ShuffleManager {
    /// Create a new ShuffleManager with dependency injection
    pub fn new(config: ShuffleConfig, cache: Arc<dyn ShuffleCache>) -> LibResult<Self> {
        let shuffle_dir = Self::get_shuffle_data_dir(&config.local_dir)?;
        fs::create_dir_all(&shuffle_dir).map_err(|_| ShuffleError::CouldNotCreateShuffleDir)?;

        let (server_uri, server_port) =
            Self::start_server(config.local_ip, config.shuffle_port, cache.clone(), &config)?;
        let (send_main, rcv_main) = Self::init_status_checker(&server_uri)?;

        let manager = ShuffleManager {
            config,
            shuffle_dir,
            server_uri,
            server_port,
            ask_status: send_main,
            rcv_status: rcv_main,
        };

        if let Ok(StatusCode::OK) = manager.check_status() {
            Ok(manager)
        } else {
            Err(ShuffleError::FailedToStart)
        }
    }

    pub fn get_server_uri(&self) -> String {
        self.server_uri.clone()
    }

    pub fn clean_up_shuffle_data(&self) {
        if self.config.log_cleanup
            && let Err(e) = fs::remove_dir_all(&self.shuffle_dir)
        {
            log::error!("failed removing tmp work dir: {}", e);
        }
    }

    pub fn check_status(&self) -> LibResult<StatusCode> {
        self.ask_status
            .send(())
            .map_err(|_| ShuffleError::InternalError)?;
        // Use a timeout so this doesn't hang when the status-checker task can't run
        // (e.g. when called from a sync context inside a current-thread Tokio runtime).
        self.rcv_status
            .recv_timeout(Duration::from_secs(2))
            .map_err(|_| ShuffleError::Other)?
    }

    /// Returns the shuffle server URI as a string.
    fn start_server(
        bind_ip: Ipv4Addr,
        port: Option<u16>,
        cache: Arc<dyn ShuffleCache>,
        shuffle_config: &ShuffleConfig,
    ) -> LibResult<(String, u16)> {
        let tls_acceptor = Self::tls_acceptor(shuffle_config)?;

        let bind_port = if let Some(p) = port {
            let conn = TcpListener::bind(SocketAddr::from((bind_ip, p))).map_err(|_| {
                let err: ShuffleError = NetworkError::FreePortNotFound(p, 0).into();
                err
            })?;
            Self::launch_async_server(conn, cache, tls_acceptor)?;
            p
        } else {
            let (conn, p) = get_free_connection(bind_ip)?;
            Self::launch_async_server(conn, cache, tls_acceptor)?;
            p
        };

        let scheme = if shuffle_config.tls_enabled() {
            "https"
        } else {
            "http"
        };
        let server_uri = format!("{}://{}:{}", scheme, bind_ip, bind_port);
        log::debug!("server_uri {:?}", server_uri);
        Ok((server_uri, bind_port))
    }

    fn tls_acceptor(config: &ShuffleConfig) -> LibResult<Option<tokio_rustls::TlsAcceptor>> {
        if config.tls_enabled() {
            Ok(Some(Self::make_tls_acceptor(config)?))
        } else {
            Ok(None)
        }
    }

    fn make_tls_acceptor(config: &ShuffleConfig) -> LibResult<tokio_rustls::TlsAcceptor> {
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        use rustls::{RootCertStore, ServerConfig};
        use rustls_pemfile::{certs, pkcs8_private_keys};
        use std::io::BufReader;
        use std::sync::Arc as StdArc;

        let cert_path = config.tls_cert.as_ref().ok_or(ShuffleError::Other)?;
        let key_path = config.tls_key.as_ref().ok_or(ShuffleError::Other)?;
        let ca_path = config.tls_ca.as_ref().ok_or(ShuffleError::Other)?;

        let chain: Vec<CertificateDer<'static>> = {
            let f = fs::File::open(cert_path).map_err(|_| ShuffleError::Other)?;
            certs(&mut BufReader::new(f))
                .collect::<Result<_, _>>()
                .map_err(|_| ShuffleError::Other)?
        };
        let private_key: PrivateKeyDer<'static> = {
            let f = fs::File::open(key_path).map_err(|_| ShuffleError::Other)?;
            pkcs8_private_keys(&mut BufReader::new(f))
                .next()
                .ok_or(ShuffleError::Other)?
                .map(PrivateKeyDer::Pkcs8)
                .map_err(|_| ShuffleError::Other)?
        };
        let ca_store = {
            let f = fs::File::open(ca_path).map_err(|_| ShuffleError::Other)?;
            let mut store = RootCertStore::empty();
            for cert in certs(&mut BufReader::new(f)).flatten() {
                store.add(cert).map_err(|_| ShuffleError::Other)?;
            }
            store
        };
        let client_auth = rustls::server::WebPkiClientVerifier::builder(StdArc::new(ca_store))
            .build()
            .map_err(|_| ShuffleError::Other)?;
        let cfg = ServerConfig::builder()
            .with_client_cert_verifier(client_auth)
            .with_single_cert(chain, private_key)
            .map_err(|_| ShuffleError::Other)?;
        Ok(tokio_rustls::TlsAcceptor::from(StdArc::new(cfg)))
    }

    fn launch_async_server(
        conn: TcpListener,
        cache: Arc<dyn ShuffleCache>,
        tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    ) -> LibResult<()> {
        use hyper::server::conn::http1;
        let (s, r) = cb_channel::bounded::<LibResult<()>>(1);

        tokio::spawn(async move {
            // Hold the start-up sender for the lifetime of the accept loop so the
            // channel stays open while the caller waits on `r` below; dropping it
            // early would disconnect `r` and spuriously report a start failure.
            let _startup_sender = s;
            conn.set_nonblocking(true)
                .map_err(|_| ShuffleError::FailedToStart)?;
            let listener =
                tokio::net::TcpListener::from_std(conn).map_err(|_| ShuffleError::FailedToStart)?;

            loop {
                let (stream, _) = listener
                    .accept()
                    .await
                    .map_err(|_| ShuffleError::FailedToStart)?;
                let cache_clone = cache.clone();

                if let Some(ref acceptor) = tls_acceptor {
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                let io = TokioIo::new(tls_stream);
                                if let Err(e) = http1::Builder::new()
                                    .serve_connection(io, ShuffleService::new(cache_clone))
                                    .await
                                {
                                    log::error!("TLS shuffle connection error: {e}");
                                }
                            }
                            Err(e) => log::warn!("TLS handshake failed on shuffle server: {e}"),
                        }
                    });
                    continue;
                }

                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    if let Err(err) = http1::Builder::new()
                        .serve_connection(io, ShuffleService::new(cache_clone))
                        .await
                    {
                        log::error!("Error serving connection: {:?}", err);
                    }
                });
            }

            #[allow(unreachable_code)]
            Err::<(), _>(ShuffleError::FailedToStart)
        });

        cb_channel::select! {
            recv(r) -> msg => { msg.map_err(|_| ShuffleError::FailedToStart)??; }
            default(Duration::from_millis(25)) => log::debug!("started shuffle server"),
        };
        Ok(())
    }

    fn init_status_checker(
        server_uri: &str,
    ) -> LibResult<(
        cb_channel::Sender<()>,
        cb_channel::Receiver<LibResult<StatusCode>>,
    )> {
        // Build a two way com lane between the main thread and the background running executor
        let (send_child, rcv_main) = cb_channel::unbounded::<LibResult<StatusCode>>();
        let (send_main, rcv_child) = cb_channel::unbounded::<()>();
        let uri_str = format!("{}/status", server_uri);
        let status_uri = Uri::try_from(&uri_str)?;

        tokio::spawn(async move {
            loop {
                if let Ok(()) = rcv_child.try_recv() {
                    // Create a new connection for each request
                    match status_uri.authority() {
                        Some(authority) => {
                            let host = authority.host();
                            let port = authority.port_u16().unwrap_or(80);

                            match tokio::net::TcpStream::connect((host, port)).await {
                                Ok(stream) => {
                                    let io = TokioIo::new(stream);
                                    let handshake = hyper::client::conn::http1::handshake(io).await;
                                    let (mut sender, conn) = match handshake {
                                        Ok(pair) => pair,
                                        Err(e) => {
                                            log::error!("Status-check handshake failed: {:?}", e);
                                            let _ =
                                                send_child.send(Err(ShuffleError::InternalError));
                                            tokio::time::sleep(Duration::from_millis(25)).await;
                                            continue;
                                        }
                                    };

                                    tokio::spawn(async move {
                                        if let Err(err) = conn.await {
                                            log::error!("Connection failed: {:?}", err);
                                        }
                                    });

                                    let request = Request::builder()
                                        .uri(status_uri.clone())
                                        .body(Body::default())
                                        .expect("invariant: status-check request is well-formed");

                                    match sender.send_request(request).await {
                                        Ok(res) => {
                                            let _ = send_child.send(Ok(res.status()));
                                        }
                                        Err(e) => {
                                            log::error!("Status check failed: {:?}", e);
                                            let _ =
                                                send_child.send(Err(ShuffleError::InternalError));
                                        }
                                    }
                                }
                                Err(e) => {
                                    log::error!("Connection failed: {:?}", e);
                                    let _ = send_child.send(Err(ShuffleError::InternalError));
                                }
                            }
                        }
                        None => {
                            let _ = send_child.send(Err(ShuffleError::InternalError));
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        });
        Ok((send_main, rcv_main))
    }

    fn get_shuffle_data_dir(local_dir_root: &std::path::Path) -> LibResult<PathBuf> {
        for _ in 0..10 {
            let local_dir = local_dir_root.join(format!("ns-shuffle-{}", Uuid::new_v4()));
            if !local_dir.exists() {
                log::debug!("creating directory at path: {:?}", local_dir);
                fs::create_dir_all(&local_dir)
                    .map_err(|_| ShuffleError::CouldNotCreateShuffleDir)?;
                return Ok(local_dir);
            }
        }
        Err(ShuffleError::CouldNotCreateShuffleDir)
    }
}

struct ShuffleService {
    cache: Arc<dyn ShuffleCache>,
}

enum ShuffleResponse {
    Status(StatusCode),
    CachedData(Vec<u8>),
}

impl ShuffleService {
    fn new(cache: Arc<dyn ShuffleCache>) -> Self {
        Self { cache }
    }

    fn response_type(&self, uri: &Uri) -> LibResult<ShuffleResponse> {
        let parts: Vec<_> = uri.path().split('/').collect();
        match parts.as_slice() {
            [_, endpoint] if *endpoint == "status" => Ok(ShuffleResponse::Status(StatusCode::OK)),
            // Lightweight liveness probe used by the driver's heartbeat loop.
            [_, endpoint] if *endpoint == "health" => Ok(ShuffleResponse::Status(StatusCode::OK)),
            [_, endpoint, shuffle_id, input_id, reduce_id] if *endpoint == "shuffle" => Ok(
                ShuffleResponse::CachedData(
                    self.get_cached_data(uri, &[*shuffle_id, *input_id, *reduce_id])?,
                ),
            ),
            _ => Err(ShuffleError::UnexpectedUri(uri.path().to_string())),
        }
    }

    fn get_cached_data(&self, uri: &Uri, parts: &[&str]) -> LibResult<Vec<u8>> {
        // the path is: .../{shuffleid}/{inputid}/{reduceid}
        let parts: Vec<_> = match parts
            .iter()
            .map(|part| ShuffleService::parse_path_part(part))
            .collect::<LibResult<_>>()
        {
            Err(_err) => {
                return Err(ShuffleError::UnexpectedUri(format!("{}", uri)));
            }
            Ok(parts) => parts,
        };
        // Resolve the reduce partition's bytes from either the consolidated (sort-shuffle)
        // layout or the legacy per-bucket layout — transparent to the reduce client.
        if let Some(cached_data) = crate::shuffle::cache::get_consolidated_or_bucket(
            self.cache.as_ref(),
            parts[0],
            parts[1],
            parts[2],
        ) {
            log::debug!(
                "got a request @ `{}`, params: {:?}, returning data",
                uri,
                (parts[0], parts[1], parts[2])
            );
            Ok(Vec::from(&cached_data[..]))
        } else {
            Err(ShuffleError::RequestedCacheNotFound)
        }
    }

    #[inline]
    fn parse_path_part(part: &str) -> LibResult<usize> {
        Ok(part
            .parse::<u64>()
            .map_err(|_| ShuffleError::NotValidRequest)? as usize)
    }
}

fn request_is_authorized(req: &Request<Incoming>) -> bool {
    crate::env::bearer_authorized(
        req.headers()
            .get(hyper::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
    )
}

impl Service<Request<Incoming>> for ShuffleService {
    type Response = Response<Body>;
    type Error = ShuffleError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let uri = req.uri().clone();
        let cache = self.cache.clone();
        let authorized = request_is_authorized(&req);

        Box::pin(async move {
            if !authorized {
                log::warn!("shuffle server: rejected unauthenticated request for {uri}");
                return Response::builder()
                    .status(401)
                    .body(Full::new(Bytes::new()))
                    .map_err(|_| ShuffleError::InternalError);
            }
            let service = ShuffleService::new(cache);
            match service.response_type(&uri) {
                Ok(response) => match response {
                    ShuffleResponse::Status(code) => Response::builder()
                        .status(code)
                        .body(Full::new(Bytes::new()))
                        .map_err(|_| ShuffleError::InternalError),
                    ShuffleResponse::CachedData(cached_data) => Response::builder()
                        .status(200)
                        .body(Full::new(Bytes::from(cached_data)))
                        .map_err(|_| ShuffleError::InternalError),
                },
                Err(err) => Ok(err.into()),
            }
        })
    }
}
