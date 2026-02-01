use std::convert::TryFrom;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::shuffle::cache::ShuffleCache;
use crate::shuffle::config::ShuffleConfig;
use crate::shuffle::error::{NetworkError, ShuffleError};
use crossbeam::channel as cb_channel;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Request, Response, StatusCode, Uri, body::Incoming, service::Service};
use hyper_util::rt::TokioIo;
use rand::Rng;
use uuid::Uuid;

pub(crate) type LibResult<T> = Result<T, ShuffleError>;
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
    Err(ShuffleError::NetworkError(NetworkError::FreePortNotFound(port, 100)))
}

fn get_dynamic_port() -> u16 {
    const FIRST_DYNAMIC_PORT: u16 = 49152;
    const LAST_DYNAMIC_PORT: u16 = 65535;
    rand::rng().random_range(FIRST_DYNAMIC_PORT..LAST_DYNAMIC_PORT)
}

/// Creates directories and files required for storing shuffle data.
/// It also creates the file server required for serving files via HTTP request.
pub struct ShuffleManager {
    config: ShuffleConfig,
    cache: Arc<dyn ShuffleCache>,
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
            Self::start_server(config.local_ip, config.shuffle_port, cache.clone())?;
        let (send_main, rcv_main) = Self::init_status_checker(&server_uri)?;

        let manager = ShuffleManager {
            config,
            cache,
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
        if self.config.log_cleanup {
            if let Err(e) = fs::remove_dir_all(&self.shuffle_dir) {
                log::error!("failed removing tmp work dir: {}", e);
            }
        }
    }

    pub fn get_output_file(
        &self,
        shuffle_id: usize,
        input_id: usize,
        output_id: usize,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let path = self
            .shuffle_dir
            .join(format!("{}/{}", shuffle_id, input_id));
        fs::create_dir_all(&path)?;
        let file_path = path.join(format!("{}", output_id));
        fs::File::create(&file_path)?;
        Ok(file_path
            .to_str()
            .ok_or_else(|| ShuffleError::CouldNotCreateShuffleDir)?
            .to_owned())
    }

    pub fn check_status(&self) -> LibResult<StatusCode> {
        self.ask_status.send(()).unwrap();
        self.rcv_status.recv().map_err(|_| ShuffleError::Other)?
    }

    /// Returns the shuffle server URI as a string.
    fn start_server(
        bind_ip: Ipv4Addr,
        port: Option<u16>,
        cache: Arc<dyn ShuffleCache>,
    ) -> LibResult<(String, u16)> {
        let port = if let Some(bind_port) = port {
            let conn = TcpListener::bind(SocketAddr::from((bind_ip, bind_port))).map_err(|_| {
                let err: ShuffleError = NetworkError::FreePortNotFound(bind_port, 0).into();
                err
            })?;
            Self::launch_async_server(conn, cache)?;
            bind_port
        } else {
            let (conn, bind_port) = get_free_connection(bind_ip)?;
            Self::launch_async_server(conn, cache)?;
            bind_port
        };
        let server_uri = format!("http://{}:{}", bind_ip, port);
        log::debug!("server_uri {:?}", server_uri);
        Ok((server_uri, port))
    }

    fn launch_async_server(conn: TcpListener, cache: Arc<dyn ShuffleCache>) -> LibResult<()> {
        use hyper::server::conn::http1;
        let (s, r) = cb_channel::bounded::<LibResult<()>>(1);

        tokio::spawn(async move {
            conn.set_nonblocking(true)
                .map_err(|_| ShuffleError::FailedToStart)?;

            let listener =
                tokio::net::TcpListener::from_std(conn).map_err(|_| ShuffleError::FailedToStart)?;

            loop {
                let (stream, _) = listener
                    .accept()
                    .await
                    .map_err(|_| ShuffleError::FailedToStart)?;

                let io = TokioIo::new(stream);
                let cache_clone = cache.clone();

                tokio::spawn(async move {
                    let service = ShuffleService::new(cache_clone);
                    if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                        log::error!("Error serving connection: {:?}", err);
                    }
                });
            }

            #[allow(unreachable_code)]
            s.send(Err(ShuffleError::FailedToStart)).unwrap();
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
                                    let (mut sender, conn) =
                                        hyper::client::conn::http1::handshake(io).await.unwrap();

                                    tokio::spawn(async move {
                                        if let Err(err) = conn.await {
                                            log::error!("Connection failed: {:?}", err);
                                        }
                                    });

                                    let request = Request::builder()
                                        .uri(status_uri.clone())
                                        .body(Body::default())
                                        .unwrap();

                                    match sender.send_request(request).await {
                                        Ok(res) => {
                                            send_child.send(Ok(res.status())).unwrap();
                                        }
                                        Err(e) => {
                                            log::error!("Status check failed: {:?}", e);
                                            send_child
                                                .send(Err(ShuffleError::InternalError))
                                                .unwrap();
                                        }
                                    }
                                }
                                Err(e) => {
                                    log::error!("Connection failed: {:?}", e);
                                    send_child.send(Err(ShuffleError::InternalError)).unwrap();
                                }
                            }
                        }
                        None => {
                            send_child.send(Err(ShuffleError::InternalError)).unwrap();
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        });
        Ok((send_main, rcv_main))
    }

    fn get_shuffle_data_dir(local_dir_root: &PathBuf) -> LibResult<PathBuf> {
        for _ in 0..10 {
            let local_dir =
                local_dir_root.join(format!("ns-shuffle-{}", Uuid::new_v4().to_string()));
            if !local_dir.exists() {
                log::debug!("creating directory at path: {:?}", &local_dir);
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
        let params = &(parts[0], parts[1], parts[2]);
        if let Some(cached_data) = self.cache.get(params) {
            log::debug!(
                "got a request @ `{}`, params: {:?}, returning data",
                uri,
                params
            );
            Ok(Vec::from(&cached_data[..]))
        } else {
            Err(ShuffleError::RequestedCacheNotFound)
        }
    }

    #[inline]
    fn parse_path_part(part: &str) -> LibResult<usize> {
        Ok(u64::from_str_radix(part, 10).map_err(|_| ShuffleError::NotValidRequest)? as usize)
    }
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

        Box::pin(async move {
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

// TODO: Tests have been commented out because they depend on env:: module which is not available
// in the atomic-shuffle crate. These tests need to be rewritten in the main atomic crate where
// env:: is available, creating concrete implementations of ShuffleCache and passing them to
// ShuffleManager::new() along with ShuffleConfig.
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use http_body_util::BodyExt;
//     use std::sync::Arc;
//     use std::thread;
//
//     async fn make_request(uri: Uri) -> Result<Response<Incoming>, Box<dyn std::error::Error>> {
//         let host = uri.host().unwrap();
//         let port = uri.port_u16().unwrap_or(80);
//
//         let stream = tokio::net::TcpStream::connect((host, port)).await?;
//         let io = TokioIo::new(stream);
//
//         let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
//
//         tokio::spawn(async move {
//             if let Err(err) = conn.await {
//                 log::error!("Connection failed: {:?}", err);
//             }
//         });
//
//         let request = Request::builder().uri(uri).body(Body::default())?;
//
//         let response = sender.send_request(request).await?;
//         Ok(response)
//     }
//
//     #[tokio::test]
//     async fn start_ok() -> Result<(), Box<dyn std::error::Error + 'static>> {
//         let (_, port) = ShuffleManager::start_server(None)?;
//
//         let url = format!(
//             "http://{}:{}/status",
//             env::Configuration::get().local_ip,
//             port
//         );
//         let res = make_request(Uri::try_from(&url)?).await?;
//         assert_eq!(res.status(), StatusCode::OK);
//         Ok(())
//     }
//
//     #[test]
//     fn start_failure() -> Result<(), Box<dyn std::error::Error + 'static>> {
//         // bind first so it fails while trying to start
//         let (_conn, port) = crate::manager::get_free_connection("0.0.0.0".parse().unwrap())?;
//         assert!(
//             ShuffleManager::start_server(Some(port))
//                 .unwrap_err()
//                 .no_port()
//         );
//         Ok(())
//     }
//
//     #[test]
//     fn status_checking_ok() -> Result<(), Box<dyn std::error::Error + 'static>> {
//         let parallelism = num_cpus::get();
//         let manager = Arc::new(env::Env::run_in_async_rt(|| ShuffleManager::new().unwrap()));
//         let mut threads = Vec::with_capacity(parallelism);
//         for _ in 0..parallelism {
//             let manager = manager.clone();
//             threads.push(thread::spawn(move || -> LibResult<()> {
//                 for _ in 0..10 {
//                     env::Env::run_in_async_rt(|| -> LibResult<()> {
//                         match manager.check_status() {
//                             Ok(StatusCode::OK) => Ok(()),
//                             _ => Err(ShuffleError::Other),
//                         }
//                     })?;
//                 }
//                 Ok(())
//             }));
//         }
//         let results = threads
//             .into_iter()
//             .filter_map(|res| res.join().ok())
//             .collect::<LibResult<Vec<_>>>()?;
//         assert_eq!(results.len(), parallelism);
//         manager.clean_up_shuffle_data();
//         Ok(())
//     }
//
//     #[tokio::test]
//     async fn cached_data_found() -> Result<(), Box<dyn std::error::Error + 'static>> {
//         let (_, port) = ShuffleManager::start_server(None)?;
//         let data = b"some random bytes".iter().copied().collect::<Vec<u8>>();
//         {
//             env::SHUFFLE_CACHE.insert((2, 1, 0), data.clone());
//         }
//         let url = format!(
//             "http://{}:{}/shuffle/2/1/0",
//             env::Configuration::get().local_ip,
//             port
//         );
//         let res = make_request(Uri::try_from(&url)?).await?;
//         assert_eq!(res.status(), StatusCode::OK);
//         let body = res.into_body().collect().await?.to_bytes();
//         assert_eq!(body.to_vec(), data);
//         Ok(())
//     }
//
//     #[tokio::test]
//     async fn cached_data_not_found() -> Result<(), Box<dyn std::error::Error + 'static>> {
//         let (_, port) = ShuffleManager::start_server(None)?;
//
//         let url = format!(
//             "http://{}:{}/shuffle/0/1/2",
//             env::Configuration::get().local_ip,
//             port
//         );
//         let res = make_request(Uri::try_from(&url)?).await?;
//         assert_eq!(res.status(), StatusCode::NOT_FOUND);
//         Ok(())
//     }
//
//     #[tokio::test]
//     async fn not_valid_endpoint() -> Result<(), Box<dyn std::error::Error + 'static>> {
//         let (_, port) = ShuffleManager::start_server(None)?;
//
//         let url = format!(
//             "http://{}:{}/not_valid",
//             env::Configuration::get().local_ip,
//             port
//         );
//         let res = make_request(Uri::try_from(&url)?).await?;
//         assert_eq!(res.status(), StatusCode::BAD_REQUEST);
//         let body = res.into_body().collect().await?.to_bytes();
//         let body_str = String::from_utf8(body.to_vec())?;
//         assert_eq!(body_str, "Failed to parse: /not_valid");
//         Ok(())
//     }
// }
