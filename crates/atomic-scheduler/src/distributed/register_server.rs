use std::net::SocketAddrV4;
use std::sync::Arc;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::{Method, Request, Response, StatusCode};

use super::DistributedScheduler;

type InffResult<T> = Result<T, std::convert::Infallible>;

/// JSON body sent by a worker to `POST /register`.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RegisterRequest {
    /// The worker's own task-listener endpoint (IP:port) as a string, e.g. `"10.0.0.1:10001"`.
    pub endpoint: String,
    /// The worker's capability declaration.
    pub capabilities: atomic_data::distributed::WorkerCapabilities,
}

fn text_response(status: StatusCode, body: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(body)))
        .expect("static status/body response is always valid")
}

/// Handle one `/register` request. Each failure stage (auth, route, body, JSON,
/// endpoint) returns immediately instead of nesting into the next check.
async fn register(
    sched: Arc<DistributedScheduler>,
    req: Request<Incoming>,
) -> InffResult<Response<Full<Bytes>>> {
    let authorized = atomic_data::env::bearer_authorized(
        req.headers()
            .get(hyper::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
    );
    if !authorized {
        log::warn!("register: rejected unauthenticated request");
        return Ok(text_response(StatusCode::UNAUTHORIZED, "unauthorized"));
    }

    if req.method() != Method::POST || req.uri().path() != "/register" {
        return Ok(text_response(StatusCode::NOT_FOUND, "not found"));
    }

    let body_bytes = match req.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            log::warn!("register: failed to read body: {e}");
            return Ok(text_response(StatusCode::BAD_REQUEST, "bad request"));
        }
    };

    let reg: RegisterRequest = match serde_json::from_slice(&body_bytes) {
        Ok(reg) => reg,
        Err(e) => {
            log::warn!("register: JSON parse error: {e}");
            return Ok(text_response(StatusCode::BAD_REQUEST, "invalid JSON"));
        }
    };

    let endpoint = match reg.endpoint.parse::<SocketAddrV4>() {
        Ok(endpoint) => endpoint,
        Err(e) => {
            log::warn!("register: invalid endpoint '{}': {e}", reg.endpoint);
            return Ok(text_response(StatusCode::BAD_REQUEST, "invalid endpoint"));
        }
    };

    sched.dynamically_add_worker(endpoint, reg.capabilities);
    Ok(text_response(StatusCode::OK, "registered"))
}

/// Start an HTTP listener that accepts worker self-registration on `POST /register`.
pub fn start_register_server(port: u16, scheduler: Arc<DistributedScheduler>) {
    tokio::spawn(async move {
        use hyper::server::conn::http1;
        use hyper::service::service_fn;

        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                log::error!("register server: failed to bind on port {port}: {e}");
                return;
            }
        };

        log::info!("worker registration endpoint listening on http://0.0.0.0:{port}/register");

        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                continue;
            };
            let io = hyper_util::rt::TokioIo::new(stream);
            let sched = Arc::clone(&scheduler);

            tokio::spawn(async move {
                let service = service_fn(move |req| register(Arc::clone(&sched), req));
                let _ = http1::Builder::new().serve_connection(io, service).await;
            });
        }
    });
}
