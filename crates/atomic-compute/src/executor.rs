use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::env;
use crate::error::{ComputeError, ComputeResult};
use crate::runtimes::{Backend, ComputeEngine};
use atomic_data::distributed::{
    TRANSPORT_HEADER_LEN, TaskEnvelope, TransportFrameKind, WireDecode, WireEncode,
    WorkerCapabilities, encode_transport_frame, parse_transport_header,
};

use atomic_data::shuffle::error::NetworkError;
use crossbeam::channel::{Receiver, Sender, bounded};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    task::{JoinSet, spawn, spawn_blocking},
};

pub struct Executor {
    port: u16,
    max_concurrent_tasks: u16,
    worker_id: Arc<str>,
    backend: ComputeEngine,
    /// Pre-built TLS acceptor. `None` means plain TCP (default).
    #[cfg(feature = "tls")]
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
}

impl Executor {
    pub fn new(port: u16, max_concurrent_tasks: u16) -> Self {
        Executor {
            port,
            max_concurrent_tasks,
            worker_id: Arc::from(format!("worker-{}", port)),
            backend: ComputeEngine::default(),
            #[cfg(feature = "tls")]
            tls_acceptor: None,
        }
    }

    /// Configure mutual TLS using cert/key/CA PEM files.
    ///
    /// Only available with the `tls` feature. When called, all incoming
    /// connections will be upgraded to mTLS before processing.
    #[cfg(feature = "tls")]
    pub fn with_tls(
        mut self,
        cert: &std::path::Path,
        key: &std::path::Path,
        ca: &std::path::Path,
    ) -> ComputeResult<Self, std::io::Error> {
        use crate::tls::tls_impl::make_server_config;
        let cfg = make_server_config(cert, key, ca)?;
        self.tls_acceptor = Some(Arc::new(tokio_rustls::TlsAcceptor::from(cfg)));
        Ok(self)
    }

    pub fn execute_task(
        &self,
        task: &TaskEnvelope,
    ) -> ComputeResult<atomic_data::distributed::TaskResultEnvelope> {
        self.backend
            .execute(&self.worker_id, task)
            .map_err(|e| ComputeError::InvalidPayload(e.to_string()))
    }

    pub fn worker_capabilities(&self) -> WorkerCapabilities {
        let mut registered_ops: Vec<String> = crate::task_registry::TASK_REGISTRY
            .keys()
            .map(|k| k.to_string())
            .collect();
        // Shuffle map types use a "shuffle:<key>" prefix to avoid colliding with
        // regular op_ids. After Fix 2, SHUFFLE_MAP_REGISTRY is keyed by the stable
        // stringify!-based string (e.g. "String::u32") instead of type_name.
        registered_ops.extend(
            crate::task_registry::SHUFFLE_MAP_REGISTRY
                .keys()
                .map(|k| format!("shuffle:{k}")),
        );
        // Advertise dynamic UDF runtimes so the scheduler can route Python/JS ops.
        #[cfg(feature = "python")]
        registered_ops.push("atomic::udf::python".to_string());
        #[cfg(feature = "js")]
        registered_ops.push("atomic::udf::js".to_string());
        log::debug!(
            "worker {} advertising {} registered ops ({} shuffle types)",
            self.worker_id,
            registered_ops.len(),
            crate::task_registry::SHUFFLE_MAP_REGISTRY.len(),
        );
        WorkerCapabilities::new(
            self.worker_id.to_string(),
            self.max_concurrent_tasks,
            registered_ops,
        )
        .with_registry_fingerprint(*crate::task_registry::REGISTRY_FINGERPRINT)
    }

    /// Worker loop: binds TCP port, reads transport frames, dispatches via ComputeEngine.
    #[allow(dropping_copy_types)]
    pub fn worker(self: Arc<Self>) -> ComputeResult<Signal> {
        env::Env::run_in_async_rt(move || -> ComputeResult<Signal> {
            tokio::runtime::Handle::current().block_on(async move {
                let (send_child, rcv_main) = bounded::<Signal>(100);
                let process_err = Arc::clone(&self).process_stream(rcv_main);
                let handler_err = spawn(Arc::clone(&self).signal_handler(send_child));
                tokio::select! {
                    err = process_err => err,
                    err = handler_err => err?,
                }
            })
        })
    }

    async fn process_stream(self: Arc<Self>, rcv_main: Receiver<Signal>) -> ComputeResult<Signal> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        log::info!("[{}] worker listening on {}", self.worker_id, addr);

        let mut tasks: JoinSet<ComputeResult<Signal>> = JoinSet::new();

        loop {
            match rcv_main.try_recv() {
                Ok(Signal::ShutDownGracefully) => {
                    log::info!("shutting down executor @{} gracefully", self.port);
                    tasks.shutdown().await;
                    return Ok(Signal::ShutDownGracefully);
                }
                Ok(Signal::ShutDownError) => {
                    log::info!("shutting down executor @{} due to error", self.port);
                    tasks.abort_all();
                    return Err(ComputeError::ExecutorShutdown);
                }
                _ => {}
            }

            if tasks.len() >= self.max_concurrent_tasks as usize {
                if let Some(result) = tasks.join_next().await {
                    propagate_task_result(result, self.port)?;
                }
                continue;
            }

            tokio::select! {
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let exec = Arc::clone(&self);
                            #[cfg(feature = "tls")]
                            if let Some(acceptor) = &exec.tls_acceptor {
                                let acceptor = acceptor.clone();
                                tasks.spawn(async move {
                                    match acceptor.accept(stream).await {
                                        Ok(tls_stream) => exec.handle_connection(tls_stream).await,
                                        Err(e) => {
                                            log::warn!("TLS handshake failed: {e}");
                                            Ok(crate::executor::Signal::Continue)
                                        }
                                    }
                                });
                                continue;
                            }
                            tasks.spawn(async move { exec.handle_connection(stream).await });
                        }
                        Err(_) => break,
                    }
                }
                Some(result) = tasks.join_next() => {
                    propagate_task_result(result, self.port)?;
                }
            }
        }
        Err(ComputeError::ExecutorShutdown)
    }

    /// Handle a single accepted connection (plain TCP or TLS): read one transport frame,
    /// execute it, write the response. This runs concurrently for up to `max_concurrent_tasks`
    /// connections.
    async fn handle_connection<S>(self: Arc<Self>, mut stream: S) -> ComputeResult<Signal>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        log::debug!("received new task @{} executor", self.port);
        let (frame_kind, payload) = self.read_transport_frame(&mut stream).await?;
        let exec_clone = Arc::clone(&self);
        let (result_kind, result_payload) =
            spawn_blocking(move || exec_clone.handle_transport_frame(frame_kind, payload))
                .await??;
        self.write_transport_frame(&mut stream, result_kind, &result_payload)
            .await?;
        log::debug!("sent result data to driver");
        Ok(Signal::Continue)
    }

    fn handle_transport_frame(
        self: &Arc<Self>,
        frame_kind: TransportFrameKind,
        payload: Vec<u8>,
    ) -> ComputeResult<(TransportFrameKind, Vec<u8>)> {
        match frame_kind {
            TransportFrameKind::TaskEnvelope => {
                let task = TaskEnvelope::decode_wire(&payload)
                    .map_err(|err| ComputeError::InvalidTransportFrame(err.to_string()))?;
                let result = self.execute_task(&task)?;

                let bytes = result
                    .encode_wire()
                    .map_err(|err| ComputeError::InvalidTransportFrame(err.to_string()))?;

                Ok((TransportFrameKind::TaskResultEnvelope, bytes))
            }
            TransportFrameKind::WorkerCapabilities => {
                let bytes = self
                    .worker_capabilities()
                    .encode_wire()
                    .map_err(|err| ComputeError::InvalidTransportFrame(err.to_string()))?;

                Ok((TransportFrameKind::WorkerCapabilities, bytes))
            }
            other => Err(ComputeError::InvalidTransportFrame(format!(
                "unsupported request frame kind: {:?}",
                other
            ))),
        }
    }

    async fn read_transport_frame<S>(
        &self,
        stream: &mut S,
    ) -> ComputeResult<(TransportFrameKind, Vec<u8>)>
    where
        S: AsyncRead + Unpin,
    {
        let mut header = [0_u8; TRANSPORT_HEADER_LEN];
        stream
            .read_exact(&mut header)
            .await
            .map_err(ComputeError::InputRead)?;
        let (frame_kind, payload_len) = parse_transport_header(&header)
            .map_err(|err| ComputeError::InvalidTransportFrame(err.to_string()))?;
        let mut payload = vec![0_u8; payload_len];
        stream
            .read_exact(&mut payload)
            .await
            .map_err(ComputeError::InputRead)?;
        Ok((frame_kind, payload))
    }

    async fn write_transport_frame<S>(
        &self,
        stream: &mut S,
        frame_kind: TransportFrameKind,
        payload: &[u8],
    ) -> ComputeResult<()>
    where
        S: AsyncWrite + Unpin,
    {
        let frame = encode_transport_frame(frame_kind, payload);
        stream
            .write_all(&frame)
            .await
            .map_err(ComputeError::OutputWrite)
    }

    /// Listens on `port + 10` for graceful or error shutdown signals.
    async fn signal_handler(self: Arc<Self>, send_child: Sender<Signal>) -> ComputeResult<Signal> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port + 10));
        log::debug!("signal handler port open @ {}", addr.port());
        let listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        let mut signal: ComputeResult<Signal> = Err(ComputeError::ExecutorShutdown);
        loop {
            let (mut stream, _peer) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };
            let mut len_buf = [0u8; 4];
            if stream.read_exact(&mut len_buf).await.is_err() {
                continue;
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            if stream.read_exact(&mut buf).await.is_err() {
                continue;
            }
            let data: Signal = match serde_json::from_slice(&buf) {
                Ok(s) => s,
                Err(_) => continue,
            };
            match data {
                Signal::ShutDownError => {
                    log::info!("received error shutdown signal @ {}", self.port);
                    send_child
                        .send(Signal::ShutDownError)
                        .map_err(|e| ComputeError::Other(e.to_string()))?;
                    signal = Err(ComputeError::ExecutorShutdown);
                    break;
                }
                Signal::ShutDownGracefully => {
                    log::info!("received graceful shutdown signal @ {}", self.port);
                    send_child
                        .send(Signal::ShutDownGracefully)
                        .map_err(|e| ComputeError::Other(e.to_string()))?;
                    signal = Ok(Signal::ShutDownGracefully);
                    break;
                }
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        signal
    }
}

/// Inspect one completed `JoinSet` result.
/// Disconnections are logged and swallowed; other errors propagate.
fn propagate_task_result(
    result: Result<ComputeResult<Signal>, tokio::task::JoinError>,
    port: u16,
) -> ComputeResult<()> {
    match result {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(ComputeError::InputRead(_))) | Ok(Err(ComputeError::OutputWrite(_))) => {
            log::debug!("[worker-{}] peer disconnected, resuming listen", port);
            Ok(())
        }
        Ok(Err(e)) => Err(e),
        Err(_join_err) => Ok(()),
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Signal {
    ShutDownError,
    ShutDownGracefully,
    Continue,
}

#[cfg(test)]
mod tests {
    #![allow(unused_must_use)]

    use super::*;
    use atomic_data::distributed::{
        TRANSPORT_HEADER_LEN, TransportFrameKind, encode_transport_frame, parse_transport_header,
    };
    use atomic_utils::get_dynamic_port;
    use core::time;
    use crossbeam::channel::{Receiver, Sender, unbounded};
    use std::io::Write;
    use std::thread;
    use std::time::{Duration, Instant};

    type Port = u16;

    fn initialize_exec() -> Arc<Executor> {
        let port = get_dynamic_port();
        Arc::new(Executor::new(port, num_cpus::get().max(1) as u16))
    }

    fn connect_to_executor(
        mut port: u16,
        signal_handler: bool,
    ) -> ComputeResult<std::net::TcpStream> {
        use std::net::TcpStream;

        let mut i: usize = 0;
        if signal_handler {
            port += 10;
        }

        let addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], port));
        loop {
            if let Ok(stream) = TcpStream::connect(addr) {
                return Ok(stream);
            }
            thread::sleep(time::Duration::from_millis(10));
            i += 1;
            if i > 10 {
                break;
            }
        }
        Err(ComputeError::Other(format!(
            "timed out after 10 retries connecting to executor at port {}",
            port
        )))
    }

    fn shutdown_msg(stream: &mut std::net::TcpStream) -> ComputeResult<()> {
        let json = serde_json::to_vec(&Signal::ShutDownGracefully)
            .map_err(|e| ComputeError::Other(e.to_string()))?;
        let len = (json.len() as u32).to_le_bytes();
        stream.write_all(&len).map_err(ComputeError::OutputWrite)?;
        stream.write_all(&json).map_err(ComputeError::OutputWrite)?;
        Ok(())
    }

    fn read_transport_response(
        stream: &mut std::net::TcpStream,
    ) -> ComputeResult<(TransportFrameKind, Vec<u8>)> {
        let mut header = [0_u8; TRANSPORT_HEADER_LEN];
        std::io::Read::read_exact(stream, &mut header).map_err(ComputeError::InputRead)?;
        let (kind, payload_len) = parse_transport_header(&header)
            .map_err(|err| ComputeError::InvalidTransportFrame(err.to_string()))?;
        let mut payload = vec![0_u8; payload_len];
        std::io::Read::read_exact(stream, &mut payload).map_err(ComputeError::InputRead)?;
        Ok((kind, payload))
    }

    async fn _start_test<TF, CF>(test_func: TF, checker_func: CF) -> ComputeResult<()>
    where
        TF: FnOnce(Receiver<ComputeResult<()>>, Port) -> ComputeResult<()> + Send + 'static,
        CF: FnOnce(Sender<ComputeResult<()>>, ComputeResult<Signal>) -> ComputeResult<()>,
    {
        let executor = initialize_exec();
        let port = executor.port;
        let (send_exec, client_rcv) = unbounded::<ComputeResult<()>>();

        let test_fut = spawn_blocking(move || test_func(client_rcv, port));
        let worker_fut = spawn_blocking(move || executor.worker());
        let (test_res, worker_res) = tokio::join!(test_fut, worker_fut);
        checker_func(send_exec, worker_res?)?;
        test_res?
    }

    #[tokio::test]
    async fn send_shutdown_signal() -> ComputeResult<()> {
        fn test(client_rcv: Receiver<ComputeResult<()>>, port: Port) -> ComputeResult<()> {
            let end = Instant::now() + Duration::from_millis(150);
            while Instant::now() < end {
                match client_rcv.try_recv() {
                    Ok(Ok(_)) => return Ok(()),
                    Ok(Err(_)) => return Err(ComputeError::Other("test failure".to_string())),
                    _ => {}
                }
                if let Ok(mut stream) = connect_to_executor(port, true) {
                    shutdown_msg(&mut stream)?;
                    return Ok(());
                }
                thread::sleep(time::Duration::from_millis(5));
            }
            Err(ComputeError::Other("unexpected error".to_string()))
        }

        fn result_checker(
            sender: Sender<ComputeResult<()>>,
            result: ComputeResult<Signal>,
        ) -> ComputeResult<()> {
            match result {
                Ok(Signal::ShutDownGracefully) => {
                    sender.send(Ok(()));
                    Ok(())
                }
                Ok(_) | Err(_) => {
                    sender.send(Err(ComputeError::Other("unexpected error".to_string())));
                    Err(ComputeError::Other("unexpected error".to_string()))
                }
            }
        }

        _start_test(test, result_checker).await
    }

    #[tokio::test]
    async fn reports_worker_capabilities() -> ComputeResult<()> {
        fn test(client_rcv: Receiver<ComputeResult<()>>, port: Port) -> ComputeResult<()> {
            let frame = encode_transport_frame(TransportFrameKind::WorkerCapabilities, &[]);

            let end = Instant::now() + Duration::from_millis(150);
            while Instant::now() < end {
                match client_rcv.try_recv() {
                    Ok(Ok(_)) => return Ok(()),
                    Ok(Err(_)) => return Err(ComputeError::Other("test failure".to_string())),
                    _ => {}
                }

                if let Ok(mut stream) = connect_to_executor(port, false) {
                    stream
                        .write_all(&frame)
                        .map_err(ComputeError::OutputWrite)?;
                    let (kind, payload) = read_transport_response(&mut stream)?;
                    assert_eq!(kind, TransportFrameKind::WorkerCapabilities);
                    let capabilities = WorkerCapabilities::decode_wire(&payload)
                        .map_err(|err| ComputeError::InvalidTransportFrame(err.to_string()))?;
                    assert!(!capabilities.worker_id.is_empty());
                    assert!(capabilities.max_tasks >= 1);
                    if let Ok(mut signal) = connect_to_executor(port, true) {
                        let _ = shutdown_msg(&mut signal);
                    }
                    return Ok(());
                }
            }

            Err(ComputeError::Other("unexpected error".to_string()))
        }

        fn result_checker(
            sender: Sender<ComputeResult<()>>,
            result: ComputeResult<Signal>,
        ) -> ComputeResult<()> {
            match result {
                Ok(Signal::ShutDownGracefully) => Ok(()),
                Ok(_) => {
                    sender.send(Err(ComputeError::Other("unexpected error".to_string())));
                    Err(ComputeError::Other("unexpected error".to_string()))
                }
                Err(err) => {
                    sender.send(Err(ComputeError::Other("unexpected error".to_string())));
                    Err(err)
                }
            }
        }

        _start_test(test, result_checker).await
    }
}
