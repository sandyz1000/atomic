use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::backend::NativeBackend;
use crate::env;
use crate::error::{Error, LibResult};
use atomic_data::distributed::{
    TRANSPORT_HEADER_LEN, TaskEnvelope, TransportFrameKind, WireDecode, WireEncode,
    WorkerCapabilities, encode_transport_frame, parse_transport_header,
};

use atomic_data::shuffle::error::NetworkError;
use crossbeam::channel::{Receiver, Sender, bounded};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::{spawn, spawn_blocking},
};

pub struct Executor {
    port: u16,
    max_concurrent_tasks: u16,
    worker_id: Arc<str>,
    backend: NativeBackend,
}

impl Executor {
    pub fn new(port: u16, max_concurrent_tasks: u16) -> Self {
        Executor {
            port,
            max_concurrent_tasks,
            worker_id: Arc::from(format!("worker-{}", port)),
            backend: NativeBackend,
        }
    }

    pub fn execute_task(
        &self,
        task: &TaskEnvelope,
    ) -> LibResult<atomic_data::distributed::TaskResultEnvelope> {
        self.backend
            .execute(&self.worker_id, task)
            .map_err(|e| Error::InvalidPayload(e.to_string()))
    }

    pub fn worker_capabilities(&self) -> WorkerCapabilities {
        WorkerCapabilities {
            version: atomic_data::distributed::WIRE_SCHEMA_V1,
            worker_id: self.worker_id.to_string(),
            max_tasks: self.max_concurrent_tasks,
        }
    }

    /// Worker loop: binds TCP port, reads transport frames, dispatches via NativeBackend.
    #[allow(dropping_copy_types)]
    pub fn worker(self: Arc<Self>) -> LibResult<Signal> {
        env::Env::run_in_async_rt(move || -> LibResult<Signal> {
            futures::executor::block_on(async move {
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

    #[allow(dropping_copy_types)]
    async fn process_stream(self: Arc<Self>, rcv_main: Receiver<Signal>) -> LibResult<Signal> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        log::info!("[{}] worker listening on {}", self.worker_id, addr);
        loop {
            let (mut stream, _peer) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };
            let rcv_main = rcv_main.clone();
            let executor = Arc::clone(&self);
            let res: LibResult<Signal> = spawn(async move {
                match rcv_main.try_recv() {
                    Ok(Signal::ShutDownError) => {
                        log::info!("shutting down executor @{} due to error", executor.port);
                        return Err(Error::ExecutorShutdown);
                    }
                    Ok(Signal::ShutDownGracefully) => {
                        log::info!("shutting down executor @{} gracefully", executor.port);
                        return Ok(Signal::ShutDownGracefully);
                    }
                    _ => {}
                }
                log::debug!("received new task @{} executor", executor.port);
                let (frame_kind, payload) = executor.read_transport_frame(&mut stream).await?;
                let exec_clone = Arc::clone(&executor);
                let (result_kind, result_payload) =
                    spawn_blocking(move || exec_clone.handle_transport_frame(frame_kind, payload))
                        .await??;

                executor.write_transport_frame(&mut stream, result_kind, &result_payload)
                    .await?;

                log::debug!("sent result data to driver");
                Ok(Signal::Continue)
            })
            .await?;
            match res {
                Ok(Signal::Continue) => continue,
                Ok(s) => return Ok(s),
                // Peer disconnected mid-frame (e.g. a health-check probe that immediately
                // drops the connection). Log and keep the listener running.
                Err(Error::InputRead(_)) | Err(Error::OutputWrite(_)) => {
                    log::debug!("[{}] peer disconnected, resuming listen", self.port);
                    continue;
                }
                Err(s) => return Err(s),
            }
        }
        Err(Error::ExecutorShutdown)
    }

    fn handle_transport_frame(
        self: &Arc<Self>,
        frame_kind: TransportFrameKind,
        payload: Vec<u8>,
    ) -> LibResult<(TransportFrameKind, Vec<u8>)> {
        match frame_kind {
            TransportFrameKind::TaskEnvelope => {
                let task = TaskEnvelope::decode_wire(&payload)
                    .map_err(|err| Error::InvalidTransportFrame(err.to_string()))?;
                let result = self.execute_task(&task)?;

                let bytes = result
                    .encode_wire()
                    .map_err(|err| Error::InvalidTransportFrame(err.to_string()))?;

                Ok((TransportFrameKind::TaskResultEnvelope, bytes))
            }
            TransportFrameKind::WorkerCapabilities => {
                let bytes = self
                    .worker_capabilities()
                    .encode_wire()
                    .map_err(|err| Error::InvalidTransportFrame(err.to_string()))?;

                Ok((TransportFrameKind::WorkerCapabilities, bytes))
            }
            other => Err(Error::InvalidTransportFrame(format!(
                "unsupported request frame kind: {:?}",
                other
            ))),
        }
    }

    async fn read_transport_frame(
        &self,
        stream: &mut tokio::net::TcpStream,
    ) -> LibResult<(TransportFrameKind, Vec<u8>)> {
        let mut header = [0_u8; TRANSPORT_HEADER_LEN];
        stream
            .read_exact(&mut header)
            .await
            .map_err(Error::InputRead)?;
        let (frame_kind, payload_len) = parse_transport_header(&header)
            .map_err(|err| Error::InvalidTransportFrame(err.to_string()))?;
        let mut payload = vec![0_u8; payload_len];
        stream
            .read_exact(&mut payload)
            .await
            .map_err(Error::InputRead)?;
        Ok((frame_kind, payload))
    }

    async fn write_transport_frame(
        &self,
        stream: &mut tokio::net::TcpStream,
        frame_kind: TransportFrameKind,
        payload: &[u8],
    ) -> LibResult<()> {
        let frame = encode_transport_frame(frame_kind, payload);
        stream.write_all(&frame).await.map_err(Error::OutputWrite)
    }

    /// Listens on `port + 10` for graceful or error shutdown signals.
    async fn signal_handler(self: Arc<Self>, send_child: Sender<Signal>) -> LibResult<Signal> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port + 10));
        log::debug!("signal handler port open @ {}", addr.port());
        let listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        let mut signal: LibResult<Signal> = Err(Error::ExecutorShutdown);
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
                        .map_err(|_| Error::Other)?;
                    signal = Err(Error::ExecutorShutdown);
                    break;
                }
                Signal::ShutDownGracefully => {
                    log::info!("received graceful shutdown signal @ {}", self.port);
                    send_child
                        .send(Signal::ShutDownGracefully)
                        .map_err(|_| Error::Other)?;
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
    use std::time::Duration;

    type Port = u16;
    type ComputeResult = std::result::Result<(), Error>;

    fn initialize_exec() -> Arc<Executor> {
        let port = get_dynamic_port();
        Arc::new(Executor::new(port, num_cpus::get().max(1) as u16))
    }

    fn connect_to_executor(mut port: u16, signal_handler: bool) -> LibResult<std::net::TcpStream> {
        use std::net::TcpStream;

        let mut i: usize = 0;
        if signal_handler {
            port += 10;
        }

        loop {
            let addr: SocketAddr = format!("{}:{}", "0.0.0.0", port).parse().unwrap();
            if let Ok(stream) = TcpStream::connect(addr) {
                return Ok(stream);
            }
            thread::sleep(time::Duration::from_millis(10));
            i += 1;
            if i > 10 {
                break;
            }
        }
        Err(Error::Other)
    }

    fn shutdown_msg(stream: &mut std::net::TcpStream) -> LibResult<()> {
        let json = serde_json::to_vec(&Signal::ShutDownGracefully).map_err(|_| Error::Other)?;
        let len = (json.len() as u32).to_le_bytes();
        stream.write_all(&len).map_err(Error::OutputWrite)?;
        stream.write_all(&json).map_err(Error::OutputWrite)?;
        Ok(())
    }

    fn read_transport_response(
        stream: &mut std::net::TcpStream,
    ) -> LibResult<(TransportFrameKind, Vec<u8>)> {
        let mut header = [0_u8; TRANSPORT_HEADER_LEN];
        std::io::Read::read_exact(stream, &mut header).map_err(Error::InputRead)?;
        let (kind, payload_len) = parse_transport_header(&header)
            .map_err(|err| Error::InvalidTransportFrame(err.to_string()))?;
        let mut payload = vec![0_u8; payload_len];
        std::io::Read::read_exact(stream, &mut payload).map_err(Error::InputRead)?;
        Ok((kind, payload))
    }

    async fn _start_test<TF, CF>(test_func: TF, checker_func: CF) -> LibResult<()>
    where
        TF: FnOnce(Receiver<ComputeResult>, Port) -> LibResult<()> + Send + 'static,
        CF: FnOnce(Sender<ComputeResult>, LibResult<Signal>) -> LibResult<()>,
    {
        let executor = initialize_exec();
        let port = executor.port;
        let (send_exec, client_rcv) = unbounded::<ComputeResult>();

        let test_fut = spawn_blocking(move || test_func(client_rcv, port));
        let worker_fut = spawn_blocking(move || executor.worker());
        let (test_res, worker_res) = tokio::join!(test_fut, worker_fut);
        checker_func(send_exec, worker_res?)?;
        test_res?
    }

    #[tokio::test]
    async fn send_shutdown_signal() -> LibResult<()> {
        fn test(client_rcv: Receiver<ComputeResult>, port: Port) -> LibResult<()> {
            let end = Instant::now() + Duration::from_millis(150);
            while Instant::now() < end {
                match client_rcv.try_recv() {
                    Ok(Ok(_)) => return Ok(()),
                    Ok(Err(_)) => return Err(Error::Other),
                    _ => {}
                }
                if let Ok(mut stream) = connect_to_executor(port, true) {
                    shutdown_msg(&mut stream)?;
                    return Ok(());
                }
                thread::sleep(time::Duration::from_millis(5));
            }
            Err(Error::Other)
        }

        fn result_checker(
            sender: Sender<ComputeResult>,
            result: LibResult<Signal>,
        ) -> LibResult<()> {
            match result {
                Ok(Signal::ShutDownGracefully) => {
                    sender.send(Ok(()));
                    Ok(())
                }
                Ok(_) | Err(_) => {
                    sender.send(Err(Error::Other));
                    Err(Error::Other)
                }
            }
        }

        _start_test(test, result_checker).await
    }

    #[tokio::test]
    async fn reports_worker_capabilities() -> LibResult<()> {
        fn test(client_rcv: Receiver<ComputeResult>, port: Port) -> LibResult<()> {
            let frame = encode_transport_frame(TransportFrameKind::WorkerCapabilities, &[]);

            let end = Instant::now() + Duration::from_millis(150);
            while Instant::now() < end {
                match client_rcv.try_recv() {
                    Ok(Ok(_)) => return Ok(()),
                    Ok(Err(_)) => return Err(Error::Other),
                    _ => {}
                }

                if let Ok(mut stream) = connect_to_executor(port, false) {
                    stream.write_all(&frame).map_err(Error::OutputWrite)?;
                    let (kind, payload) = read_transport_response(&mut stream)?;
                    assert_eq!(kind, TransportFrameKind::WorkerCapabilities);
                    let capabilities = WorkerCapabilities::decode_wire(&payload)
                        .map_err(|err| Error::InvalidTransportFrame(err.to_string()))?;
                    assert!(!capabilities.worker_id.is_empty());
                    assert!(capabilities.max_tasks >= 1);
                    if let Ok(mut signal) = connect_to_executor(port, true) {
                        let _ = shutdown_msg(&mut signal);
                    }
                    return Ok(());
                }
            }

            Err(Error::Other)
        }

        fn result_checker(
            sender: Sender<ComputeResult>,
            result: LibResult<Signal>,
        ) -> LibResult<()> {
            match result {
                Ok(Signal::ShutDownGracefully) => Ok(()),
                Ok(_) => {
                    sender.send(Err(Error::Other));
                    Err(Error::Other)
                }
                Err(err) => {
                    sender.send(Err(Error::Other));
                    Err(err)
                }
            }
        }

        _start_test(test, result_checker).await
    }
}
