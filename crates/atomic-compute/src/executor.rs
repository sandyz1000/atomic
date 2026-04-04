use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::backend::{BackendContext, WorkerRuntime};
use crate::env;
use crate::error::{Error, LibResult};
use atomic_data::distributed::{
    TaskEnvelope, TaskResultEnvelope, TransportFrameKind, WorkerCapabilities,
    TRANSPORT_HEADER_LEN, WireDecode, WireEncode, encode_transport_frame,
    parse_transport_header,
};

use crossbeam::{Receiver, Sender, channel::bounded};
use atomic_data::shuffle::error::NetworkError;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    stream::StreamExt,
    task::{spawn, spawn_blocking},
    time::delay_for,
};
use tokio_util::compat::{Tokio02AsyncReadCompatExt, Tokio02AsyncWriteCompatExt};



pub(crate) struct Executor {
    port: u16,
    worker_runtime: WorkerRuntime,
    backend_ctx: BackendContext,
}

impl Executor {
    pub fn new(port: u16) -> Self {
        let conf = env::Configuration::get();
        let worker_runtime = conf
            .slave
            .as_ref()
            .map(|slave| WorkerRuntime::from_execution_backend(slave.backend))
            .unwrap_or_else(|| {
                WorkerRuntime::default_for(conf.deployment_mode == env::DeploymentMode::Distributed)
            });
        Executor {
            port,
            worker_runtime,
            backend_ctx: BackendContext {
                worker_id: Arc::from(format!("worker-{}", port)),
            },
        }
    }

    /// Executes the artifact-first distributed envelope protocol.
    pub fn execute_distributed_task_envelope(
        &self,
        task: &TaskEnvelope,
    ) -> LibResult<TaskResultEnvelope> {
        self.worker_runtime.execute(&self.backend_ctx, task)
    }

    pub fn worker_capabilities(&self) -> WorkerCapabilities {
        let max_concurrent_tasks = env::Configuration::get()
            .slave
            .as_ref()
            .map(|slave| slave.max_concurrent_tasks)
            .unwrap_or(1);
        self.worker_runtime
            .capabilities(self.backend_ctx.worker_id.to_string(), max_concurrent_tasks)
    }

    /// Worker which spawns threads for received tasks, deserializes them,
    /// executes the task and sends the result back to the master.
    ///
    /// This will spawn it's own Tokio runtime to run the tasks on.
    #[allow(clippy::drop_copy)]
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

    #[allow(clippy::drop_copy)]
    async fn process_stream(self: Arc<Self>, rcv_main: Receiver<Signal>) -> LibResult<Signal> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let mut listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        while let Some(Ok(mut stream)) = listener.incoming().next().await {
            let rcv_main = rcv_main.clone();
            let selfc = Arc::clone(&self);
            let res: LibResult<Signal> = spawn(async move {
                match rcv_main.try_recv() {
                    Ok(Signal::ShutDownError) => {
                        log::info!("shutting down executor @{} due to error", selfc.port);
                        return Err(Error::ExecutorShutdown);
                    }
                    Ok(Signal::ShutDownGracefully) => {
                        log::info!("shutting down executor @{} gracefully", selfc.port);
                        return Ok(Signal::ShutDownGracefully);
                    }
                    _ => {}
                }
                log::debug!("received new task @{} executor", selfc.port);
                let (frame_kind, payload) = selfc.read_transport_frame(&mut stream).await?;
                let (result_kind, result_payload) = spawn_blocking(move || {
                    selfc.handle_transport_frame(frame_kind, payload)
                })
                .await??;
                selfc
                    .write_transport_frame(&mut stream, result_kind, &result_payload)
                    .await?;
                log::debug!("sent result data to driver");
                Ok(Signal::Continue)
            })
            .await?;
            match res {
                Ok(Signal::Continue) => continue,
                Ok(s) => return Ok(s),
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
                let result = self.execute_distributed_task_envelope(&task)?;
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

    /// A listener for exit signal from master to end the whole slave process.
    async fn signal_handler(self: Arc<Self>, send_child: Sender<Signal>) -> LibResult<Signal> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.port + 10));
        log::debug!("signal handler port open @ {}", addr.port());
        let mut listener = TcpListener::bind(addr)
            .await
            .map_err(NetworkError::TcpListener)?;
        let mut signal: LibResult<Signal> = Err(Error::ExecutorShutdown);
        while let Some(Ok(stream)) = listener.incoming().next().await {
            let stream = stream.compat();
            let mut stream_std = stream.into_inner();
            // Deserialize signal directly with bincode
            let data = bincode::deserialize_from::<_, Signal>(&mut stream_std)?;
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
        // give some time to the executor threads to shut down hopefully
        delay_for(Duration::from_millis(1_000)).await;
        signal
    }
}

pub fn run_worker_from_config() -> LibResult<()> {
    let port = env::Configuration::get()
        .slave
        .as_ref()
        .map(|slave| slave.port)
        .ok_or(Error::GetOrCreateConfig(
            "worker port not configured for slave deployment",
        ))?;
    let executor = Arc::new(Executor::new(port));
    executor.worker().map(|_| ())
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) enum Signal {
    ShutDownError,
    ShutDownGracefully,
    Continue,
}

#[cfg(test)]
mod tests {
    #![allow(unused_must_use)]

    use super::*;
    use core::time;
    use atomic_data::distributed::{
        TransportFrameKind, TRANSPORT_HEADER_LEN, encode_transport_frame,
        parse_transport_header,
    };
    use crossbeam::channel::{Receiver, Sender, unbounded};
    use atomic_utils::get_dynamic_port;
    use std::io::Write;
    use std::thread;
    use std::time::Duration;

    type Port = u16;
    type ComputeResult = std::result::Result<(), ()>;

    fn initialize_exec() -> Arc<Executor> {
        let port = get_dynamic_port();
        Arc::new(Executor::new(port))
    }

    fn connect_to_executor(mut port: u16, signal_handler: bool) -> LibResult<std::net::TcpStream> {
        use std::net::TcpStream;

        let mut i: usize = 0;
        if signal_handler {
            // connect to signal handling port
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

    fn send_shutdown_signal_msg(stream: &mut std::net::TcpStream) -> LibResult<()> {
        // Serialize signal directly with bincode
        bincode::serialize_into(stream, &Signal::ShutDownGracefully)?;
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
                    send_shutdown_signal_msg(&mut stream)?;
                    return Ok(());
                }
                thread::sleep(time::Duration::from_millis(5));
            }
            Err(Error::Other)
        }

        fn result_checker(sender: Sender<ComputeResult>, result: LibResult<Signal>) -> LibResult<()> {
            match result {
                Ok(Signal::ShutDownGracefully) => {
                    sender.send(Ok(()));
                    Ok(())
                }
                Ok(_) | Err(_) => {
                    sender.send(Err(()));
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
                    assert!(capabilities.max_concurrent_tasks >= 1);
                    return Ok(());
                }
            }

            Err(Error::Other)
        }

        fn result_checker(sender: Sender<ComputeResult>, result: LibResult<Signal>) -> LibResult<()> {
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
