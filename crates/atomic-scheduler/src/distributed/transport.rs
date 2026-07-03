use std::net::SocketAddrV4;

use atomic_data::distributed::{
    TRANSPORT_HEADER_LEN, TaskEnvelope, TaskResultEnvelope, TransportFrameKind, WireDecode,
    WireEncode, encode_transport_frame, parse_transport_header,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::error::{LibResult, SchedulerError};

use super::DistributedScheduler;

impl DistributedScheduler {
    pub async fn submit_task_to_worker(
        &self,
        task: &TaskEnvelope,
        target: SocketAddrV4,
    ) -> LibResult<TaskResultEnvelope> {
        let mut stream = TcpStream::connect(target).await?;
        if let Some(token) = atomic_data::env::get_auth_token() {
            Self::write_transport_frame(&mut stream, TransportFrameKind::Auth, token.as_bytes())
                .await?;
        }
        let payload = task.encode_wire()?;
        Self::write_transport_frame(&mut stream, TransportFrameKind::TaskEnvelope, &payload)
            .await?;
        let (kind, payload) = Self::read_transport_frame(&mut stream).await?;
        if kind != TransportFrameKind::TaskResultEnvelope {
            return Err(SchedulerError::Transport(format!(
                "unexpected response frame kind: {:?}",
                kind
            )));
        }
        Ok(TaskResultEnvelope::decode_wire(&payload)?)
    }

    async fn write_transport_frame(
        stream: &mut TcpStream,
        frame_kind: TransportFrameKind,
        payload: &[u8],
    ) -> LibResult<()> {
        let frame = encode_transport_frame(frame_kind, payload);
        stream.write_all(&frame).await?;
        Ok(())
    }

    async fn read_transport_frame(
        stream: &mut TcpStream,
    ) -> LibResult<(TransportFrameKind, Vec<u8>)> {
        let mut header = [0_u8; TRANSPORT_HEADER_LEN];
        stream.read_exact(&mut header).await?;
        let (kind, payload_len) = parse_transport_header(&header)?;
        let mut payload = vec![0_u8; payload_len];
        stream.read_exact(&mut payload).await?;
        Ok((kind, payload))
    }
}
