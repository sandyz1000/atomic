use crate::error::{DataError, DataResult};

pub const TRANSPORT_FRAME_MAGIC: [u8; 4] = *b"ATOM";
pub const TRANSPORT_FRAME_VERSION_V1: u8 = 1;
pub const TRANSPORT_HEADER_LEN: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportFrameKind {
    TaskEnvelope = 3,
    TaskResultEnvelope = 4,
    WorkerCapabilities = 5,
    /// Connection-opening auth frame; payload is the raw cluster token bytes.
    /// Required as the first frame when the receiver has an auth token configured.
    Auth = 6,
}

impl TryFrom<u8> for TransportFrameKind {
    type Error = DataError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            3 => Ok(Self::TaskEnvelope),
            4 => Ok(Self::TaskResultEnvelope),
            5 => Ok(Self::WorkerCapabilities),
            6 => Ok(Self::Auth),
            _ => Err(DataError::Other(format!(
                "unknown transport frame kind: {}",
                value
            ))),
        }
    }
}

pub fn encode_transport_frame(kind: TransportFrameKind, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(TRANSPORT_HEADER_LEN + payload.len());
    frame.extend_from_slice(&TRANSPORT_FRAME_MAGIC);
    frame.push(TRANSPORT_FRAME_VERSION_V1);
    frame.push(kind as u8);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

pub fn parse_transport_header(
    header: &[u8; TRANSPORT_HEADER_LEN],
) -> DataResult<(TransportFrameKind, usize)> {
    if header[..4] != TRANSPORT_FRAME_MAGIC {
        return Err(DataError::Other(
            "invalid transport frame magic".to_string(),
        ));
    }

    if header[4] != TRANSPORT_FRAME_VERSION_V1 {
        return Err(DataError::Other(format!(
            "unsupported transport frame version: {}",
            header[4]
        )));
    }

    let kind = TransportFrameKind::try_from(header[5])?;
    let payload_len = u32::from_be_bytes([header[6], header[7], header[8], header[9]]) as usize;
    Ok((kind, payload_len))
}
