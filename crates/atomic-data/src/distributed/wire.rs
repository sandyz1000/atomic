use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize, rancor::Error};

use super::{RkyvWireSerializer, RkyvWireStrategy, RkyvWireValidator};
use crate::error::BaseResult;

/// Encodes a value into the distributed wire format.
pub trait WireEncode {
    fn encode_wire(&self) -> BaseResult<Vec<u8>>;
}

/// Decodes a value from the distributed wire format.
pub trait WireDecode: Sized {
    fn decode_wire(bytes: &[u8]) -> BaseResult<Self>;
}

impl<T> WireEncode for T
where
    T: for<'a> RkyvSerialize<RkyvWireSerializer<'a>>,
{
    fn encode_wire(&self) -> BaseResult<Vec<u8>> {
        Ok(rkyv::to_bytes::<Error>(self)?.to_vec())
    }
}

impl<T> WireDecode for T
where
    T: Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
        + RkyvDeserialize<T, RkyvWireStrategy>,
{
    fn decode_wire(bytes: &[u8]) -> BaseResult<Self> {
        Ok(rkyv::from_bytes::<T, Error>(bytes)?)
    }
}
