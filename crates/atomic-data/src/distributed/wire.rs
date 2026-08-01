use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize, rancor::Error};

use super::{RkyvWireSerializer, RkyvWireStrategy, RkyvWireValidator};
use crate::error::DataResult;

/// Encodes a value into the distributed wire format.
pub trait WireEncode {
    fn encode_wire(&self) -> DataResult<Vec<u8>>;
}

/// Decodes a value from the distributed wire format.
pub trait WireDecode: Sized {
    fn decode_wire(bytes: &[u8]) -> DataResult<Self>;
}

impl<T> WireEncode for T
where
    T: for<'a> RkyvSerialize<RkyvWireSerializer<'a>>,
{
    fn encode_wire(&self) -> DataResult<Vec<u8>> {
        Ok(rkyv::to_bytes::<Error>(self)?.to_vec())
    }
}

impl<T> WireDecode for T
where
    T: Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
        + RkyvDeserialize<T, RkyvWireStrategy>,
{
    fn decode_wire(bytes: &[u8]) -> DataResult<Self> {
        Ok(rkyv::from_bytes::<T, Error>(bytes)?)
    }
}

/// Bound bundle exposing the raw rkyv supertraits needed for encode + decode.
///
/// Unlike [`WireEncode`]/[`WireDecode`] (blanket-impl'd, so their underlying
/// rkyv bounds are invisible to the compiler), `WireSerde`'s supertraits are
/// the raw `Archive + Serialize + …` bounds. This lets the compiler derive
/// composed-type bounds automatically: `K: WireSerde` + `V: WireSerde`
/// ⇒ `Vec<(K, V)>: WireEncode + WireDecode`.
///
/// Use `WireSerde` wherever a generic type participates in composed-type
/// serialization (shuffle buckets, partitioner bounds, disk cache). Use the
/// simpler `WireEncode`/`WireDecode` when the type is encoded/decoded on its
/// own (envelope structs, checkpoint payloads) and composition is unnecessary.
pub trait WireSerde:
    Archive<
        Archived: for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
            + RkyvDeserialize<Self, RkyvWireStrategy>,
    > + for<'a> RkyvSerialize<RkyvWireSerializer<'a>>
    + Sized
{
}

impl<T> WireSerde for T
where
    T: Archive + for<'a> RkyvSerialize<RkyvWireSerializer<'a>>,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
        + RkyvDeserialize<T, RkyvWireStrategy>,
{
}
