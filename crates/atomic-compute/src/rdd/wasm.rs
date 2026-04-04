use crate::rdd::{BaseError, Data, TypedRdd};
use atomic_data::distributed::{RkyvWireSerializer, RkyvWireStrategy, RkyvWireValidator};
use rkyv::bytecheck::CheckBytes;

pub trait WasmBytesRddExt {
    fn collect_wasm(&self, operation_id: &str) -> Result<Vec<u8>, BaseError>;

    fn run_wasm_u32(&self, operation_id: &str) -> Result<Vec<u32>, BaseError>;

    fn run_wasm_u32_partitions(&self, operation_id: &str) -> Result<Vec<u32>, BaseError> {
        self.run_wasm_u32(operation_id)
    }
}

impl WasmBytesRddExt for TypedRdd<u8> {
    fn collect_wasm(&self, operation_id: &str) -> Result<Vec<u8>, BaseError> {
        let partitions = self
            .get_context()
            .run_registered_wasm_job(self.inner().clone(), operation_id)
            .map_err(|err| BaseError::Other(err.to_string()))?;
        let size = partitions.iter().map(Vec::len).sum();
        Ok(partitions
            .into_iter()
            .fold(Vec::with_capacity(size), |mut acc, partition| {
                acc.extend(partition);
                acc
            }))
    }

    fn run_wasm_u32(&self, operation_id: &str) -> Result<Vec<u32>, BaseError> {
        self.get_context()
            .run_registered_wasm_job(self.inner().clone(), operation_id)
            .map_err(|err| BaseError::Other(err.to_string()))?
            .into_iter()
            .map(|bytes| {
                let raw: [u8; 4] = bytes.try_into().map_err(|_| {
                    BaseError::Other(
                        "expected each wasm partition result to contain exactly 4 bytes"
                            .to_string(),
                    )
                })?;
                Ok(u32::from_le_bytes(raw))
            })
            .collect()
    }
}

pub trait WasmRddExt<T>
where
    T: Data + Clone,
{
    fn run_wasm<U>(&self, operation_id: &str) -> Result<Vec<U>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        U: rkyv::Archive,
        U::Archived:
            for<'a> CheckBytes<RkyvWireValidator<'a>> + rkyv::Deserialize<U, RkyvWireStrategy>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>;

    fn run_wasm_cfg<U, C>(&self, operation_id: &str, config: &C) -> Result<Vec<U>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        U: rkyv::Archive,
        U::Archived:
            for<'a> CheckBytes<RkyvWireValidator<'a>> + rkyv::Deserialize<U, RkyvWireStrategy>,
        C: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>;

    fn collect_wasm_rkyv<U>(&self, operation_id: &str) -> Result<Vec<U>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        Vec<U>: rkyv::Archive,
        <Vec<U> as rkyv::Archive>::Archived:
            for<'a> CheckBytes<RkyvWireValidator<'a>> + rkyv::Deserialize<Vec<U>, RkyvWireStrategy>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>;

    fn run_wasm_partitions_rkyv<U>(&self, operation_id: &str) -> Result<Vec<U>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        U: rkyv::Archive,
        U::Archived:
            for<'a> CheckBytes<RkyvWireValidator<'a>> + rkyv::Deserialize<U, RkyvWireStrategy>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        self.run_wasm(operation_id)
    }

    fn run_wasm_partitions_rkyv_with_config<U, C>(
        &self,
        operation_id: &str,
        config: &C,
    ) -> Result<Vec<U>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        U: rkyv::Archive,
        U::Archived:
            for<'a> CheckBytes<RkyvWireValidator<'a>> + rkyv::Deserialize<U, RkyvWireStrategy>,
        C: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        self.run_wasm_cfg(operation_id, config)
    }
}

impl<T> WasmRddExt<T> for TypedRdd<T>
where
    T: Data + Clone,
{
    fn run_wasm<U>(&self, operation_id: &str) -> Result<Vec<U>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        U: rkyv::Archive,
        U::Archived:
            for<'a> CheckBytes<RkyvWireValidator<'a>> + rkyv::Deserialize<U, RkyvWireStrategy>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        self.run_wasm_cfg(operation_id, &())
    }

    fn run_wasm_cfg<U, C>(&self, operation_id: &str, config: &C) -> Result<Vec<U>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        U: rkyv::Archive,
        U::Archived:
            for<'a> CheckBytes<RkyvWireValidator<'a>> + rkyv::Deserialize<U, RkyvWireStrategy>,
        C: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        self.get_context()
            .run_registered_wasm_job_rkyv::<T, U, C>(self.inner().clone(), operation_id, config)
            .map_err(|err| BaseError::Other(err.to_string()))
    }

    fn collect_wasm_rkyv<U>(&self, operation_id: &str) -> Result<Vec<U>, BaseError>
    where
        Vec<T>: for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
        Vec<U>: rkyv::Archive,
        <Vec<U> as rkyv::Archive>::Archived:
            for<'a> CheckBytes<RkyvWireValidator<'a>> + rkyv::Deserialize<Vec<U>, RkyvWireStrategy>,
        (): for<'a> rkyv::Serialize<RkyvWireSerializer<'a>>,
    {
        let partitions = self.run_wasm::<Vec<U>>(operation_id)?;
        let size = partitions.iter().map(Vec::len).sum();
        Ok(partitions
            .into_iter()
            .fold(Vec::with_capacity(size), |mut acc, partition| {
                acc.extend(partition);
                acc
            }))
    }
}
