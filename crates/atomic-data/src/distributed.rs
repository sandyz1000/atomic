use crate::error::{BaseError, BaseResult};
use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize, de::pooling::Pool,
    rancor::Error, ser::allocator::ArenaHandle, util::AlignedVec,
};
use serde::{Deserialize, Serialize};

pub type RkyvWireSerializer<'a> =
    rkyv::api::high::HighSerializer<AlignedVec, ArenaHandle<'a>, Error>;
pub type RkyvWireValidator<'a> = rkyv::api::high::HighValidator<'a, Error>;
pub type RkyvWireStrategy = rkyv::rancor::Strategy<Pool, Error>;

/// Semantic version for wire contracts used by distributed task transport.
pub const WIRE_SCHEMA_V1: u16 = 1;
pub const TRANSPORT_FRAME_MAGIC: [u8; 4] = *b"ATOM";
pub const TRANSPORT_FRAME_VERSION_V1: u8 = 1;
pub const TRANSPORT_HEADER_LEN: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportFrameKind {
    TaskEnvelope = 3,
    TaskResultEnvelope = 4,
    WorkerCapabilities = 5,
}

impl TryFrom<u8> for TransportFrameKind {
    type Error = BaseError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            3 => Ok(Self::TaskEnvelope),
            4 => Ok(Self::TaskResultEnvelope),
            5 => Ok(Self::WorkerCapabilities),
            _ => Err(BaseError::Other(format!(
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
) -> BaseResult<(TransportFrameKind, usize)> {
    if header[..4] != TRANSPORT_FRAME_MAGIC {
        return Err(BaseError::Other(
            "invalid transport frame magic".to_string(),
        ));
    }

    if header[4] != TRANSPORT_FRAME_VERSION_V1 {
        return Err(BaseError::Other(format!(
            "unsupported transport frame version: {}",
            header[4]
        )));
    }

    let kind = TransportFrameKind::try_from(header[5])?;
    let payload_len = u32::from_be_bytes([header[6], header[7], header[8], header[9]]) as usize;
    Ok((kind, payload_len))
}

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
        rkyv::to_bytes::<Error>(self)
            .map(|bytes| bytes.to_vec())
            .map_err(|err| BaseError::Other(err.to_string()))
    }
}

impl<T> WireDecode for T
where
    T: Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<RkyvWireValidator<'a>>
        + RkyvDeserialize<T, RkyvWireStrategy>,
{
    fn decode_wire(bytes: &[u8]) -> BaseResult<Self> {
        rkyv::from_bytes::<T, Error>(bytes).map_err(|err| BaseError::Other(err.to_string()))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionBackend {
    LocalThread,
    Docker,
    Wasm,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum ArtifactKind {
    Docker,
    Wasm,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeKind {
    Rust,
    Python,
    JavaScript,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum ResultStatus {
    Success,
    RetryableFailure,
    FatalFailure,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum WasmValueEncoding {
    RawBytes,
    Rkyv,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize,
)]
pub struct ResourceProfile {
    pub cpu_millis: u32,
    pub memory_mb: u32,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct DockerTaskPayload {
    pub command: Vec<String>,
    pub env: Vec<(String, String)>,
    pub working_dir: Option<String>,
    pub stdin_data: Option<Vec<u8>>,
    pub log_stream_key: Option<String>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize,
)]
pub struct WasmTaskPayload {
    pub abi_version: u16,
    pub config_encoding: WasmValueEncoding,
    pub config_payload: Vec<u8>,
    pub partition_encoding: WasmValueEncoding,
    pub result_encoding: WasmValueEncoding,
}

impl WasmTaskPayload {
    pub fn new(config_payload: Vec<u8>) -> Self {
        Self {
            abi_version: WIRE_SCHEMA_V1,
            config_encoding: WasmValueEncoding::Rkyv,
            config_payload,
            partition_encoding: WasmValueEncoding::Rkyv,
            result_encoding: WasmValueEncoding::Rkyv,
        }
    }

    pub fn with_encodings(
        config_encoding: WasmValueEncoding,
        config_payload: Vec<u8>,
        partition_encoding: WasmValueEncoding,
        result_encoding: WasmValueEncoding,
    ) -> Self {
        Self {
            abi_version: WIRE_SCHEMA_V1,
            config_encoding,
            config_payload,
            partition_encoding,
            result_encoding,
        }
    }

    pub fn raw_bytes() -> Self {
        Self::with_encodings(
            WasmValueEncoding::RawBytes,
            Vec::new(),
            WasmValueEncoding::RawBytes,
            WasmValueEncoding::RawBytes,
        )
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize,
)]
pub struct FoldActionConfig<T> {
    pub zero: T,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize,
)]
pub struct AggregateActionConfig<U> {
    pub zero: U,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize, Serialize, Deserialize,
)]
pub struct ArtifactDescriptor {
    /// Stable identifier mapped in driver and worker registries.
    pub operation_id: String,
    /// The scheduler picks one backend for the task instead of probing workers at runtime.
    pub execution_backend: ExecutionBackend,
    pub artifact_kind: ArtifactKind,
    /// Registry reference. Examples:
    /// - Docker: `registry/repo/image@sha256:...`
    /// - WASM: `oci://registry/repo/module@sha256:...`
    pub artifact_ref: String,
    /// Entrypoint inside the artifact (exported wasm fn or container command id).
    pub entrypoint: String,
    pub runtime: RuntimeKind,
    /// Immutable artifact digest that ties task placement to a build output.
    pub artifact_digest: Option<String>,
    /// Build target used to produce the artifact, such as `wasm32-wasip2`.
    pub build_target: Option<String>,
    pub profile: ResourceProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactManifest {
    pub schema_version: u16,
    pub wasm_artifacts: Vec<WasmArtifactManifestEntry>,
    /// Docker artifacts listed in this manifest. Defaults to empty when absent in TOML.
    #[serde(default)]
    pub docker_artifacts: Vec<DockerArtifactManifestEntry>,
}

impl ArtifactManifest {
    pub fn new(wasm_artifacts: Vec<WasmArtifactManifestEntry>) -> Self {
        Self {
            schema_version: WIRE_SCHEMA_V1,
            wasm_artifacts,
            docker_artifacts: Vec::new(),
        }
    }
}

/// A Docker artifact entry in a manifest file.
///
/// Example TOML:
/// ```toml
/// [[docker_artifacts]]
/// image = "registry/repo/image@sha256:abc123"
/// args = ["/usr/local/bin/worker", "--op", "map"]
/// env = [["OPERATION", "map"]]
///
/// [docker_artifacts.descriptor]
/// operation_id = "demo.map.docker.v1"
/// execution_backend = "docker"
/// artifact_kind = "docker"
/// artifact_ref = "registry/repo/image@sha256:abc123"
/// entrypoint = "/usr/local/bin/worker"
/// runtime = "rust"
/// artifact_digest = "sha256:abc123"
/// build_target = "x86_64-unknown-linux-musl"
///
/// [docker_artifacts.descriptor.profile]
/// cpu_millis = 500
/// memory_mb = 256
/// timeout_ms = 5000
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DockerArtifactManifestEntry {
    pub descriptor: ArtifactDescriptor,
    /// Full image reference, e.g. `registry/repo/image@sha256:...`
    pub image: String,
    /// Command to run inside the container. Partition bytes arrive via stdin.
    #[serde(default)]
    pub args: Vec<String>,
    /// Static environment variables baked into the manifest.
    #[serde(default)]
    pub env: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmArtifactManifestEntry {
    pub descriptor: ArtifactDescriptor,
    pub abi_version: u16,
    pub module_path: Option<String>,
}

impl WasmArtifactManifestEntry {
    pub fn new(descriptor: ArtifactDescriptor, module_path: Option<String>) -> Self {
        Self {
            descriptor,
            abi_version: WIRE_SCHEMA_V1,
            module_path,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct TaskEnvelope {
    pub schema_version: u16,
    pub run_id: usize,
    pub stage_id: usize,
    pub task_id: usize,
    pub attempt_id: usize,
    pub partition_id: usize,
    /// Correlates scheduler logs and worker logs.
    pub trace_id: String,
    pub artifact: ArtifactDescriptor,
    /// Values that closures would normally capture, encoded by the driver.
    pub payload: Vec<u8>,
    /// Serialized partition bytes.
    pub partition_data: Vec<u8>,
}

impl TaskEnvelope {
    pub fn new(
        run_id: usize,
        stage_id: usize,
        task_id: usize,
        attempt_id: usize,
        partition_id: usize,
        trace_id: String,
        artifact: ArtifactDescriptor,
        payload: Vec<u8>,
        partition_data: Vec<u8>,
    ) -> Self {
        Self {
            schema_version: WIRE_SCHEMA_V1,
            run_id,
            stage_id,
            task_id,
            attempt_id,
            partition_id,
            trace_id,
            artifact,
            payload,
            partition_data,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct TaskResultEnvelope {
    pub schema_version: u16,
    pub run_id: usize,
    pub stage_id: usize,
    pub task_id: usize,
    pub attempt_id: usize,
    pub status: ResultStatus,
    pub result_data: Vec<u8>,
    pub error_message: Option<String>,
    pub worker_id: String,
}

impl TaskResultEnvelope {
    pub fn ok(
        run_id: usize,
        stage_id: usize,
        task_id: usize,
        attempt_id: usize,
        worker_id: String,
        result_data: Vec<u8>,
    ) -> Self {
        Self {
            schema_version: WIRE_SCHEMA_V1,
            run_id,
            stage_id,
            task_id,
            attempt_id,
            status: ResultStatus::Success,
            result_data,
            error_message: None,
            worker_id,
        }
    }

    pub fn retryable_failure(
        run_id: usize,
        stage_id: usize,
        task_id: usize,
        attempt_id: usize,
        worker_id: String,
        error_message: String,
        result_data: Vec<u8>,
    ) -> Self {
        Self {
            schema_version: WIRE_SCHEMA_V1,
            run_id,
            stage_id,
            task_id,
            attempt_id,
            status: ResultStatus::RetryableFailure,
            result_data,
            error_message: Some(error_message),
            worker_id,
        }
    }

    pub fn fatal_failure(
        run_id: usize,
        stage_id: usize,
        task_id: usize,
        attempt_id: usize,
        worker_id: String,
        error_message: String,
    ) -> Self {
        Self {
            schema_version: WIRE_SCHEMA_V1,
            run_id,
            stage_id,
            task_id,
            attempt_id,
            status: ResultStatus::FatalFailure,
            result_data: Vec::new(),
            error_message: Some(error_message),
            worker_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct WorkerCapabilities {
    pub schema_version: u16,
    pub worker_id: String,
    pub execution_backend: ExecutionBackend,
    pub supported_artifacts: Vec<ArtifactKind>,
    pub supported_runtimes: Vec<RuntimeKind>,
    pub max_concurrent_tasks: u16,
}

impl WorkerCapabilities {
    pub fn supports(&self, artifact: &ArtifactDescriptor) -> bool {
        self.execution_backend == artifact.execution_backend
            && self.supported_artifacts.contains(&artifact.artifact_kind)
            && self.supported_runtimes.contains(&artifact.runtime)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wasm_descriptor() -> ArtifactDescriptor {
        ArtifactDescriptor {
            operation_id: "map.words.v1".to_string(),
            execution_backend: ExecutionBackend::Wasm,
            artifact_kind: ArtifactKind::Wasm,
            artifact_ref: "oci://registry/atomic/map-words@sha256:abc".to_string(),
            entrypoint: "map_words".to_string(),
            runtime: RuntimeKind::Rust,
            artifact_digest: Some("sha256:abc".to_string()),
            build_target: Some("wasm32-wasip2".to_string()),
            profile: ResourceProfile {
                cpu_millis: 250,
                memory_mb: 128,
                timeout_ms: 5_000,
            },
        }
    }

    #[test]
    fn wasm_payload_round_trips_with_rkyv() {
        let payload = WasmTaskPayload::new(vec![1, 2, 3, 4]);
        let bytes = payload.encode_wire().expect("serialize payload");
        let decoded = WasmTaskPayload::decode_wire(&bytes).expect("deserialize payload");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn manifest_entry_keeps_wasm_descriptor_metadata() {
        let entry = WasmArtifactManifestEntry::new(
            wasm_descriptor(),
            Some("target/wasm-artifacts/map_words.wasm".to_string()),
        );

        assert_eq!(entry.abi_version, WIRE_SCHEMA_V1);
        assert_eq!(entry.descriptor.execution_backend, ExecutionBackend::Wasm);
        assert_eq!(entry.descriptor.artifact_kind, ArtifactKind::Wasm);
        assert_eq!(
            entry.module_path.as_deref(),
            Some("target/wasm-artifacts/map_words.wasm")
        );
    }

    #[test]
    fn rkyv_codec_round_trips_partition_values() {
        let encoded = vec![1_u32, 2, 3].encode_wire().expect("serialize values");
        let decoded = Vec::<u32>::decode_wire(&encoded).expect("deserialize values");
        assert_eq!(decoded, vec![1, 2, 3]);
    }

    #[test]
    fn legacy_frame_ids_are_rejected() {
        assert!(TransportFrameKind::try_from(1).is_err());
        assert!(TransportFrameKind::try_from(2).is_err());
    }
}
