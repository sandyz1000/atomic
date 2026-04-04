use std::marker::PhantomData;
use std::path::Path;

use crate::distributed::{ArtifactDescriptor, ExecutionBackend};
use crate::error::{BaseError, BaseResult};
use crate::task_registry::TaskRegistry;

/// A compile-time typed reference to a prebuilt artifact (WASM or Docker).
///
/// `Input` is the type of RDD elements the artifact expects per partition.
/// `Output` is the type the artifact produces per partition.
///
/// The stub carries no closure logic — all execution happens inside the prebuilt binary.
/// `PhantomData<fn(Input) -> Output>` is `Send + Sync` unconditionally and costs nothing at runtime.
///
/// # Example
/// ```ignore
/// let map_stub: WasmStub<u8, u32> = WasmStub::from_manifest("build/manifest.toml", "demo.map.v1")?;
/// let result: Vec<u32> = rdd.map_via(&map_stub)?;
/// ```
pub struct ArtifactStub<Input, Output> {
    pub descriptor: ArtifactDescriptor,
    _phantom: PhantomData<fn(Input) -> Output>,
}

impl<I, O> ArtifactStub<I, O> {
    /// Construct a stub from a resolved artifact descriptor.
    pub fn new(descriptor: ArtifactDescriptor) -> Self {
        Self {
            descriptor,
            _phantom: PhantomData,
        }
    }

    /// Load a stub from a manifest TOML file by operation id.
    ///
    /// Returns an error if the manifest cannot be read or the operation id is not found.
    pub fn from_manifest(path: impl AsRef<Path>, operation_id: &str) -> BaseResult<Self> {
        let registry = TaskRegistry::load_toml_file(path)?;
        let descriptor = registry.resolve(operation_id).ok_or_else(|| {
            BaseError::Other(format!(
                "operation '{}' not found in manifest",
                operation_id
            ))
        })?;
        Ok(Self::new(descriptor))
    }

    /// The stable operation id for this artifact.
    pub fn operation_id(&self) -> &str {
        &self.descriptor.op_id
    }

    /// The execution backend this artifact targets (Wasm or Docker).
    pub fn backend(&self) -> ExecutionBackend {
        self.descriptor.backend
    }
}

impl<I, O> Clone for ArtifactStub<I, O> {
    fn clone(&self) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<I, O> std::fmt::Debug for ArtifactStub<I, O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArtifactStub")
            .field("operation_id", &self.descriptor.op_id)
            .field("backend", &self.descriptor.backend)
            .finish()
    }
}

/// Type-safe stub for a WASM task artifact.
///
/// Calling `.map_via(&stub)` on a `TypedRdd<I>` will execute the WASM module
/// and return `Vec<O>`. The compiler verifies that the RDD element type matches `I`.
pub type WasmStub<I, O> = ArtifactStub<I, O>;

/// Type-safe stub for a Docker task artifact.
///
/// The container receives rkyv-encoded partition bytes on stdin and writes
/// rkyv-encoded results to stdout. Same call-site API as `WasmStub`.
pub type DockerStub<I, O> = ArtifactStub<I, O>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::{ArtifactKind, ResourceProfile, RuntimeKind};

    fn wasm_descriptor() -> ArtifactDescriptor {
        ArtifactDescriptor {
            op_id: "test.op.v1".into(),
            backend: ExecutionBackend::Wasm,
            kind: ArtifactKind::Wasm,
            uri: "oci://test@sha256:abc".into(),
            entrypoint: "run_map".into(),
            runtime: RuntimeKind::Rust,
            digest: Some("sha256:abc".into()),
            build_target: Some("wasm32-unknown-unknown".into()),
            profile: ResourceProfile { cpu_millis: 100, memory_mb: 64, timeout_ms: 1000 },
        }
    }

    fn docker_descriptor() -> ArtifactDescriptor {
        ArtifactDescriptor {
            op_id: "test.docker.v1".into(),
            backend: ExecutionBackend::Docker,
            kind: ArtifactKind::Docker,
            uri: "registry/image@sha256:abc".into(),
            entrypoint: "/worker".into(),
            runtime: RuntimeKind::Rust,
            digest: None,
            build_target: None,
            profile: ResourceProfile { cpu_millis: 500, memory_mb: 256, timeout_ms: 5000 },
        }
    }

    #[test]
    fn stub_new_stores_descriptor() {
        let stub: ArtifactStub<u8, u8> = ArtifactStub::new(wasm_descriptor());
        assert_eq!(stub.descriptor.op_id, "test.op.v1");
    }

    #[test]
    fn stub_operation_id_returns_op_id() {
        let stub: ArtifactStub<u8, u8> = ArtifactStub::new(wasm_descriptor());
        assert_eq!(stub.operation_id(), "test.op.v1");
    }

    #[test]
    fn stub_backend_returns_wasm() {
        let stub: ArtifactStub<u8, u8> = ArtifactStub::new(wasm_descriptor());
        assert_eq!(stub.backend(), ExecutionBackend::Wasm);
    }

    #[test]
    fn docker_stub_backend_returns_docker() {
        let stub: DockerStub<u8, u8> = DockerStub::new(docker_descriptor());
        assert_eq!(stub.backend(), ExecutionBackend::Docker);
    }

    #[test]
    fn stub_clone_is_independent() {
        let stub: ArtifactStub<u8, u8> = ArtifactStub::new(wasm_descriptor());
        let cloned = stub.clone();
        assert_eq!(stub.operation_id(), cloned.operation_id());
        assert_eq!(stub.backend(), cloned.backend());
    }

    #[test]
    fn stub_debug_contains_op_id() {
        let stub: ArtifactStub<u8, u8> = ArtifactStub::new(wasm_descriptor());
        let debug = format!("{:?}", stub);
        assert!(debug.contains("test.op.v1"));
    }

    #[test]
    fn from_manifest_missing_file_returns_err() {
        let result = ArtifactStub::<u8, u8>::from_manifest("/nonexistent/path.toml", "any.op");
        assert!(result.is_err());
    }

    #[test]
    fn from_manifest_missing_op_id_returns_err() {
        let toml = r#"
schema_version = 1
[[wasm_artifacts]]
abi_version = 1
module_path = "/tmp/foo.wasm"
[wasm_artifacts.descriptor]
operation_id = "real.op.v1"
execution_backend = "wasm"
artifact_kind = "wasm"
artifact_ref = "/tmp/foo.wasm"
entrypoint = "run"
runtime = "rust"
[wasm_artifacts.descriptor.profile]
cpu_millis = 100
memory_mb = 64
timeout_ms = 1000
"#;
        let dir = std::env::temp_dir();
        let path = dir.join("atomic_stub_test_manifest.toml");
        std::fs::write(&path, toml).expect("write temp manifest");
        let result = ArtifactStub::<u8, u8>::from_manifest(&path, "missing.op");
        assert!(result.is_err());
    }
}
