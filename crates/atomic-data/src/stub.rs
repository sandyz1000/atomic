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
        &self.descriptor.operation_id
    }

    /// The execution backend this artifact targets (Wasm or Docker).
    pub fn backend(&self) -> ExecutionBackend {
        self.descriptor.execution_backend
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
            .field("operation_id", &self.descriptor.operation_id)
            .field("backend", &self.descriptor.execution_backend)
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
