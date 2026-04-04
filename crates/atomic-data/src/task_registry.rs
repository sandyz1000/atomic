use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::distributed::{ArtifactDescriptor, ArtifactManifest, DockerTaskPayload};
use crate::error::{BaseError, BaseResult};

#[derive(Debug, Clone)]
struct RegistryEntry {
    descriptor: ArtifactDescriptor,
    /// WASM: local module path override (replaces artifact_ref at resolve time).
    module_path: Option<String>,
    /// Docker: command args from the manifest entry.
    docker_args: Option<Vec<String>>,
    /// Docker: static env vars from the manifest entry.
    docker_env: Option<Vec<(String, String)>>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskRegistry {
    entries: HashMap<String, RegistryEntry>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Registers an operation id to an immutable artifact descriptor.
    pub fn register(&mut self, descriptor: ArtifactDescriptor) -> Option<ArtifactDescriptor> {
        self.entries
            .insert(
                descriptor.operation_id.clone(),
                RegistryEntry {
                    descriptor,
                    module_path: None,
                    docker_args: None,
                    docker_env: None,
                },
            )
            .map(|entry| entry.descriptor)
    }

    pub fn register_manifest(&mut self, manifest: ArtifactManifest) {
        for entry in manifest.wasm_artifacts {
            self.entries.insert(
                entry.descriptor.operation_id.clone(),
                RegistryEntry {
                    descriptor: entry.descriptor,
                    module_path: entry.module_path,
                    docker_args: None,
                    docker_env: None,
                },
            );
        }
        for entry in manifest.docker_artifacts {
            let args = if entry.args.is_empty() { None } else { Some(entry.args) };
            let env = if entry.env.is_empty() { None } else { Some(entry.env) };
            self.entries.insert(
                entry.descriptor.operation_id.clone(),
                RegistryEntry {
                    descriptor: entry.descriptor,
                    module_path: None,
                    docker_args: args,
                    docker_env: env,
                },
            );
        }
    }

    pub fn from_manifest(manifest: ArtifactManifest) -> Self {
        let mut registry = Self::new();
        registry.register_manifest(manifest);
        registry
    }

    pub fn from_toml_str(manifest: &str) -> BaseResult<Self> {
        let manifest = toml::from_str::<ArtifactManifest>(manifest)
            .map_err(|err| BaseError::Other(err.to_string()))?;
        Ok(Self::from_manifest(manifest))
    }

    pub fn load_toml_file(path: impl AsRef<Path>) -> BaseResult<Self> {
        let manifest = fs::read_to_string(path.as_ref())
            .map_err(|err| BaseError::Other(err.to_string()))?;
        Self::from_toml_str(&manifest)
    }

    pub fn get(&self, operation_id: &str) -> Option<&ArtifactDescriptor> {
        self.entries.get(operation_id).map(|entry| &entry.descriptor)
    }

    pub fn resolve(&self, operation_id: &str) -> Option<ArtifactDescriptor> {
        let entry = self.entries.get(operation_id)?;
        let mut descriptor = entry.descriptor.clone();
        if let Some(module_path) = &entry.module_path {
            descriptor.artifact_ref = module_path.clone();
        }
        Some(descriptor)
    }

    /// Returns the `DockerTaskPayload` template for a Docker artifact.
    ///
    /// The `stdin_data` field is left `None` — the caller fills it with the
    /// rkyv-encoded partition bytes before submitting the task.
    pub fn resolve_docker_payload(&self, operation_id: &str) -> Option<DockerTaskPayload> {
        let entry = self.entries.get(operation_id)?;
        Some(DockerTaskPayload {
            command: entry.docker_args.clone().unwrap_or_default(),
            env: entry.docker_env.clone().unwrap_or_default(),
            working_dir: None,
            stdin_data: None,
            log_stream_key: None,
        })
    }

    pub fn contains(&self, operation_id: &str) -> bool {
        self.entries.contains_key(operation_id)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use crate::distributed::{
        ArtifactDescriptor, ArtifactKind, ArtifactManifest, ExecutionBackend, ResourceProfile,
        RuntimeKind, WasmArtifactManifestEntry,
    };

    use super::TaskRegistry;

    fn wasm_descriptor(operation_id: &str) -> ArtifactDescriptor {
        ArtifactDescriptor {
            operation_id: operation_id.into(),
            execution_backend: ExecutionBackend::Wasm,
            artifact_kind: ArtifactKind::Wasm,
            artifact_ref: format!("oci://registry/atomic/{}@sha256:abc", operation_id),
            entrypoint: "map_word_count".into(),
            runtime: RuntimeKind::Rust,
            artifact_digest: Some("sha256:abc".into()),
            build_target: Some("wasm32-wasip2".into()),
            profile: ResourceProfile {
                cpu_millis: 500,
                memory_mb: 256,
                timeout_ms: 30_000,
            },
        }
    }

    #[test]
    fn registers_and_resolves_operation() {
        let mut registry = TaskRegistry::new();
        let descriptor = wasm_descriptor("wc.map.v1");

        registry.register(descriptor);
        assert!(registry.contains("wc.map.v1"));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn loads_registry_from_manifest() {
        let manifest = ArtifactManifest::new(vec![
            WasmArtifactManifestEntry::new(
                wasm_descriptor("wc.map.v1"),
                Some("target/wasm/map_words.wasm".into()),
            ),
            WasmArtifactManifestEntry::new(
                wasm_descriptor("wc.reduce.v1"),
                Some("target/wasm/reduce_words.wasm".into()),
            ),
        ]);

        let registry = TaskRegistry::from_manifest(manifest);
        assert!(registry.contains("wc.map.v1"));
        assert!(registry.contains("wc.reduce.v1"));
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn parses_registry_from_toml_manifest() {
        let manifest = r#"
schema_version = 1

[[wasm_artifacts]]
abi_version = 1
module_path = "target/wasm/map_words.wasm"

[wasm_artifacts.descriptor]
operation_id = "wc.map.v1"
execution_backend = "wasm"
artifact_kind = "wasm"
artifact_ref = "oci://registry/atomic/wc-map@sha256:abc"
entrypoint = "map_word_count"
runtime = "rust"
artifact_digest = "sha256:abc"
build_target = "wasm32-wasip2"

[wasm_artifacts.descriptor.profile]
cpu_millis = 500
memory_mb = 256
timeout_ms = 30000
"#;

        let registry = TaskRegistry::from_toml_str(manifest).expect("manifest should parse");
        let descriptor = registry.get("wc.map.v1").expect("descriptor must exist");
        assert_eq!(descriptor.execution_backend, ExecutionBackend::Wasm);
        assert_eq!(descriptor.build_target.as_deref(), Some("wasm32-wasip2"));

        let resolved = registry.resolve("wc.map.v1").expect("resolved descriptor must exist");
        assert_eq!(resolved.artifact_ref, "target/wasm/map_words.wasm");
    }
}
