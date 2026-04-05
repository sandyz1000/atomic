use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::distributed::{ArtifactDescriptor, ArtifactManifest, DockerTaskPayload};
use crate::error::{BaseError, BaseResult};

#[derive(Debug, Clone)]
struct RegistryEntry {
    descriptor: ArtifactDescriptor,
    /// WASM: local module path override (replaces uri at resolve time).
    wasm_path: Option<String>,
    /// Docker: command args from the manifest entry.
    args: Option<Vec<String>>,
    /// Docker: static env vars from the manifest entry.
    env: Option<Vec<(String, String)>>,
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
                descriptor.op_id.clone(),
                RegistryEntry {
                    descriptor,
                    wasm_path: None,
                    args: None,
                    env: None,
                },
            )
            .map(|entry| entry.descriptor)
    }

    pub fn register_manifest(&mut self, manifest: ArtifactManifest) {
        for entry in manifest.wasm {
            self.entries.insert(
                entry.descriptor.op_id.clone(),
                RegistryEntry {
                    descriptor: entry.descriptor,
                    wasm_path: entry.module_path,
                    args: None,
                    env: None,
                },
            );
        }
        for entry in manifest.docker {
            let args = if entry.args.is_empty() { None } else { Some(entry.args) };
            let env = if entry.env.is_empty() { None } else { Some(entry.env) };
            self.entries.insert(
                entry.descriptor.op_id.clone(),
                RegistryEntry {
                    descriptor: entry.descriptor,
                    wasm_path: None,
                    args,
                    env,
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
        if let Some(module_path) = &entry.wasm_path {
            descriptor.uri = module_path.clone();
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
            command: entry.args.clone().unwrap_or_default(),
            env: entry.env.clone().unwrap_or_default(),
            work_dir: None,
            stdin: None,
            log_key: None,
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
        ArtifactDescriptor, ArtifactKind, ArtifactManifest, DockerManifestEntry,
        ExecutionBackend, ResourceProfile, RuntimeKind, WasmManifestEntry,
    };

    use super::TaskRegistry;

    fn wasm_descriptor(op_id: &str) -> ArtifactDescriptor {
        ArtifactDescriptor {
            op_id: op_id.into(),
            backend: ExecutionBackend::Wasm,
            kind: ArtifactKind::Wasm,
            uri: format!("oci://registry/atomic/{}@sha256:abc", op_id),
            entrypoint: "map_word_count".into(),
            runtime: RuntimeKind::Rust,
            digest: Some("sha256:abc".into()),
            build_target: Some("wasm32-wasip2".into()),
            profile: ResourceProfile {
                cpu_millis: 500,
                memory_mb: 256,
                timeout_ms: 30_000,
            },
        }
    }

    fn docker_descriptor(op_id: &str) -> ArtifactDescriptor {
        ArtifactDescriptor {
            op_id: op_id.into(),
            backend: ExecutionBackend::Docker,
            kind: ArtifactKind::Docker,
            uri: "registry/image@sha256:abc".into(),
            entrypoint: "/worker".into(),
            runtime: RuntimeKind::Rust,
            digest: None,
            build_target: None,
            profile: ResourceProfile { cpu_millis: 500, memory_mb: 256, timeout_ms: 30_000 },
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
            WasmManifestEntry::new(
                wasm_descriptor("wc.map.v1"),
                Some("target/wasm/map_words.wasm".into()),
            ),
            WasmManifestEntry::new(
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
        assert_eq!(descriptor.backend, ExecutionBackend::Wasm);
        assert_eq!(descriptor.build_target.as_deref(), Some("wasm32-wasip2"));

        let resolved = registry.resolve("wc.map.v1").expect("resolved descriptor must exist");
        assert_eq!(resolved.uri, "target/wasm/map_words.wasm");
    }

    #[test]
    fn loads_docker_manifest_entry() {
        let toml = r#"
schema_version = 1

[[docker_artifacts]]
image = "registry/image@sha256:abc"
args = ["/worker", "--op", "sum"]
env = [["OP", "sum"]]

[docker_artifacts.descriptor]
operation_id = "demo.sum.docker.v1"
execution_backend = "docker"
artifact_kind = "docker"
artifact_ref = "registry/image@sha256:abc"
entrypoint = "/worker"
runtime = "rust"

[docker_artifacts.descriptor.profile]
cpu_millis = 500
memory_mb = 256
timeout_ms = 5000
"#;
        let registry = TaskRegistry::from_toml_str(toml).expect("manifest should parse");
        assert!(registry.contains("demo.sum.docker.v1"));
        let descriptor = registry.get("demo.sum.docker.v1").unwrap();
        assert_eq!(descriptor.backend, ExecutionBackend::Docker);
    }

    #[test]
    fn resolve_docker_payload_returns_args() {
        let mut manifest = ArtifactManifest::new(vec![]);
        manifest.docker.push(DockerManifestEntry {
            descriptor: docker_descriptor("sum.v1"),
            image: "registry/image:latest".into(),
            args: vec!["/worker".into(), "--op".into(), "sum".into()],
            env: vec![("OP".into(), "sum".into())],
        });
        let registry = TaskRegistry::from_manifest(manifest);
        let payload = registry.resolve_docker_payload("sum.v1").expect("payload");
        assert_eq!(payload.command, vec!["/worker", "--op", "sum"]);
        assert_eq!(payload.env, vec![("OP".into(), "sum".into())]);
    }

    #[test]
    fn register_overwrites_existing_entry() {
        let mut registry = TaskRegistry::new();
        registry.register(wasm_descriptor("op.v1"));
        let prev = registry.register(wasm_descriptor("op.v1"));
        assert!(prev.is_some());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn resolve_missing_op_returns_none() {
        let registry = TaskRegistry::new();
        assert!(registry.resolve("nonexistent.op").is_none());
        assert!(registry.get("nonexistent.op").is_none());
    }
}
