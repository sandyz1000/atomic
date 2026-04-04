use std::sync::Arc;
use std::{fs, path::PathBuf, time::Duration};

use atomic_data::distributed::{
    ArtifactKind, DockerTaskPayload, ExecutionBackend, RuntimeKind, TaskEnvelope,
    TaskResultEnvelope, WIRE_SCHEMA_V1, WasmTaskPayload, WasmValueEncoding, WireDecode, WireEncode,
    WorkerCapabilities,
};
use bollard::Docker;
use bollard::container::AttachContainerResults;
use bollard::models::ContainerCreateBody;
use bollard::query_parameters::{
    AttachContainerOptionsBuilder, CreateImageOptionsBuilder, ListImagesOptionsBuilder,
    LogsOptionsBuilder, RemoveContainerOptionsBuilder,
};
use dashmap::DashMap;
use futures::TryStreamExt;
use rkyv::from_bytes;
use sha2::{Digest, Sha256};
use tokio::time::timeout;
use wasmtime::{Engine, Instance, Memory, Module, Store, TypedFunc};

use crate::error::{Error, LibResult};

#[derive(Debug, Clone)]
pub struct BackendContext {
    pub worker_id: Arc<str>,
}

pub trait WorkerExecutionBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn exec_backend(&self) -> ExecutionBackend;
    fn artifacts(&self) -> &'static [ArtifactKind];
    fn supported_runtimes(&self) -> &'static [RuntimeKind];
    fn execute(&self, ctx: &BackendContext, task: &TaskEnvelope) -> LibResult<TaskResultEnvelope>;
}

#[derive(Default)]
pub struct LocalThreadBackend;

impl WorkerExecutionBackend for LocalThreadBackend {
    fn name(&self) -> &'static str {
        "local-thread"
    }

    fn exec_backend(&self) -> ExecutionBackend {
        ExecutionBackend::LocalThread
    }

    fn artifacts(&self) -> &'static [ArtifactKind] {
        &[]
    }

    fn supported_runtimes(&self) -> &'static [RuntimeKind] {
        &[RuntimeKind::Rust]
    }

    fn execute(&self, ctx: &BackendContext, task: &TaskEnvelope) -> LibResult<TaskResultEnvelope> {
        Ok(TaskResultEnvelope::ok(
            task.run_id,
            task.stage_id,
            task.task_id,
            task.attempt_id,
            ctx.worker_id.to_string(),
            Vec::new(),
        ))
    }
}

#[derive(Default)]
pub struct DockerBackend;

impl WorkerExecutionBackend for DockerBackend {
    fn name(&self) -> &'static str {
        "docker"
    }

    fn exec_backend(&self) -> ExecutionBackend {
        ExecutionBackend::Docker
    }

    fn artifacts(&self) -> &'static [ArtifactKind] {
        &[ArtifactKind::Docker]
    }

    fn supported_runtimes(&self) -> &'static [RuntimeKind] {
        &[RuntimeKind::Rust]
    }

    fn execute(&self, ctx: &BackendContext, task: &TaskEnvelope) -> LibResult<TaskResultEnvelope> {
        futures::executor::block_on(self.execute_async(ctx, task))
    }
}

impl DockerBackend {
    async fn execute_async(
        &self,
        ctx: &BackendContext,
        task: &TaskEnvelope,
    ) -> LibResult<TaskResultEnvelope> {
        let payload = self.decode_payload(task)?;
        let image = task.artifact.uri.clone();

        let docker = Docker::connect_with_local_defaults()
            .map_err(|e| Error::DockerRuntime(e.to_string()))?;

        self.ensure_image(&docker, &image).await?;

        let container_id = self
            .create_container(&docker, &image, &payload)
            .await
            .map_err(|e| Error::DockerRuntime(e.to_string()))?;

        let timeout_ms = task.artifact.profile.timeout_ms.max(1);
        let stdin = payload.stdin.as_deref();
        let run_result = timeout(
            Duration::from_millis(timeout_ms),
            self.run_container_and_collect_logs(&docker, &container_id, stdin),
        )
        .await;

        let _ = docker
            .remove_container(
                &container_id,
                Some(RemoveContainerOptionsBuilder::default().force(true).build()),
            )
            .await;

        match run_result {
            Ok(Ok((exit_code, logs))) if exit_code == 0 => Ok(TaskResultEnvelope::ok(
                task.run_id,
                task.stage_id,
                task.task_id,
                task.attempt_id,
                ctx.worker_id.to_string(),
                logs,
            )),
            Ok(Ok((exit_code, logs))) => Ok(TaskResultEnvelope::retryable_failure(
                task.run_id,
                task.stage_id,
                task.task_id,
                task.attempt_id,
                ctx.worker_id.to_string(),
                Error::ContainerExit { image, exit_code }.to_string(),
                logs,
            )),
            Ok(Err(err)) => Ok(TaskResultEnvelope::retryable_failure(
                task.run_id,
                task.stage_id,
                task.task_id,
                task.attempt_id,
                ctx.worker_id.to_string(),
                err.to_string(),
                Vec::new(),
            )),
            Err(_) => Ok(TaskResultEnvelope::retryable_failure(
                task.run_id,
                task.stage_id,
                task.task_id,
                task.attempt_id,
                ctx.worker_id.to_string(),
                format!("docker task timeout after {}ms", timeout_ms),
                Vec::new(),
            )),
        }
    }

    fn decode_payload(&self, task: &TaskEnvelope) -> LibResult<DockerTaskPayload> {
        from_bytes::<DockerTaskPayload, rkyv::rancor::Error>(&task.payload)
            .map_err(|e| Error::InvalidPayload(e.to_string()))
    }

    async fn ensure_image(&self, docker: &Docker, image: &str) -> LibResult<()> {
        let list = docker
            .list_images(Some(ListImagesOptionsBuilder::default().all(true).build()))
            .await
            .map_err(|e| Error::DockerRuntime(e.to_string()))?;

        let exists = list
            .iter()
            .any(|img| img.repo_tags.iter().any(|t| t.as_str() == image));

        if !exists {
            let mut pull = docker.create_image(
                Some(
                    CreateImageOptionsBuilder::default()
                        .from_image(image)
                        .build(),
                ),
                None,
                None,
            );

            while pull
                .try_next()
                .await
                .map_err(|e| Error::DockerRuntime(e.to_string()))?
                .is_some()
            {}
        }

        Ok(())
    }

    async fn create_container(
        &self,
        docker: &Docker,
        image: &str,
        payload: &DockerTaskPayload,
    ) -> Result<String, bollard::errors::Error> {
        let env = payload
            .env
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>();

        let mut cmd = payload.command.clone();
        if cmd.is_empty() {
            cmd.push("/bin/sh".to_string());
            cmd.push("-lc".to_string());
            cmd.push("true".to_string());
        }

        let has_stdin = payload.stdin.is_some();
        let cfg = ContainerCreateBody {
            image: Some(image.to_string()),
            cmd: Some(cmd),
            env: if env.is_empty() { None } else { Some(env) },
            working_dir: payload.work_dir.clone(),
            // Open a stdin pipe when partition data needs to be sent.
            attach_stdin: if has_stdin { Some(true) } else { None },
            open_stdin: if has_stdin { Some(true) } else { None },
            stdin_once: if has_stdin { Some(true) } else { None },
            ..Default::default()
        };

        let container = docker.create_container(None, cfg).await?;

        Ok(container.id)
    }

    /// Run the container and collect stdout/stderr as result bytes.
    ///
    /// When `stdin_data` is present, the bytes are written to the container's
    /// stdin before starting. The container is expected to write rkyv-encoded
    /// results to stdout.
    async fn run_container_and_collect_logs(
        &self,
        docker: &Docker,
        container_id: &str,
        stdin_data: Option<&[u8]>,
    ) -> LibResult<(i64, Vec<u8>)> {
        if let Some(data) = stdin_data {
            // Attach to stdin/stdout before starting so we don't miss any output.
            use tokio::io::AsyncWriteExt;

            let attach_opts = AttachContainerOptionsBuilder::default()
                .stdin(true)
                .stdout(true)
                .stderr(true)
                .stream(true)
                .build();
            let AttachContainerResults {
                mut input,
                mut output,
            } = docker
                .attach_container(container_id, Some(attach_opts))
                .await
                .map_err(|e| Error::DockerRuntime(e.to_string()))?;

            docker
                .start_container(container_id, None)
                .await
                .map_err(|e| Error::DockerRuntime(e.to_string()))?;

            // Write partition bytes then close stdin.
            input
                .write_all(data)
                .await
                .map_err(|e| Error::DockerRuntime(e.to_string()))?;
            input
                .flush()
                .await
                .map_err(|e| Error::DockerRuntime(e.to_string()))?;
            drop(input);

            // Collect stdout bytes from the attach output stream.
            let mut collected = Vec::new();
            while let Some(chunk) = output
                .try_next()
                .await
                .map_err(|e| Error::DockerRuntime(e.to_string()))?
            {
                collected.extend_from_slice(&chunk.into_bytes());
            }

            let mut waiter = docker.wait_container(container_id, None);
            let mut exit_code = 1_i64;
            while let Some(status) = waiter
                .try_next()
                .await
                .map_err(|e| Error::DockerRuntime(e.to_string()))?
            {
                exit_code = status.status_code;
            }

            Ok((exit_code, collected))
        } else {
            docker
                .start_container(container_id, None)
                .await
                .map_err(|e| Error::DockerRuntime(e.to_string()))?;

            let mut logs = docker.logs(
                container_id,
                Some(
                    LogsOptionsBuilder::default()
                        .follow(true)
                        .stdout(true)
                        .stderr(true)
                        .timestamps(true)
                        .build(),
                ),
            );

            let mut collected = Vec::new();
            while let Some(line) = logs
                .try_next()
                .await
                .map_err(|e| Error::DockerRuntime(e.to_string()))?
            {
                collected.extend_from_slice(&line.into_bytes());
                collected.push(b'\n');
            }

            let mut waiter = docker.wait_container(container_id, None);
            let mut exit_code = 1_i64;
            while let Some(status) = waiter
                .try_next()
                .await
                .map_err(|e| Error::DockerRuntime(e.to_string()))?
            {
                exit_code = status.status_code;
            }

            Ok((exit_code, collected))
        }
    }
}

/// Executes prebuilt WASM modules via wasmtime.
///
/// Compiled `Module` objects are cached by `artifact_ref + digest` so that
/// repeated task calls skip recompilation (~5ms) and only pay instantiation cost (~0.1ms).
pub struct WasmBackend {
    engine: Arc<Engine>,
    /// key: "artifact_ref:digest" → compiled Module
    module_cache: Arc<DashMap<String, Arc<Module>>>,
}

impl Default for WasmBackend {
    fn default() -> Self {
        Self {
            engine: Arc::new(Engine::default()),
            module_cache: Arc::new(DashMap::new()),
        }
    }
}

impl WorkerExecutionBackend for WasmBackend {
    fn name(&self) -> &'static str {
        "wasm"
    }

    fn exec_backend(&self) -> ExecutionBackend {
        ExecutionBackend::Wasm
    }

    fn artifacts(&self) -> &'static [ArtifactKind] {
        &[ArtifactKind::Wasm]
    }

    fn supported_runtimes(&self) -> &'static [RuntimeKind] {
        &[RuntimeKind::Rust]
    }

    fn execute(&self, ctx: &BackendContext, task: &TaskEnvelope) -> LibResult<TaskResultEnvelope> {
        match self.execute_module(task) {
            Ok(result_data) => Ok(TaskResultEnvelope::ok(
                task.run_id,
                task.stage_id,
                task.task_id,
                task.attempt_id,
                ctx.worker_id.to_string(),
                result_data,
            )),
            Err(err) => Ok(TaskResultEnvelope::fatal_failure(
                task.run_id,
                task.stage_id,
                task.task_id,
                task.attempt_id,
                ctx.worker_id.to_string(),
                err.to_string(),
            )),
        }
    }
}

impl WasmBackend {
    /// Returns a compiled `Module` from the cache, compiling and caching it on first call.
    ///
    /// The cache key is `artifact_ref:digest` — each unique (path, digest) pair is compiled once.
    fn get_or_compile(&self, task: &TaskEnvelope) -> LibResult<Arc<Module>> {
        let key = format!(
            "{}:{}",
            task.artifact.uri,
            task.artifact.digest.as_deref().unwrap_or("")
        );
        if let Some(cached) = self.module_cache.get(&key) {
            return Ok(cached.clone());
        }
        let bytes = self.load_module_bytes(task)?;
        self.validate_digest(task, &bytes)?;
        let module = Arc::new(
            Module::new(&self.engine, &bytes).map_err(|err| Error::WasmRuntime(err.to_string()))?,
        );
        self.module_cache.insert(key, module.clone());
        Ok(module)
    }

    fn execute_module(&self, task: &TaskEnvelope) -> LibResult<Vec<u8>> {
        let payload = self.decode_payload(task)?;
        self.validate_payload(&payload)?;

        let module = self.get_or_compile(task)?;
        let mut store = Store::new(&*self.engine, ());
        let instance = Instance::new(&mut store, &module, &[])
            .map_err(|err| Error::WasmRuntime(err.to_string()))?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| Error::WasmRuntime("missing exported memory".to_string()))?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(|err| Error::WasmRuntime(err.to_string()))?;
        let run = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, task.artifact.entrypoint.as_str())
            .map_err(|err| Error::WasmRuntime(err.to_string()))?;
        let dealloc = instance
            .get_func(&mut store, "dealloc")
            .and_then(|func| func.typed::<(i32, i32), ()>(&mut store).ok());

        self.execute_guest(
            &mut store,
            &memory,
            &alloc,
            &run,
            dealloc.as_ref(),
            &task.data,
        )
    }

    fn decode_payload(&self, task: &TaskEnvelope) -> LibResult<WasmTaskPayload> {
        WasmTaskPayload::decode_wire(&task.payload)
            .map_err(|err| Error::InvalidPayload(err.to_string()))
    }

    fn validate_payload(&self, payload: &WasmTaskPayload) -> LibResult<()> {
        if payload.abi_version != WIRE_SCHEMA_V1 {
            return Err(Error::InvalidPayload(format!(
                "unsupported wasm abi version {}",
                payload.abi_version
            )));
        }

        if !matches!(
            payload.part_enc,
            WasmValueEncoding::RawBytes | WasmValueEncoding::Rkyv | WasmValueEncoding::Json
        ) {
            return Err(Error::InvalidPayload(
                "unsupported wasm partition encoding".to_string(),
            ));
        }

        if !matches!(
            payload.result_enc,
            WasmValueEncoding::RawBytes | WasmValueEncoding::Rkyv | WasmValueEncoding::Json
        ) {
            return Err(Error::InvalidPayload(
                "unsupported wasm result encoding".to_string(),
            ));
        }

        Ok(())
    }

    fn load_module_bytes(&self, task: &TaskEnvelope) -> LibResult<Vec<u8>> {
        let path = self.artifact_path(&task.artifact.uri)?;
        fs::read(&path).map_err(|err| {
            Error::ArtifactLoad(format!("failed to read {}: {}", path.display(), err))
        })
    }

    fn artifact_path(&self, artifact_ref: &str) -> LibResult<PathBuf> {
        if let Some(path) = artifact_ref.strip_prefix("file://") {
            return Ok(PathBuf::from(path));
        }

        let path = PathBuf::from(artifact_ref);
        if path.exists() {
            return Ok(path);
        }

        Err(Error::ArtifactLoad(format!(
            "unsupported wasm artifact reference {}",
            artifact_ref
        )))
    }

    fn validate_digest(&self, task: &TaskEnvelope, module_bytes: &[u8]) -> LibResult<()> {
        let Some(expected_digest) = task.artifact.digest.as_deref() else {
            return Ok(());
        };

        let hash = Sha256::digest(module_bytes);
        let actual: String = hash.iter().fold(String::new(), |mut s, b| {
            use std::fmt::Write;
            let _ = write!(s, "{:02x}", b);
            s
        });
        let expected = expected_digest
            .strip_prefix("sha256:")
            .unwrap_or(expected_digest);
        if actual != expected {
            return Err(Error::ArtifactValidation(format!(
                "digest mismatch for {}: expected {}, got {}",
                task.artifact.uri, expected, actual
            )));
        }

        Ok(())
    }

    fn execute_guest(
        &self,
        store: &mut Store<()>,
        memory: &Memory,
        alloc: &TypedFunc<i32, i32>,
        run: &TypedFunc<(i32, i32), i64>,
        dealloc: Option<&TypedFunc<(i32, i32), ()>>,
        input: &[u8],
    ) -> LibResult<Vec<u8>> {
        let input_len = i32::try_from(input.len())
            .map_err(|_| Error::WasmRuntime("input too large for wasm guest".to_string()))?;
        let input_ptr = alloc
            .call(&mut *store, input_len)
            .map_err(|err| Error::WasmRuntime(err.to_string()))?;

        memory
            .write(&mut *store, input_ptr as usize, input)
            .map_err(|err| Error::WasmRuntime(err.to_string()))?;

        let packed = run
            .call(&mut *store, (input_ptr, input_len))
            .map_err(|err| Error::WasmRuntime(err.to_string()))?;

        if let Some(dealloc) = dealloc {
            let _ = dealloc.call(&mut *store, (input_ptr, input_len));
        }

        let result_ptr = (packed >> 32) as u32 as usize;
        let result_len = packed as u32 as usize;
        if result_len == 0 {
            return Ok(Vec::new());
        }

        let mut result = vec![0_u8; result_len];
        memory
            .read(&mut *store, result_ptr, &mut result)
            .map_err(|err| Error::WasmRuntime(err.to_string()))?;

        if let Some(dealloc) = dealloc {
            let _ = dealloc.call(
                &mut *store,
                (
                    i32::try_from(result_ptr)
                        .map_err(|_| Error::WasmRuntime("result pointer overflow".to_string()))?,
                    i32::try_from(result_len)
                        .map_err(|_| Error::WasmRuntime("result length overflow".to_string()))?,
                ),
            );
        }

        Ok(result)
    }
}

pub enum WorkerRuntime {
    Thread(LocalThreadBackend),
    Docker(DockerBackend),
    Wasm(WasmBackend),
}

impl WorkerRuntime {
    pub fn from_execution_backend(backend: ExecutionBackend) -> Self {
        match backend {
            ExecutionBackend::LocalThread => Self::Thread(LocalThreadBackend),
            ExecutionBackend::Docker => Self::Docker(DockerBackend),
            ExecutionBackend::Wasm => Self::Wasm(WasmBackend::default()),
        }
    }

    pub fn default_for(distributed_mode: bool) -> Self {
        if distributed_mode {
            Self::Docker(DockerBackend)
        } else {
            Self::Thread(LocalThreadBackend)
        }
    }

    pub fn name(&self) -> &'static str {
        self.as_backend().name()
    }

    pub fn capabilities(
        &self,
        worker_id: impl Into<String>,
        max_concurrent_tasks: u16,
    ) -> WorkerCapabilities {
        let backend = self.as_backend();
        WorkerCapabilities {
            version: WIRE_SCHEMA_V1,
            worker_id: worker_id.into(),
            backend: backend.exec_backend(),
            artifacts: backend.artifacts().to_vec(),
            runtimes: backend.supported_runtimes().to_vec(),
            max_tasks: max_concurrent_tasks,
        }
    }

    pub fn execute(
        &self,
        ctx: &BackendContext,
        task: &TaskEnvelope,
    ) -> LibResult<TaskResultEnvelope> {
        let backend = self.as_backend();
        let configured = backend.exec_backend();
        if configured != task.artifact.backend {
            return Err(Error::UnsupportedExecutionBackend {
                configured,
                requested: task.artifact.backend,
            });
        }

        if !backend.artifacts().contains(&task.artifact.kind) {
            return Err(Error::UnsupportedArtifact {
                backend: configured,
                artifact_kind: task.artifact.kind,
            });
        }

        if !backend
            .supported_runtimes()
            .contains(&task.artifact.runtime)
        {
            return Err(Error::UnsupportedOperation(
                "task runtime is not supported by the configured worker backend",
            ));
        }

        backend.execute(ctx, task)
    }

    /// Construct a runtime from an artifact descriptor, dispatching on `RuntimeKind` first.
    ///
    /// Python and JS tasks are always embedded in the worker regardless of `execution_backend`.
    /// The `execution_backend` field controls scheduler placement, not invocation mechanism.
    pub fn from_artifact(descriptor: &atomic_data::distributed::ArtifactDescriptor) -> Self {
        match descriptor.backend {
            ExecutionBackend::Wasm => Self::Wasm(WasmBackend::default()),
            ExecutionBackend::Docker => Self::Docker(DockerBackend),
            ExecutionBackend::LocalThread => Self::Thread(LocalThreadBackend),
        }
    }

    fn as_backend(&self) -> &dyn WorkerExecutionBackend {
        match self {
            Self::Thread(backend) => backend,
            Self::Docker(backend) => backend,
            Self::Wasm(backend) => backend,
        }
    }
}

#[cfg(test)]
mod tests {
    use atomic_data::distributed::{
        ArtifactDescriptor, ArtifactKind, DockerTaskPayload, ExecutionBackend, ResourceProfile,
        RuntimeKind, TaskEnvelope, WIRE_SCHEMA_V1, WasmTaskPayload, WasmValueEncoding, WireEncode,
    };
    use rkyv::to_bytes;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;
    use wat::parse_str;

    use super::{BackendContext, WorkerRuntime};

    use rkyv::rancor::Error as RkyvError;

    fn test_task(kind: ArtifactKind, backend: ExecutionBackend) -> TaskEnvelope {
        let payload = DockerTaskPayload {
            command: vec!["echo".to_string(), "hello".to_string()],
            env: vec![("ATOMIC_TASK".to_string(), "1".to_string())],
            work_dir: None,
            stdin: None,
            log_key: None,
        };

        let payload_bytes = to_bytes::<RkyvError>(&payload)
            .expect("payload serialization should succeed")
            .to_vec();

        TaskEnvelope::new(
            1,
            2,
            3,
            0,
            4,
            "trace-1".to_string(),
            ArtifactDescriptor {
                op_id: "op.v1".to_string(),
                backend,
                kind,
                uri: "busybox:latest".to_string(),
                entrypoint: "run".to_string(),
                runtime: RuntimeKind::Rust,
                digest: Some("sha256:test".to_string()),
                build_target: None,
                profile: ResourceProfile {
                    cpu_millis: 500,
                    memory_mb: 256,
                    timeout_ms: 2_000,
                },
            },
            payload_bytes,
            vec![],
        )
    }

    #[test]
    fn runtime_name_tracks_explicit_backend_choice() {
        let runtime = WorkerRuntime::from_execution_backend(ExecutionBackend::Docker);
        assert_eq!(runtime.name(), "docker");

        let runtime = WorkerRuntime::from_execution_backend(ExecutionBackend::Wasm);
        assert_eq!(runtime.name(), "wasm");
    }

    #[test]
    fn wasm_backend_executes_map_artifact() {
        let runtime = WorkerRuntime::from_execution_backend(ExecutionBackend::Wasm);
        let ctx = BackendContext {
            worker_id: "worker-test".into(),
        };
        let task = wasm_task("run_map", map_module_bytes(), vec![1, 2, 3], None);
        let result = runtime
            .execute(&ctx, &task)
            .expect("wasm task envelope response");
        assert_eq!(result.data, vec![2, 3, 4]);
    }

    #[test]
    fn wasm_backend_executes_reduce_artifact() {
        let runtime = WorkerRuntime::from_execution_backend(ExecutionBackend::Wasm);
        let ctx = BackendContext {
            worker_id: "worker-test".into(),
        };
        let task = wasm_task("run_reduce", reduce_module_bytes(), vec![1, 2, 3], None);
        let result = runtime
            .execute(&ctx, &task)
            .expect("wasm task envelope response");
        assert_eq!(result.data, 6_u32.to_le_bytes());
    }

    #[test]
    fn wasm_backend_validates_digest() {
        let runtime = WorkerRuntime::from_execution_backend(ExecutionBackend::Wasm);
        let ctx = BackendContext {
            worker_id: "worker-test".into(),
        };
        let task = wasm_task(
            "run_map",
            map_module_bytes(),
            vec![1, 2, 3],
            Some("sha256:deadbeef".to_string()),
        );
        let result = runtime
            .execute(&ctx, &task)
            .expect("wasm task envelope response");
        assert!(result.error.as_deref().is_some());
    }

    #[test]
    fn runtime_rejects_task_for_other_backend() {
        let runtime = WorkerRuntime::from_execution_backend(ExecutionBackend::Docker);
        let ctx = BackendContext {
            worker_id: "worker-test".into(),
        };
        let task = test_task(ArtifactKind::Wasm, ExecutionBackend::Wasm);
        let result = runtime.execute(&ctx, &task);
        assert!(result.is_err());
    }

    #[test]
    fn docker_payload_rkyv_roundtrip() {
        let payload = DockerTaskPayload {
            command: vec!["python".to_string(), "main.py".to_string()],
            env: vec![("K".to_string(), "V".to_string())],
            work_dir: Some("/work".to_string()),
            stdin: Some(vec![1, 2, 3]),
            log_key: Some("run-1".to_string()),
        };

        let bytes = to_bytes::<rkyv::rancor::Error>(&payload)
            .expect("serialize")
            .to_vec();
        let decoded = rkyv::from_bytes::<DockerTaskPayload, rkyv::rancor::Error>(&bytes)
            .expect("deserialize");
        assert_eq!(decoded.command, payload.command);
        assert_eq!(decoded.env, payload.env);
    }

    fn wasm_task(
        entrypoint: &str,
        module: Vec<u8>,
        partition_data: Vec<u8>,
        artifact_digest: Option<String>,
    ) -> TaskEnvelope {
        let dir = tempdir().expect("tempdir");
        let module_path = dir.path().join(format!("{}.wasm", entrypoint));
        std::fs::write(&module_path, &module).expect("write wasm module");

        let payload = WasmTaskPayload {
            abi_version: WIRE_SCHEMA_V1,
            cfg_enc: WasmValueEncoding::RawBytes,
            cfg_data: Vec::new(),
            part_enc: WasmValueEncoding::RawBytes,
            result_enc: WasmValueEncoding::RawBytes,
        };
        let payload_bytes = payload.encode_wire().expect("serialize wasm payload");
        let digest =
            artifact_digest.or_else(|| Some(format!("sha256:{:?}", Sha256::digest(&module))));

        let task = TaskEnvelope::new(
            1,
            2,
            3,
            0,
            4,
            "trace-1".to_string(),
            ArtifactDescriptor {
                op_id: "op.v1".to_string(),
                backend: ExecutionBackend::Wasm,
                kind: ArtifactKind::Wasm,
                uri: module_path.display().to_string(),
                entrypoint: entrypoint.to_string(),
                runtime: RuntimeKind::Rust,
                digest,
                build_target: Some("wasm32-wasip2".to_string()),
                profile: ResourceProfile {
                    cpu_millis: 500,
                    memory_mb: 256,
                    timeout_ms: 2_000,
                },
            },
            payload_bytes,
            partition_data,
        );

        std::mem::forget(dir);
        task
    }

    fn map_module_bytes() -> Vec<u8> {
        parse_str(
            r#"(module
                (memory (export \"memory\") 1)
                (global $heap (mut i32) (i32.const 1024))
                (func $alloc (export \"alloc\") (param $len i32) (result i32)
                    (local $ptr i32)
                    global.get $heap
                    local.set $ptr
                    global.get $heap
                    local.get $len
                    i32.add
                    global.set $heap
                    local.get $ptr)
                (func (export \"run_map\") (param $ptr i32) (param $len i32) (result i64)
                    (local $out i32)
                    (local $i i32)
                    local.get $len
                    call $alloc
                    local.set $out
                    block $done
                        loop $copy
                            local.get $i
                            local.get $len
                            i32.ge_u
                            br_if $done
                            local.get $out
                            local.get $i
                            i32.add
                            local.get $ptr
                            local.get $i
                            i32.add
                            i32.load8_u
                            i32.const 1
                            i32.add
                            i32.store8
                            local.get $i
                            i32.const 1
                            i32.add
                            local.set $i
                            br $copy
                        end
                    end
                    local.get $out
                    i64.extend_i32_u
                    i64.const 32
                    i64.shl
                    local.get $len
                    i64.extend_i32_u
                    i64.or))"#,
        )
        .expect("map wat should compile")
    }

    fn reduce_module_bytes() -> Vec<u8> {
        parse_str(
            r#"(module
                (memory (export \"memory\") 1)
                (global $heap (mut i32) (i32.const 1024))
                (func $alloc (export \"alloc\") (param $len i32) (result i32)
                    (local $ptr i32)
                    global.get $heap
                    local.set $ptr
                    global.get $heap
                    local.get $len
                    i32.add
                    global.set $heap
                    local.get $ptr)
                (func (export \"run_reduce\") (param $ptr i32) (param $len i32) (result i64)
                    (local $out i32)
                    (local $i i32)
                    (local $sum i32)
                    i32.const 4
                    call $alloc
                    local.set $out
                    block $done
                        loop $sum_loop
                            local.get $i
                            local.get $len
                            i32.ge_u
                            br_if $done
                            local.get $sum
                            local.get $ptr
                            local.get $i
                            i32.add
                            i32.load8_u
                            i32.add
                            local.set $sum
                            local.get $i
                            i32.const 1
                            i32.add
                            local.set $i
                            br $sum_loop
                        end
                    end
                    local.get $out
                    local.get $sum
                    i32.store
                    local.get $out
                    i64.extend_i32_u
                    i64.const 32
                    i64.shl
                    i64.const 4
                    i64.or))"#,
        )
        .expect("reduce wat should compile")
    }
}
