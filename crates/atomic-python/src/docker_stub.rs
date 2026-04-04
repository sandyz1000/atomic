use pyo3::prelude::*;
use pyo3::types::PyList;

use atomic_data::distributed::ArtifactDescriptor;
use atomic_data::stub::DockerStub;
use atomic_data::task_registry::TaskRegistry;

use crate::rdd::PyRdd;

/// A reference to a pre-built Docker task artifact.
///
/// Identical semantics to Rust's `DockerStub<I, O>` — references a pre-built
/// image by operation id. When passed to `Rdd.map_via()`, partition data is
/// JSON-encoded and sent to the Docker container via stdin; results are
/// JSON-decoded from stdout.
///
/// The same Docker image works with both Rust and Python clients.
///
/// # Example
/// ```python
/// stub = atomic.DockerStub.from_manifest("build/manifest.toml", "demo.map.v1")
/// result = ctx.parallelize([1, 2, 3]).map_via(stub)
/// ```
#[pyclass(name = "DockerStub")]
pub struct PyDockerStub {
    pub(crate) descriptor: ArtifactDescriptor,
}

#[pymethods]
impl PyDockerStub {
    /// Load a Docker stub from a manifest TOML file by operation id.
    #[staticmethod]
    pub fn from_manifest(path: &str, operation_id: &str) -> PyResult<Self> {
        let registry = TaskRegistry::load_toml_file(path)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let descriptor = registry.resolve(operation_id).ok_or_else(|| {
            pyo3::exceptions::PyKeyError::new_err(format!(
                "operation '{}' not found in manifest '{}'",
                operation_id, path
            ))
        })?;
        Ok(Self { descriptor })
    }

    pub fn operation_id(&self) -> &str {
        &self.descriptor.op_id
    }

    pub fn image(&self) -> &str {
        &self.descriptor.uri
    }

    pub fn __repr__(&self) -> String {
        format!(
            "DockerStub(operation_id={:?}, image={:?})",
            self.descriptor.op_id, self.descriptor.uri
        )
    }
}

impl PyDockerStub {
    /// Execute partitions against the Docker container.
    ///
    /// Splits `elements` into `num_partitions` slices, JSON-encodes each slice,
    /// sends it to the Docker container stdin, and JSON-decodes the results.
    /// Results are collected flat into a new `PyRdd`.
    pub fn execute_partitions(
        &self,
        py: Python,
        elements: &[Py<PyAny>],
        num_partitions: usize,
    ) -> PyResult<PyRdd> {
        let partitions = split_into_partitions(elements, num_partitions);
        let mut results: Vec<Py<PyAny>> = Vec::new();

        for partition in partitions {
            let partition_json = encode_partition_json(py, &partition)?;
            let output_bytes = run_docker_partition(self, &partition_json)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;
            let decoded = decode_partition_json(py, &output_bytes)?;
            results.extend(decoded);
        }

        Ok(PyRdd::from_data(py, results, num_partitions))
    }
}

/// Split elements into `n` roughly equal partitions.
fn split_into_partitions(elements: &[Py<PyAny>], n: usize) -> Vec<Vec<&Py<PyAny>>> {
    let n = n.max(1);
    let chunk_size = (elements.len() + n - 1) / n;
    if chunk_size == 0 {
        return vec![vec![]];
    }
    elements
        .chunks(chunk_size)
        .map(|c| c.iter().collect())
        .collect()
}

/// JSON-encode a partition as a Python list using `json.dumps`.
fn encode_partition_json(py: Python, partition: &[&Py<PyAny>]) -> PyResult<Vec<u8>> {
    let json_mod = py.import("json")?;
    let py_list = PyList::new(py, partition.iter().map(|o| o.bind(py).clone()))?;
    let json_str: String = json_mod.call_method1("dumps", (py_list,))?.extract()?;
    Ok(json_str.into_bytes())
}

/// JSON-decode the output bytes from a Docker container into a `Vec<Py<PyAny>>`.
fn decode_partition_json(py: Python, bytes: &[u8]) -> PyResult<Vec<Py<PyAny>>> {
    let json_mod = py.import("json")?;
    let s = std::str::from_utf8(bytes)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let result = json_mod.call_method1("loads", (s,))?;
    match result.downcast::<PyList>() {
        Ok(list) => Ok(list.iter().map(|item| item.unbind()).collect()),
        Err(_) => Ok(vec![result.unbind()]),
    }
}

/// Run a Docker container for a single partition, sending JSON bytes on stdin
/// and returning the stdout bytes.
///
/// Uses `bollard` indirectly via `std::process::Command` for simplicity in the
/// Python layer — a full tokio runtime is not required. The container must be
/// pre-pulled. Stdin/stdout use the rkyv-framing protocol:
///   [4-byte LE u32 = payload length][N bytes payload]
fn run_docker_partition(stub: &PyDockerStub, json_bytes: &[u8]) -> Result<Vec<u8>, String> {
    use std::io::{Read, Write};
    use std::process::{Command, Stdio};

    let image = &stub.descriptor.uri;
    let entrypoint = &stub.descriptor.entrypoint;

    let mut child = Command::new("docker")
        .args(["run", "--rm", "-i", image.as_str(), entrypoint.as_str()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to spawn docker: {}", e))?;

    // Write 4-byte LE length prefix + payload
    {
        let stdin = child.stdin.as_mut().ok_or("no stdin")?;
        let len = json_bytes.len() as u32;
        stdin
            .write_all(&len.to_le_bytes())
            .map_err(|e| format!("write len: {}", e))?;
        stdin
            .write_all(json_bytes)
            .map_err(|e| format!("write payload: {}", e))?;
        // Send shutdown frame (length = 0)
        stdin
            .write_all(&0u32.to_le_bytes())
            .map_err(|e| format!("write shutdown: {}", e))?;
    }

    let output = child
        .wait_with_output()
        .map_err(|e| format!("docker wait: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "docker container exited non-zero: {}",
            stderr.trim()
        ));
    }

    // Read 4-byte LE length prefix + payload from stdout
    let stdout = &output.stdout;
    if stdout.len() < 4 {
        return Err(format!(
            "docker output too short ({} bytes), expected length prefix",
            stdout.len()
        ));
    }
    let len = u32::from_le_bytes([stdout[0], stdout[1], stdout[2], stdout[3]]) as usize;
    if stdout.len() < 4 + len {
        return Err(format!(
            "docker output truncated: expected {} bytes, got {}",
            4 + len,
            stdout.len()
        ));
    }
    Ok(stdout[4..4 + len].to_vec())
}
