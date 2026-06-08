/// Python UDF subprocess worker pool.
///
/// Spawns a fixed pool of Python processes. Each process runs the embedded
/// [`WORKER_SCRIPT`] in an infinite read-task / write-result loop. Because each
/// subprocess has its own CPython interpreter and its own GIL, tasks dispatched to
/// different subprocesses execute in true parallel — unlike the embedded-PyO3 model
/// where all tokio threads compete for one process-wide GIL.
///
/// ## IPC protocol
///
/// Frames in both directions use a 4-byte big-endian length prefix followed by a
/// UTF-8 JSON payload:
///
/// **Request** (Rust → Python stdin):
/// ```json
/// { "action": "map", "fn_b64": "<base64(pickle)>", "data_json": "[…]", "zero_b64": null }
/// ```
///
/// **Response** (Python stdout → Rust):
/// ```json
/// { "ok": true,  "data_json": "[…]", "error": null }
/// { "ok": false, "data_json": null,  "error": "…" }
/// ```
use std::sync::OnceLock;

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::{Mutex, mpsc};

use atomic_data::distributed::PythonUdfPayload;


const WORKER_SCRIPT: &str = r#"
import sys, json, base64, struct

# Prefer cloudpickle (ships closures + lambdas) but fall back to stdlib pickle.
try:
    import cloudpickle as _pkl
except ImportError:
    import pickle as _pkl

def read_frame():
    raw = sys.stdin.buffer.read(4)
    if len(raw) < 4:
        return None
    length = struct.unpack(">I", raw)[0]
    data = b""
    while len(data) < length:
        chunk = sys.stdin.buffer.read(length - len(data))
        if not chunk:
            return None
        data += chunk
    return json.loads(data)

def write_frame(obj):
    payload = json.dumps(obj).encode("utf-8")
    sys.stdout.buffer.write(struct.pack(">I", len(payload)))
    sys.stdout.buffer.write(payload)
    sys.stdout.buffer.flush()

while True:
    task = read_frame()
    if task is None:
        break
    try:
        # fn_bytes is a pickled partition-level function: lambda partition: [...]
        partition_fn = _pkl.loads(base64.b64decode(task["fn_b64"]))
        partition    = json.loads(task["data_json"])
        result       = partition_fn(partition)
        write_frame({"ok": True, "data_json": json.dumps(result), "error": None})
    except Exception as e:
        write_frame({"ok": False, "data_json": None, "error": str(e)})
"#;


#[derive(Serialize)]
struct TaskFrame<'a> {
    fn_b64: String,
    data_json: &'a str,
}

#[derive(Deserialize)]
struct ResultFrame {
    ok: bool,
    data_json: Option<String>,
    error: Option<String>,
}


struct PythonWorkerHandle {
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    child: Child,
}

impl PythonWorkerHandle {
    async fn send_task(&mut self, frame: &TaskFrame<'_>) -> Result<Vec<u8>, String> {
        let payload =
            serde_json::to_vec(frame).map_err(|e| format!("serialize task frame: {e}"))?;
        let len = (payload.len() as u32).to_be_bytes();

        self.stdin
            .write_all(&len)
            .await
            .map_err(|e| format!("write frame len: {e}"))?;
        self.stdin
            .write_all(&payload)
            .await
            .map_err(|e| format!("write frame payload: {e}"))?;
        self.stdin
            .flush()
            .await
            .map_err(|e| format!("flush stdin: {e}"))?;

        let result = self.read_result().await?;
        if result.ok {
            Ok(result.data_json.unwrap_or_default().into_bytes())
        } else {
            Err(result
                .error
                .unwrap_or_else(|| "unknown python error".to_string()))
        }
    }

    async fn read_result(&mut self) -> Result<ResultFrame, String> {
        let mut len_buf = [0u8; 4];
        self.stdout
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| format!("read result len: {e}"))?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut buf = vec![0u8; len];
        self.stdout
            .read_exact(&mut buf)
            .await
            .map_err(|e| format!("read result payload: {e}"))?;

        serde_json::from_slice(&buf).map_err(|e| format!("deserialize result frame: {e}"))
    }

    fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}


/// Process-wide pool of persistent Python worker subprocesses.
pub struct PythonWorkerPool {
    idle_tx: mpsc::Sender<PythonWorkerHandle>,
    idle_rx: Mutex<mpsc::Receiver<PythonWorkerHandle>>,
    script_path: std::path::PathBuf,
    pool_size: usize,
}

impl PythonWorkerPool {
    /// Resolve the worker script path.
    ///
    /// If `ATOMIC_PYTHON_WORKER_SCRIPT` is set, use that file directly (useful for
    /// debugging — point it at a local `.py` file and edits take effect on next pool
    /// restart without recompiling). Otherwise write the embedded `WORKER_SCRIPT` to
    /// a temp file that persists for the process lifetime.
    fn write_worker_script() -> Result<std::path::PathBuf, String> {
        if let Ok(path) = std::env::var("ATOMIC_PYTHON_WORKER_SCRIPT") {
            return Ok(std::path::PathBuf::from(path));
        }
        let mut tmp =
            NamedTempFile::new().map_err(|e| format!("create worker script temp file: {e}"))?;
        std::io::Write::write_all(&mut tmp, WORKER_SCRIPT.as_bytes())
            .map_err(|e| format!("write worker script: {e}"))?;
        tmp.into_temp_path()
            .keep()
            .map_err(|e| format!("keep temp script: {e}"))
    }

    /// Spawn `pool_size` Python worker subprocesses and return a ready pool.
    pub async fn new(pool_size: usize) -> Result<Self, String> {
        let script_path = Self::write_worker_script()?;

        let (tx, rx) = mpsc::channel(pool_size);

        for _ in 0..pool_size {
            let handle = Self::spawn_worker(&script_path).await?;
            tx.send(handle)
                .await
                .map_err(|_| "pool channel closed during init".to_string())?;
        }

        Ok(Self {
            idle_tx: tx,
            idle_rx: Mutex::new(rx),
            script_path,
            pool_size,
        })
    }

    async fn spawn_worker(script_path: &std::path::Path) -> Result<PythonWorkerHandle, String> {
        use std::process::Stdio;
        use tokio::process::Command;

        let mut child = Command::new("python3")
            .arg(script_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| format!("spawn python3: {e}"))?;

        let stdin = child.stdin.take().ok_or("no stdin on python worker")?;
        let stdout = child.stdout.take().ok_or("no stdout on python worker")?;

        Ok(PythonWorkerHandle {
            stdin: BufWriter::new(stdin),
            stdout: BufReader::new(stdout),
            child,
        })
    }

    /// Execute a Python UDF task, dispatching to an idle subprocess.
    ///
    /// If the chosen subprocess has died, it is restarted and the task is retried
    /// once. A second failure returns an error.
    pub async fn execute(&self, spec: &PythonUdfPayload, data: &[u8]) -> Result<Vec<u8>, String> {
        let fn_b64 = B64.encode(&spec.fn_bytes);
        let data_str = std::str::from_utf8(data).map_err(|e| format!("data utf8: {e}"))?;

        let frame = TaskFrame {
            fn_b64,
            data_json: data_str,
        };

        let mut handle = self.take_idle().await?;

        if !handle.is_alive() {
            log::warn!("python worker died; restarting");
            handle = Self::spawn_worker(&self.script_path).await?;
        }

        match handle.send_task(&frame).await {
            Ok(result) => {
                self.return_idle(handle).await;
                Ok(result)
            }
            Err(first_err) => {
                log::warn!("python worker task failed ({first_err}); retrying with fresh worker");
                let mut fresh = Self::spawn_worker(&self.script_path)
                    .await
                    .map_err(|e| format!("respawn after failure: {e}"))?;
                match fresh.send_task(&frame).await {
                    Ok(result) => {
                        self.return_idle(fresh).await;
                        Ok(result)
                    }
                    Err(second_err) => Err(format!(
                        "python udf failed twice: first={first_err}; retry={second_err}"
                    )),
                }
            }
        }
    }

    async fn take_idle(&self) -> Result<PythonWorkerHandle, String> {
        self.idle_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| "python worker pool exhausted".to_string())
    }

    async fn return_idle(&self, handle: PythonWorkerHandle) {
        // Best-effort: if the channel is full (shouldn't happen with correct pool_size)
        // just drop the handle (subprocess exits when stdin closes).
        let _ = self.idle_tx.try_send(handle);
    }
}

/// Process-wide Python worker pool. Initialized once at worker startup via
/// [`init_python_worker_pool`].
pub static PYTHON_WORKER_POOL: OnceLock<PythonWorkerPool> = OnceLock::new();

/// Initialize the global [`PYTHON_WORKER_POOL`] with `pool_size` subprocesses.
///
/// Builds a temporary single-thread tokio runtime for the async pool init so
/// that it is safe to call from any context, including `start_worker` before
/// the main executor loop starts. Safe to call multiple times — only the first
/// call has any effect.
pub fn init_python_worker_pool(pool_size: usize) {
    PYTHON_WORKER_POOL.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio rt for python worker pool init");
        rt.block_on(PythonWorkerPool::new(pool_size))
            .expect("failed to start Python worker pool")
    });
}


/// [`OpDispatcher`] for `TaskRuntime::Python` ops.
///
/// Decodes the [`PythonUdfPayload`] from `op.payload` and forwards the partition
/// bytes to the process-wide [`PYTHON_WORKER_POOL`].
pub(crate) struct PythonDispatcher;

impl crate::backend::OpDispatcher for PythonDispatcher {
    fn dispatch(
        &self,
        op: &atomic_data::distributed::PipelineOp,
        _partition_id: usize,
        data: &[u8],
    ) -> std::result::Result<Vec<u8>, String> {
        let spec: PythonUdfPayload = serde_json::from_slice(&op.payload)
            .map_err(|e| format!("python_udf payload decode: {e}"))?;
        let pool = PYTHON_WORKER_POOL
            .get()
            .ok_or("PythonWorkerPool not initialized — call init_python_worker_pool() at startup")?;
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(pool.execute(&spec, data))
        })
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pool_map_roundtrip() {
        // Use cloudpickle to serialize a lambda (stdlib pickle cannot pickle lambdas).
        // Skip the test if cloudpickle is not installed — it is an optional dependency.
        let output = std::process::Command::new("python3")
            .args([
                "-c",
                "import cloudpickle, sys; sys.stdout.buffer.write(cloudpickle.dumps(lambda x: x*2))",
            ])
            .output();

        let fn_bytes = match output {
            Ok(o) if o.status.success() && !o.stdout.is_empty() => o.stdout,
            _ => {
                // cloudpickle not installed or python3 not available — skip
                eprintln!("skipping pool_map_roundtrip: cloudpickle not available");
                return;
            }
        };

        let pool = PythonWorkerPool::new(2).await.expect("pool init");
        let spec = PythonUdfPayload {
            fn_bytes,
            zero_bytes: vec![],
        };
        let data = b"[1, 2, 3]";

        let result = pool.execute(&spec, data).await.expect("map succeeded");

        let parsed: Vec<i64> = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed, vec![2, 4, 6]);
    }
}
