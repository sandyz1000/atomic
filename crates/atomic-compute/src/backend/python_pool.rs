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

use atomic_data::distributed::{PythonUdfPayload, UdfAction};

// ── Embedded Python worker script ─────────────────────────────────────────────

const WORKER_SCRIPT: &str = r#"
import sys, json, base64, struct, itertools, functools

# Prefer cloudpickle (ships closures + lambdas) but fall back to stdlib pickle.
# The driver is expected to use cloudpickle.dumps(); cloudpickle.loads() handles
# both cloudpickle and stdlib pickle payloads.
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

def apply_fold(fn, items, zero_b64):
    zero = _pkl.loads(base64.b64decode(zero_b64))
    return [functools.reduce(fn, items, zero)]

ACTION_MAP = {
    "map":             lambda fn, items, z: list(map(fn, items)),
    "filter":          lambda fn, items, z: list(filter(fn, items)),
    "flat_map":        lambda fn, items, z: list(itertools.chain.from_iterable(map(fn, items))),
    "map_values":      lambda fn, items, z: [[k, fn(v)] for k, v in items],
    "flat_map_values": lambda fn, items, z: [[k, w] for k, v in items for w in fn(v)],
    "key_by":          lambda fn, items, z: [[fn(x), x] for x in items],
    "reduce":          lambda fn, items, z: [functools.reduce(fn, items)] if items else [],
    "fold":            lambda fn, items, z: apply_fold(fn, items, z),
}

while True:
    task = read_frame()
    if task is None:
        break
    try:
        fn_obj = _pkl.loads(base64.b64decode(task["fn_b64"]))
        items  = json.loads(task["data_json"])
        zero   = task.get("zero_b64")
        result = ACTION_MAP[task["action"]](fn_obj, items, zero)
        write_frame({"ok": True, "data_json": json.dumps(result), "error": None})
    except Exception as e:
        write_frame({"ok": False, "data_json": None, "error": str(e)})
"#;

// ── IPC types ─────────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct TaskFrame<'a> {
    action:   &'a str,
    fn_b64:   String,
    data_json: &'a str,
    zero_b64: Option<String>,
}

#[derive(Deserialize)]
struct ResultFrame {
    ok:       bool,
    data_json: Option<String>,
    error:    Option<String>,
}

// ── Worker handle ─────────────────────────────────────────────────────────────

struct PythonWorkerHandle {
    stdin:  BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    child:  Child,
}

impl PythonWorkerHandle {
    async fn send_task(&mut self, frame: &TaskFrame<'_>) -> Result<Vec<u8>, String> {
        let payload = serde_json::to_vec(frame)
            .map_err(|e| format!("serialize task frame: {e}"))?;
        let len = (payload.len() as u32).to_be_bytes();

        self.stdin.write_all(&len).await
            .map_err(|e| format!("write frame len: {e}"))?;
        self.stdin.write_all(&payload).await
            .map_err(|e| format!("write frame payload: {e}"))?;
        self.stdin.flush().await
            .map_err(|e| format!("flush stdin: {e}"))?;

        let result = self.read_result().await?;
        if result.ok {
            Ok(result.data_json.unwrap_or_default().into_bytes())
        } else {
            Err(result.error.unwrap_or_else(|| "unknown python error".to_string()))
        }
    }

    async fn read_result(&mut self) -> Result<ResultFrame, String> {
        let mut len_buf = [0u8; 4];
        self.stdout.read_exact(&mut len_buf).await
            .map_err(|e| format!("read result len: {e}"))?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut buf = vec![0u8; len];
        self.stdout.read_exact(&mut buf).await
            .map_err(|e| format!("read result payload: {e}"))?;

        serde_json::from_slice(&buf)
            .map_err(|e| format!("deserialize result frame: {e}"))
    }

    fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}

// ── Pool ──────────────────────────────────────────────────────────────────────

/// Process-wide pool of persistent Python worker subprocesses.
pub struct PythonWorkerPool {
    idle_tx:   mpsc::Sender<PythonWorkerHandle>,
    idle_rx:   Mutex<mpsc::Receiver<PythonWorkerHandle>>,
    script_path: std::path::PathBuf,
    pool_size: usize,
}

impl PythonWorkerPool {
    /// Spawn `pool_size` Python worker subprocesses and return a ready pool.
    pub async fn new(pool_size: usize) -> Result<Self, String> {
        // Write the embedded worker script to a temp file. The file is kept alive
        // by the returned `PythonWorkerPool` via `script_path` (NamedTempFile would
        // delete on drop; we persist it for the process lifetime via into_temp_path).
        let mut tmp = NamedTempFile::new()
            .map_err(|e| format!("create worker script temp file: {e}"))?;
        std::io::Write::write_all(&mut tmp, WORKER_SCRIPT.as_bytes())
            .map_err(|e| format!("write worker script: {e}"))?;
        let script_path = tmp.into_temp_path().keep()
            .map_err(|e| format!("keep temp script: {e}"))?;

        let (tx, rx) = mpsc::channel(pool_size);

        for _ in 0..pool_size {
            let handle = Self::spawn_worker(&script_path).await?;
            tx.send(handle).await
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
        use tokio::process::Command;
        use std::process::Stdio;

        let mut child = Command::new("python3")
            .arg(script_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| format!("spawn python3: {e}"))?;

        let stdin  = child.stdin.take().ok_or("no stdin on python worker")?;
        let stdout = child.stdout.take().ok_or("no stdout on python worker")?;

        Ok(PythonWorkerHandle {
            stdin:  BufWriter::new(stdin),
            stdout: BufReader::new(stdout),
            child,
        })
    }

    /// Execute a Python UDF task, dispatching to an idle subprocess.
    ///
    /// If the chosen subprocess has died, it is restarted and the task is retried
    /// once. A second failure returns an error.
    pub async fn execute(
        &self,
        action: UdfAction,
        spec: &PythonUdfPayload,
        data: &[u8],
    ) -> Result<Vec<u8>, String> {
        let action_str = udf_action_str(action);
        let fn_b64     = B64.encode(&spec.fn_bytes);
        let zero_b64   = if spec.zero_bytes.is_empty() {
            None
        } else {
            Some(B64.encode(&spec.zero_bytes))
        };
        let data_str = std::str::from_utf8(data)
            .map_err(|e| format!("data utf8: {e}"))?;

        let frame = TaskFrame {
            action: action_str,
            fn_b64,
            data_json: data_str,
            zero_b64,
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
                let mut fresh = Self::spawn_worker(&self.script_path).await
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

fn udf_action_str(action: UdfAction) -> &'static str {
    match action {
        UdfAction::Map          => "map",
        UdfAction::Filter       => "filter",
        UdfAction::FlatMap      => "flat_map",
        UdfAction::MapValues    => "map_values",
        UdfAction::FlatMapValues => "flat_map_values",
        UdfAction::KeyBy        => "key_by",
        UdfAction::Reduce       => "reduce",
        UdfAction::Fold         => "fold",
    }
}

// ── Global singleton ──────────────────────────────────────────────────────────

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

// ── Tests ─────────────────────────────────────────────────────────────────────

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
        let spec = PythonUdfPayload { fn_bytes, zero_bytes: vec![] };
        let data = b"[1, 2, 3]";

        let result = pool
            .execute(UdfAction::Map, &spec, data)
            .await
            .expect("map succeeded");

        let parsed: Vec<i64> = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed, vec![2, 4, 6]);
    }
}
