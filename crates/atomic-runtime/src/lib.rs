/// Re-export the `#[task]` attribute macro.
///
/// # Example
/// ```ignore
/// #[atomic_runtime::task]
/// fn run_map(input: Vec<u8>) -> Vec<u8> {
///     input.into_iter().map(|x| x + 1).collect()
/// }
/// ```
pub use atomic_runtime_macros::task;
#[cfg(not(target_arch = "wasm32"))]
use rkyv::{
    api::high::{HighSerializer, HighValidator}, bytecheck::CheckBytes, de::Pool, rancor::{Error, Strategy}, ser::allocator::ArenaHandle, util::AlignedVec
};

/// Re-export `serde_json` so macro-generated JSON task code can reference it
/// via `atomic_runtime::serde_json` without adding a direct dependency.
pub use serde_json;

/// Internals used by macro-generated WASM code. Not part of the public API.
#[cfg(target_arch = "wasm32")]
pub mod __internal;

/// Internals used by macro-generated WASM code (stub for non-wasm builds).
#[cfg(not(target_arch = "wasm32"))]
pub mod __internal {
    /// Placeholder — only compiled on wasm32 targets.
    #[doc(hidden)]
    pub unsafe fn call_wasm<I, O, F>(_ptr: i32, _len: i32, _handler: F) -> i64
    where
        F: Fn(I) -> O,
    {
        unreachable!("call_wasm is only available on wasm32 targets")
    }

    /// Placeholder — only compiled on wasm32 targets.
    #[doc(hidden)]
    pub unsafe fn call_wasm_json<I, O, F>(_ptr: i32, _len: i32, _handler: F) -> i64
    where
        F: Fn(I) -> O,
    {
        unreachable!("call_wasm_json is only available on wasm32 targets")
    }
}

/// Run a task handler in a stdin/stdout framing loop.
///
/// This is the entrypoint for Docker task containers. The container binary
/// compiled with `#[task]` (native target) calls `serve(handler)` from `main()`.
/// The worker keeps the container alive and sends multiple task invocations
/// through the same process — no cold-start overhead after the first call.
///
/// # Frame format
/// ```text
/// [4 bytes LE u32 = payload length]   (0 = graceful shutdown)
/// [N bytes rkyv-encoded input]
/// ```
///
/// The handler is called with the decoded `I` and the result `O` is
/// rkyv-encoded and written back with the same framing.
#[cfg(not(target_arch = "wasm32"))]
pub fn serve<I, O, F>(handler: F)
where
    I: rkyv::Archive,
    <I as rkyv::Archive>::Archived: for<'a> CheckBytes<HighValidator<'a, Error>>
        + rkyv::Deserialize<I, Strategy<Pool, Error>>,
    O: for<'a> rkyv::Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, Error>>,
    F: Fn(I) -> O,
{
    use std::io::{Read, Write};
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut stdin = stdin.lock();
    let mut stdout = stdout.lock();

    loop {
        // Read 4-byte LE frame length.
        let mut len_buf = [0u8; 4];
        if stdin.read_exact(&mut len_buf).is_err() {
            break; // EOF — driver closed the pipe
        }
        let frame_len = u32::from_le_bytes(len_buf) as usize;
        if frame_len == 0 {
            break; // Explicit graceful shutdown signal
        }

        // Read rkyv-encoded input frame.
        let mut frame = vec![0u8; frame_len];
        if stdin.read_exact(&mut frame).is_err() {
            break;
        }

        let input: I =
            rkyv::from_bytes::<I, Error>(&frame).expect("atomic-runtime: rkyv decode failed");

        let output = handler(input);

        let out_bytes = rkyv::to_bytes::<Error>(&output)
            .expect("atomic-runtime: rkyv encode failed")
            .to_vec();

        // Write 4-byte LE frame length then payload.
        stdout
            .write_all(&(out_bytes.len() as u32).to_le_bytes())
            .expect("atomic-runtime: write len failed");
        stdout
            .write_all(&out_bytes)
            .expect("atomic-runtime: write payload failed");
        stdout.flush().expect("atomic-runtime: flush failed");
    }
}

/// Like [`serve`] but uses JSON (serde_json) instead of rkyv.
///
/// Use this for Docker tasks that need to be callable from Python or JavaScript,
/// where callers cannot produce rkyv-encoded bytes but can produce JSON naturally.
/// Pair with `#[task(json)]` in the task crate.
///
/// # Frame format
/// ```text
/// [4 bytes LE u32 = payload length]   (0 = graceful shutdown)
/// [N bytes JSON-encoded input]
/// ```
///
/// The handler result is JSON-encoded and written back with the same framing.
#[cfg(not(target_arch = "wasm32"))]
pub fn serve_json<I, O, F>(handler: F)
where
    I: serde::de::DeserializeOwned,
    O: serde::Serialize,
    F: Fn(I) -> O,
{
    use std::io::{Read, Write};
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut stdin = stdin.lock();
    let mut stdout = stdout.lock();

    loop {
        let mut len_buf = [0u8; 4];
        if stdin.read_exact(&mut len_buf).is_err() {
            break;
        }
        let frame_len = u32::from_le_bytes(len_buf) as usize;
        if frame_len == 0 {
            break;
        }

        let mut frame = vec![0u8; frame_len];
        if stdin.read_exact(&mut frame).is_err() {
            break;
        }

        let input: I =
            serde_json::from_slice(&frame).expect("atomic-runtime: json decode failed");

        let output = handler(input);

        let out_bytes =
            serde_json::to_vec(&output).expect("atomic-runtime: json encode failed");

        stdout
            .write_all(&(out_bytes.len() as u32).to_le_bytes())
            .expect("atomic-runtime: write len failed");
        stdout
            .write_all(&out_bytes)
            .expect("atomic-runtime: write payload failed");
        stdout.flush().expect("atomic-runtime: flush failed");
    }
}
