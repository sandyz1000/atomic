/// Docker task that receives and returns JSON via the atomic framing protocol.
///
/// Compatible with `atomic-python`'s `PyDockerStub`, which JSON-encodes Python
/// objects and uses the same 4-byte LE length-prefix framing as the rkyv path.
///
/// Frame format (identical to rkyv tasks):
///   [4-byte LE u32 = payload length]   (0 = graceful shutdown)
///   [N bytes JSON payload]
///
/// Input:  JSON array of numbers, e.g. `[1, 2, 3, 4]`
/// Output: JSON array with a single number (the sum), e.g. `[10]`
///
/// Returning a single-element array lets `PyDockerStub.execute_partitions`
/// decode the result as a one-element Python list that is then flattened into
/// the output RDD — one sum per partition.
use std::io::{Read, Write};

fn main() {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut stdin = stdin.lock();
    let mut stdout = stdout.lock();

    loop {
        // Read 4-byte LE frame length.
        let mut len_buf = [0u8; 4];
        if stdin.read_exact(&mut len_buf).is_err() {
            break;
        }
        let frame_len = u32::from_le_bytes(len_buf) as usize;
        if frame_len == 0 {
            break; // Graceful shutdown signal
        }

        // Read JSON payload.
        let mut frame = vec![0u8; frame_len];
        if stdin.read_exact(&mut frame).is_err() {
            break;
        }

        // Parse JSON array of numbers and sum them.
        let numbers: Vec<serde_json::Value> =
            serde_json::from_slice(&frame).unwrap_or_default();
        let total: f64 = numbers
            .iter()
            .filter_map(|v| v.as_f64())
            .sum();

        // Return as a JSON array so PyDockerStub decodes it as a Python list.
        let output = serde_json::to_vec(&[total]).expect("json encode");

        // Write 4-byte LE length prefix + payload.
        stdout
            .write_all(&(output.len() as u32).to_le_bytes())
            .expect("write len");
        stdout.write_all(&output).expect("write payload");
        stdout.flush().expect("flush");
    }
}
