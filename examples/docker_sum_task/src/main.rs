use atomic_runtime::task;

/// Sum all byte values in the partition and return a 4-byte LE u32.
///
/// Input:  rkyv-encoded `Vec<u8>` — one partition's worth of data.
/// Output: rkyv-encoded `Vec<u8>` containing 4 bytes (the LE u32 sum).
///
/// The `#[task]` macro generates `fn main()` that calls `atomic_runtime::serve()`,
/// which runs a stdin/stdout framing loop:
///
///   [4-byte LE u32 = payload length]  (0 = graceful shutdown)
///   [N bytes rkyv-encoded payload]
///
/// The container stays alive between calls — no cold-start cost after the first.
#[task]
fn run_sum(data: Vec<u8>) -> Vec<u8> {
    let total: u32 = data.iter().map(|&b| b as u32).sum();
    total.to_le_bytes().to_vec()
}
