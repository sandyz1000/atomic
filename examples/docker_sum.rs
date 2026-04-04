/// End-to-end Docker task example (Rust client).
///
/// Demonstrates how the atomic 4-byte framing protocol works: the same protocol
/// that `atomic_runtime::serve()` implements inside Docker containers built with
/// the `#[task]` macro.
///
/// Prerequisites:
///   docker build -t docker-sum-task:latest \
///     -f examples/docker_sum_task/Dockerfile .
///
/// Run:
///   cargo run --example docker_sum
use std::io::{Read, Write};
use std::process::{Command, Stdio};

use rkyv::rancor::Error as RkyvError;

/// Send one partition to the Docker container and return the rkyv-decoded result.
///
/// Frame protocol (same as `atomic_runtime::serve`):
///   SEND  [4-byte LE u32 = payload length][rkyv bytes]
///   SEND  [4-byte LE u32 = 0]              ← graceful shutdown
///   RECV  [4-byte LE u32 = result length][rkyv bytes]
fn call_docker_task(image: &str, partition: Vec<u8>) -> Vec<u8> {
    let encoded = rkyv::to_bytes::<RkyvError>(&partition).expect("rkyv encode");
    let frame_len = (encoded.len() as u32).to_le_bytes();

    let mut child = Command::new("docker")
        .args(["run", "--rm", "-i", image])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit()) // pass container logs to terminal
        .spawn()
        .expect("failed to spawn docker — is Docker running and image built?");

    {
        let stdin = child.stdin.as_mut().expect("stdin");
        stdin.write_all(&frame_len).expect("write frame len");
        stdin.write_all(&encoded).expect("write payload");
        // Shutdown frame: 4-byte zero → serve() loop exits gracefully
        stdin.write_all(&0u32.to_le_bytes()).expect("write shutdown");
    }
    drop(child.stdin.take());

    let mut stdout = child.stdout.take().expect("stdout");
    let mut len_buf = [0u8; 4];
    stdout.read_exact(&mut len_buf).expect("read result len");
    let out_len = u32::from_le_bytes(len_buf) as usize;
    let mut out_bytes = vec![0u8; out_len];
    stdout.read_exact(&mut out_bytes).expect("read result payload");
    child.wait().expect("docker wait");

    rkyv::from_bytes::<Vec<u8>, RkyvError>(&out_bytes).expect("rkyv decode result")
}

fn main() {
    let image = "docker-sum-task:latest";

    // Simulate three partitions of data
    let partitions: Vec<Vec<u8>> = vec![
        vec![1, 2, 3, 4],    // sum = 10
        vec![10, 20, 30],    // sum = 60
        vec![100],           // sum = 100
    ];

    println!("Dispatching {} partitions to Docker image {image}", partitions.len());

    let grand_total: u32 = partitions
        .into_iter()
        .enumerate()
        .map(|(i, partition)| {
            let result_bytes = call_docker_task(image, partition);
            // Task returns 4-byte LE u32
            let sum = u32::from_le_bytes(
                result_bytes.try_into().expect("expected 4-byte result"),
            );
            println!("  partition {i}: sum = {sum}");
            sum
        })
        .sum();

    println!("grand total = {grand_total}"); // 170
}
