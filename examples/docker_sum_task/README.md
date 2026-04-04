# docker-sum-task

A Docker task that sums all byte values in a partition using the `#[task]` macro.

The `#[task]` attribute on a native target generates a `main()` that calls
`atomic_runtime::serve()` — a stdin/stdout framing loop that keeps the container
alive between calls, eliminating cold-start overhead after the first invocation.

## Frame protocol

```
[4-byte LE u32 = payload length]   (0 = graceful shutdown)
[N bytes rkyv-encoded payload]
```

Both input (`Vec<u8>`) and output (`Vec<u8>`) are rkyv-encoded.

## Build

Run from the **repo root** (so the relative `atomic-runtime` path resolves):

```bash
docker build -t docker-sum-task:latest \
  -f examples/docker_sum_task/Dockerfile .
```

## Test manually

```bash
# Write a 4-byte LE length prefix (10 bytes payload) followed by rkyv-encoded Vec<u8>
# Then send the 0-length shutdown frame.
# The easiest way is to use the Rust client example:
cargo run --example docker_sum
```

## Run with the Rust client example

```bash
# Build the image first, then:
cargo run --example docker_sum
# grand total = 170
```
