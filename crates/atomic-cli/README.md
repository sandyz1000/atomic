# atomic-cli

Command-line tool for building and distributing Atomic worker binaries to remote hosts.

## Overview

`atomic-cli` cross-compiles your project for Linux targets using `cargo-zigbuild` (no Docker required), ships the binary to remote workers over secure SSH/SFTP, and writes a cluster config that the driver can read to discover workers.

It does **not** start or stop worker processes — worker lifecycle is managed separately.

## Prerequisites

- **Rust toolchain** installed
- **Zig** installed for cross-compilation: https://ziglang.org/download/
- **`cargo-zigbuild`** — installed automatically on first `build` if missing
- Remote workers accessible via SSH with public-key authentication
- Worker hosts already in `~/.ssh/known_hosts` (unknown hosts are rejected)

```sh
# Add a worker to known_hosts before shipping:
ssh-keyscan -H <worker-host> >> ~/.ssh/known_hosts
```

## Installation

```sh
cargo install --path crates/atomic-cli
```

Or run directly from the workspace:

```sh
cargo run -p atomic-cli -- <command>
```

## Commands

### `build` — Cross-compile for a Linux target

```sh
atomic build [--target <triple>] [--release] [-- <extra cargo args>]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--target` | `x86_64-unknown-linux-musl` | Target triple passed to `cargo zigbuild` |
| `--release` | `true` | Build in release mode |

**Example:**

```sh
atomic build
# → target/x86_64-unknown-linux-musl/release/<binary>

atomic build --target aarch64-unknown-linux-musl
```

---

### `ship` — Upload a binary to remote workers

Connects to each worker over SSH, uploads the binary via SFTP to a temporary path, verifies the SHA-256 checksum on the remote, then atomically renames it to the final destination.

```sh
atomic ship --workers <host1,host2,...> --binary <path> [options]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--workers` | *(required)* | Comma-separated worker hostnames or IPs |
| `--binary` | *(required)* | Local path to the compiled binary |
| `--remote-path` | `/opt/atomic/worker` | Absolute path on each worker |
| `--user` | `ubuntu` | SSH username |
| `--key` | auto-detect | SSH private key (`~/.ssh/id_ed25519`, `id_ecdsa`, `id_rsa` tried in order) |
| `--port` | `10000` | Worker port recorded in the cluster config |

After shipping, worker addresses are written to `.atomic/cluster.toml` so the driver can discover them automatically.

**Example:**

```sh
atomic ship \
  --workers 10.0.0.1,10.0.0.2,10.0.0.3 \
  --binary target/x86_64-unknown-linux-musl/release/my_app \
  --user ec2-user \
  --key ~/.ssh/my_cluster_key \
  --remote-path /home/ec2-user/atomic-worker
```

**Security guarantees:**

- Host keys are verified against `~/.ssh/known_hosts` — unknown hosts are rejected (no `StrictHostKeyChecking=no`).
- Host key mismatch triggers a hard error with instructions to investigate before proceeding.
- The binary is written to `<remote-path>.tmp` and only moved to the final path after SHA-256 is verified on the remote, so the final destination always contains a complete, untampered binary.
- The SSH private key never appears in a shell command or process argument list.

---

### `submit` — Build then ship in one step

```sh
atomic submit --workers <host1,host2,...> [options] [-- <driver args>]
```

Runs `build --release`, locates the output binary, calls `ship`, then runs the driver binary locally with `--driver --workers <addrs>`.

| Flag | Default | Description |
|------|---------|-------------|
| `--workers` | *(optional)* | If omitted, reads `.atomic/cluster.toml` |
| `--target` | `x86_64-unknown-linux-musl` | Cross-compilation target |
| `--remote-path` | `/opt/atomic/worker` | Destination path on workers |
| `--user` | `ubuntu` | SSH username |
| `--key` | auto-detect | SSH private key |
| `--port` | `10000` | Worker port |

**Example:**

```sh
atomic submit \
  --workers 10.0.0.1,10.0.0.2 \
  -- --my-app-flag value
```

---

### `stop` — Send a shutdown signal to workers

Sends a graceful shutdown frame over TCP to each worker.

```sh
atomic stop [--workers <host:port,...>]
```

If `--workers` is omitted, addresses are read from `.atomic/cluster.toml`.

**Example:**

```sh
atomic stop
# reads from .atomic/cluster.toml

atomic stop --workers 10.0.0.1:10000,10.0.0.2:10000
```

---

## Cluster Config

`.atomic/cluster.toml` is written by `ship` and `submit`. The driver reads it to discover workers.

```toml
[[workers]]
address = "10.0.0.1:10000"

[[workers]]
address = "10.0.0.2:10000"
```

## Typical Workflow

```sh
# 1. Add workers to known_hosts
ssh-keyscan -H 10.0.0.1 10.0.0.2 >> ~/.ssh/known_hosts

# 2. Cross-compile
atomic build

# 3. Ship to workers
atomic ship \
  --workers 10.0.0.1,10.0.0.2 \
  --binary target/x86_64-unknown-linux-musl/release/my_app

# 4. Start workers on each host (out of band — systemd, SSH, etc.)

# 5. Run the driver locally
./target/release/my_app --driver --workers 10.0.0.1:10000,10.0.0.2:10000

# 6. Stop workers when done
atomic stop
```

Or use `submit` to collapse steps 2–3 and 5 into one command:

```sh
atomic submit --workers 10.0.0.1,10.0.0.2
```
