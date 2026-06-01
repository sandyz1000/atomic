# Deployment Guide

This guide covers building, distributing, and operating Atomic in a multi-machine cluster.

---

## 1. Build a static binary

Atomic uses `atomic-cli` to cross-compile and ship binaries to remote workers.

```bash
# Install the CLI tool
cargo install --path crates/atomic-cli

# Build a static Linux x86_64 binary (default target)
atomic build

# Build for a specific target
atomic build --target aarch64-unknown-linux-musl

# Build with optional features
atomic build --features tls,s3
```

`atomic build` uses [`cargo-zigbuild`](https://github.com/rust-cross/cargo-zigbuild) (auto-installed if absent) to produce a statically-linked musl binary. The result is in `target/<target>/release/`.

---

## 2. Ship to workers

```bash
# Upload to one worker (SSH key from agent or ~/.ssh/id_ed25519)
atomic ship --workers user@10.0.0.101

# Upload to multiple workers
atomic ship --workers user@10.0.0.101,user@10.0.0.102,user@10.0.0.103

# Build + ship in one step
atomic submit --workers user@10.0.0.101,user@10.0.0.102
```

The `ship` command:
1. Verifies the remote host against `~/.ssh/known_hosts` (rejects unknown hosts)
2. Uploads the binary via SFTP to `<path>.tmp`
3. Verifies the SHA-256 checksum on the remote
4. Renames atomically to the final path

---

## 3. Start workers

Workers are started by running the same binary with `--worker`:

```bash
# Foreground worker on port 10001
./my_app --worker --port 10001 --local-ip 10.0.0.101

# With environment variables
ATOMIC_DEPLOYMENT_MODE=worker ATOMIC_WORKER_PORT=10001 ./my_app
```

Typical systemd unit:

```ini
[Unit]
Description=Atomic Worker

[Service]
ExecStart=/opt/atomic/my_app --worker --port 10001 --local-ip %H
Restart=on-failure
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

---

## 4. Configure the driver

In your Python or Rust driver, specify the worker addresses:

**Python:**
```python
import os
os.environ["ATOMIC_DEPLOYMENT_MODE"] = "distributed"
os.environ["ATOMIC_LOCAL_IP"] = "10.0.0.100"
os.environ["ATOMIC_WORKERS"] = "10.0.0.101:10001,10.0.0.102:10001"

import atomic_compute
ctx = atomic_compute.Context()
```

**Rust:**
```rust
use atomic_compute::env::Config;

let config = Config::builder()
    .local_ip("10.0.0.100".parse()?)
    .workers(vec![
        "10.0.0.101:10001".parse()?,
        "10.0.0.102:10001".parse()?,
    ])
    .build();
let ctx = Context::new_with_config(config)?;
```

---

## 5. S3 object store

Requires the `s3` feature (`atomic build --features s3`).

Set standard AWS credentials:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
# Or use an IAM instance role — no env vars needed
```

Then use `s3://` URIs in your jobs:

```python
rdd = ctx.text_file("s3://my-bucket/data/input/")
rdd.save_as_text_file("s3://my-bucket/data/output/")
```

```rust
ctx.text_file("s3://my-bucket/data/input/")?.save_as_text_file("s3://my-bucket/data/output/")?;
```

---

## 6. mTLS for worker communication

Requires the `tls` feature.

Generate certificates with your preferred CA (example using `cfssl`):

```bash
# Generate CA + worker certs
cfssl gencert -initca ca-csr.json | cfssljson -bare ca
cfssl gencert -ca=ca.pem -ca-key=ca-key.pem -config=config.json worker-csr.json | cfssljson -bare worker
```

Configure each process:

```bash
# Workers and driver both need the same CA cert + their own cert/key
export ATOMIC_TLS_CA_CERT=/etc/atomic/ca.pem
export ATOMIC_TLS_CERT=/etc/atomic/worker.pem
export ATOMIC_TLS_KEY=/etc/atomic/worker-key.pem
```

Or in Rust:

```rust
let config = Config::builder()
    .local_ip("10.0.0.100".parse()?)
    .workers(vec!["10.0.0.101:10001".parse()?])
    .build();
// Set tls_ca_cert, tls_cert, tls_key fields or use ATOMIC_TLS_* env vars
```

---

## 7. Prometheus metrics

Enable metrics on the driver:

```python
os.environ["ATOMIC_METRICS_PORT"] = "9090"
```

Metrics are served at `http://driver:9090/metrics`. Scrape with Prometheus and visualize in Grafana.

---

## 8. Graceful shutdown

```python
# From the Python driver script
ctx.stop()
```

This sends a graceful-shutdown signal to every registered worker and clears the driver's shuffle infrastructure. Workers finish their current task before exiting.

In Rust, per-job cancellation is also available via the `Context` API:

```rust
// Rust only — not available in Python or TypeScript bindings
let job_id = ctx.submit_job(...)?;
ctx.cancel_job(job_id)?;
```
