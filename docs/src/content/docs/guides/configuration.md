---
title: Configuration Reference
description: All ATOMIC_* environment variables and Config struct fields.
---

Atomic is configured either via environment variables (`ATOMIC_*`) or by constructing a [`Config`](https://docs.rs/atomic-compute) struct explicitly in Rust.

The `Config::builder()` fluent API maps directly to the env vars below.

---

## Core settings

| Env var | Config field | Default | Description |
|---|---|---|---|
| `ATOMIC_DEPLOYMENT_MODE` | `mode` | `local` | `local` or `distributed` |
| `ATOMIC_LOCAL_IP` | `local_ip` | `127.0.0.1` | IP address this process binds to (shuffle server, worker registration) |
| `ATOMIC_WORK_DIR` | `work_dir` | OS temp dir | Directory for shuffle spill files and RDD cache |
| `ATOMIC_SHUFFLE_PORT` | `shuffle_port` | OS-assigned | Port for the shuffle HTTP server |
| `ATOMIC_WORKERS` | `workers` | `[]` | Comma-separated `ip:port` list of remote worker addresses |

---

## Shuffle & memory

| Env var | Config field | Default | Description |
|---|---|---|---|
| `ATOMIC_SHUFFLE_SPILL_THRESHOLD` | `shuffle_spill_threshold` | `None` (no spill) | Bytes of in-memory shuffle data before spilling to disk |
| `ATOMIC_COALESCE_SHUFFLE_THRESHOLD_BYTES` | `coalesce_shuffle_threshold_bytes` | `0` (disabled) | Adaptively coalesce small shuffle partitions when the stage total is below this threshold |

---

## Observability

| Env var | Config field | Default | Description |
|---|---|---|---|
| `ATOMIC_METRICS_PORT` | `metrics_port` | `None` (disabled) | Port to expose `GET /metrics` in Prometheus text format |
| `ATOMIC_LOG_LEVEL` | `log.log_level` | `info` | Log level: `error`, `warn`, `info`, `debug`, `trace` |

---

## Reliability

| Env var | Config field | Default | Description |
|---|---|---|---|
| `ATOMIC_SPECULATION_MULTIPLIER` | `speculation_multiplier` | `None` (disabled) | When set (e.g. `1.5`), tasks running longer than `multiplier × median_duration` after 50% of stage completes are speculatively re-run |
| `ATOMIC_HEARTBEAT_INTERVAL_SECS` | `heartbeat_interval_secs` | `0` (disabled) | How often the driver probes each worker's `/health` endpoint |
| `ATOMIC_HEARTBEAT_TIMEOUT_MS` | `heartbeat_timeout_ms` | `2000` | Per-probe timeout in milliseconds |

---

## TLS (mTLS for worker communication)

Requires the `tls` feature flag (`cargo build --features tls`).

| Env var | Config field | Default | Description |
|---|---|---|---|
| `ATOMIC_TLS_CA_CERT` | `tls_ca_cert` | `None` | Path to the cluster CA certificate (PEM). Setting this enables TLS. |
| `ATOMIC_TLS_CERT` | `tls_cert` | `None` | Path to this process's certificate (PEM). Required when CA cert is set. |
| `ATOMIC_TLS_KEY` | `tls_key` | `None` | Path to this process's private key (PEM). Required when CA cert is set. |

---

## Config builder (Rust)

```rust
use atomic_compute::env::Config;

let config = Config::builder()
    .local_ip("10.0.0.100".parse()?)
    .workers(vec!["10.0.0.101:10001".parse()?])
    .metrics_port(9090)
    .speculation_multiplier(1.5)
    .shuffle_spill_threshold(512 * 1024 * 1024)  // 512 MB
    .work_dir("/data/atomic-tmp")
    .build();
```

---

## Key/value API

For dynamic configuration from a properties file or CLI flags:

```rust
let config = Config::builder()
    .set("local_ip", "10.0.0.100")
    .set("metrics_port", "9090")
    .set("speculation_multiplier", "1.5")
    .build();
```

Supported keys: `work_dir`, `local_ip`, `shuffle_port`, `metrics_port`, `speculation_multiplier`, `heartbeat_interval_secs`, `heartbeat_timeout_ms`.

---

## Worker-specific settings

Workers are started by running the same binary with `--worker --port N`:

```bash
./my_app --worker --port 10001 --local-ip 10.0.0.101
```

Or via env vars:

```bash
ATOMIC_DEPLOYMENT_MODE=worker ATOMIC_WORKER_PORT=10001 ./my_app
```

The `AtomicApp::build()` helper parses these automatically:

```rust
let app = AtomicApp::build().await?;
let ctx = app.driver_context()?;  // driver path
// Worker path: the process never returns from AtomicApp::build() in worker mode
```
