# docker-json-task

A Docker task designed for **Python clients** using `atomic-python`'s `PyDockerStub`.

`PyDockerStub` encodes Python partition data as JSON (rather than rkyv), so this
task uses a manually-written JSON framing loop instead of the `#[task]` macro.

## Why JSON?

`PyDockerStub` in `atomic-python` converts Python objects using `json.dumps`, making
them compatible with any language or tool that speaks JSON. The frame protocol is
identical — only the payload encoding changes:

| Client | Payload encoding | Task type |
|---|---|---|
| Rust (`TypedRdd`) | rkyv | `docker_sum_task` (`#[task]`) |
| Python (`PyDockerStub`) | JSON | `docker_json_task` (this crate) |

## Build

```bash
# Can be built standalone (no full repo copy needed):
docker build -t docker-json-task:latest \
  -f examples/docker_json_task/Dockerfile .
```

## Run with the Python client

```bash
# After building the atomic Python wheel:
cd crates/atomic-python && maturin develop --release
cd ../..
python examples/python/docker_sum_job.py
# Sum of 1..12 = 78.0
```
