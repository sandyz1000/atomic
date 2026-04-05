# ── Stage 1: build ─────────────────────────────────────────────────────────────
# Run: docker build -t docker-json-task:latest -f examples/docker_json_task/Dockerfile .
FROM rust:1.85-slim AS builder

WORKDIR /build
# Only the task crate is needed — no atomic-runtime dep, so no full repo copy.
COPY examples/docker_json_task/ examples/docker_json_task/

RUN cargo build --release --manifest-path examples/docker_json_task/Cargo.toml

# ── Stage 2: minimal runtime image ─────────────────────────────────────────────
FROM debian:bookworm-slim

COPY --from=builder /build/target/release/docker-task /usr/local/bin/docker-task

ENTRYPOINT ["docker-task"]
