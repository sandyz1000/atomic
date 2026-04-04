# ── Stage 1: build ─────────────────────────────────────────────────────────────
# Build from the repo root so the relative path `crates/atomic-runtime` resolves.
# Run: docker build -t docker-sum-task:latest -f examples/docker_sum_task/Dockerfile .
FROM rust:1.85-slim AS builder

WORKDIR /build
COPY . .

RUN cargo build --release --manifest-path examples/docker_sum_task/Cargo.toml

# ── Stage 2: minimal runtime image ─────────────────────────────────────────────
FROM debian:bookworm-slim

COPY --from=builder /build/target/release/docker-sum-task /usr/local/bin/docker-sum-task

ENTRYPOINT ["docker-sum-task"]
