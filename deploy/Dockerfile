# Atomic — single-image build for driver + worker.
#
# The whole point of Atomic's compile-time dispatch is that ONE binary plays both
# roles: run it plainly for the driver, or with `--worker --port N` for a worker.
# So this image contains exactly one binary and you choose the role at runtime
# (see deploy/helm/atomic). The `REGISTRY_FINGERPRINT` handshake guarantees every
# pod from this image runs byte-identical task code — which is exactly the
# invariant a Kubernetes rolling update of an immutable image gives you.
#
# Build your own app by overriding BIN:
#   docker build -f deploy/Dockerfile --build-arg BIN=my_app -t myrepo/atomic:0.1 .
# BIN must be a workspace package with a [[bin]] of the same name that calls
# `AtomicApp::build()` (all the examples do). Defaults to the `joins` example so
# the image is runnable out of the box.

# ── Build stage ───────────────────────────────────────────────────────────────
FROM rust:1.84-slim-bookworm AS build

# Native deps some crates need at build time (openssl/tls, pkg-config, cmake for
# any vendored C). Kept minimal; drop what your app doesn't use.
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev cmake clang \
    && rm -rf /var/lib/apt/lists/*

ARG BIN=joins
WORKDIR /src
COPY . .

# Build only the requested binary in release mode.
RUN cargo build --release -p "${BIN}" \
    && cp "target/release/${BIN}" /atomic-bin

# ── Runtime stage ─────────────────────────────────────────────────────────────
# Distroless/cc: glibc + libssl present, no shell/package manager. Small and
# robust for binaries with native deps. For a pure-Rust app you can swap this for
# `gcr.io/distroless/static-debian12` (build the BIN with the musl target first).
FROM gcr.io/distroless/cc-debian12

COPY --from=build /atomic-bin /usr/local/bin/atomic-app

# Worker task port (TCP) and shuffle/health + metrics HTTP ports. Match these to
# values.yaml in the Helm chart.
EXPOSE 10001 9090

# Default to driver mode; the Helm worker StatefulSet overrides args with
# ["--worker", "--port", "10001"].
ENTRYPOINT ["/usr/local/bin/atomic-app"]
