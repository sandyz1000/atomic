# Atomic Project Notes

## Project Goal

Atomic is a stable-Rust rewrite and refactor of Vega.

- Preserve the useful high-level execution model from Vega.
- Remove dependence on unstable Rust assumptions.
- Replace trait-object and function serialization patterns with explicit artifact execution.
- Use rkyv for distributed serialization and deserialization.

## Repository Structure

- `crates/atomic-data`: shared execution data structures, task types, registry, distributed envelopes.
- `crates/atomic-compute`: execution runtime, worker environment, executor, backend implementations.
- `crates/atomic-scheduler`: DAG building, stage planning, job tracking, local and distributed scheduling.
- `crates/atomic-utils`: shared utilities.
- `notes/`: architecture notes and design documents.

## Architecture Rules

### Serialization

- Use rkyv for distributed wire payloads.
- Do not reintroduce generic closure serialization for distributed execution.
- Do not depend on Cap'n Proto or Vega-style serializable function wrappers for new distributed work.

### Execution Model

- Local mode runs work on threads in process.
- Distributed mode is artifact-first.
- The scheduler must pick one execution backend for a job or stage.
- Workers must be configured for one runtime backend instead of probing multiple backends at task execution time.

### Backends

- `LocalThread`: in-process local execution.
- `Docker`: distributed execution using immutable container images.
- `Wasm`: distributed execution using prebuilt WASM modules.

## Distributed Contract

Distributed tasks are described with `atomic_data::distributed`.

- `ExecutionBackend`: explicit backend target.
- `ArtifactDescriptor`: immutable artifact metadata including digest and build target.
- `TaskEnvelope`: rkyv-encoded task metadata, payload, and partition bytes.
- `TaskResultEnvelope`: rkyv-encoded result or failure.
- `WorkerCapabilities`: worker placement contract.

## Docker Direction

Docker tasks should:

1. Reference a concrete image digest.
2. Decode task payload data on the worker.
3. Launch a container with the requested command and environment.
4. Stream logs.
5. Return structured success or failure data.

## WASM Direction

WASM support is build-time driven.

1. Compile task logic into a WASM artifact ahead of time.
2. Publish or register the artifact with a stable digest.
3. Reference that module from the scheduler.
4. Execute the module on the worker.

Workers should not compile Rust into WASM at task runtime.

## Current Implementation State

### Done

- rkyv distributed envelope types exist.
- task registry exists.
- explicit runtime selection exists in compute.
- distributed worker configuration now carries a backend choice.
- Docker backend lifecycle is implemented with bollard.
- scheduler design note reflects the artifact-first model.

### Not Done Yet

- distributed scheduler placement using `WorkerCapabilities`.
- end-to-end rkyv transport integration in the distributed scheduler.
- WASM runtime execution backend.
- build tooling for WASM artifacts and manifests.

## Guardrails For Future Changes

- Reuse Vega's DAG, stage, and shuffle ideas when they fit.
- Do not copy Vega's closure serialization model.
- Keep local and distributed execution paths conceptually aligned, but allow different physical task representations.
- Prefer immutable artifact references and explicit backend routing over dynamic runtime guessing.