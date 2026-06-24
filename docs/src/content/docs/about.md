---
title: About Atomic
description: What Atomic is, the problems it solves, and its design goals.
---

Atomic is a distributed compute engine written in stable Rust. It runs
partition-based data jobs either on a thread pool in a single process or across
a cluster of worker machines, using the same job code in both cases.

A job is a chain of lazy transformations over a partitioned dataset (an RDD).
Transformations build a directed acyclic graph (DAG); an action triggers
execution. The engine splits the DAG into stages at shuffle boundaries, runs
each partition in parallel, and aggregates results on the driver.

## Why Atomic exists

Distributed data processing usually forces a choice between a fast compiled
language with no cluster runtime, or a managed cluster runtime tied to the JVM.
Atomic provides a compiled-Rust execution core with three language front ends —
Rust, Python, and TypeScript — over one engine.

The design targets three problems:

- **Code distribution without closure serialization.** Rust cannot serialize
  function pointers on stable toolchains. Atomic registers task functions at
  compile time and dispatches them by string identifier, so the driver and
  workers run the same binary and agree on the task table at link time.

- **Wire safety.** Distributed payloads use `rkyv`, validated before use. The
  task registry has a fingerprint that the driver and each worker compare at
  connection time, so a stale worker binary fails at handshake instead of
  producing wrong results.

- **One API across languages.** The Rust, Python, and TypeScript RDD APIs are
  the same surface over the same engine. A job prototyped in Python can have a
  hot path rewritten as a compiled Rust task without changing the driver
  script.

## Design goals

- **Stable Rust only.** No nightly features, no unstable intrinsics.
- **Compile-time task dispatch.** Distributed work is registered with the
  `#[task]` macro and dispatched by identifier, not by shipping closures.
- **Aligned local and distributed paths.** Local mode runs the same
  transformations on a thread pool that distributed mode runs on workers.
- **Explicit configuration.** A `Config` struct is built at the entry point and
  passed to the context; environment variables map onto the same fields.

## What Atomic provides

| Crate | Responsibility |
|---|---|
| `atomic-data` | Core types: RDD traits, wire envelopes, dependencies, partitioners |
| `atomic-compute` | Execution runtime: context, executor, RDD implementations, task registry |
| `atomic-scheduler` | DAG building, stage planning, local and distributed schedulers |
| `atomic-sql` | SQL and DataFrame queries on Apache DataFusion |
| `atomic-streaming` | Micro-batch streaming |
| `atomic-structured` | Continuous SQL/DataFrame queries with windows, joins, and watermarks |
| `atomic-graph` | Graph processing with a Pregel engine and built-in algorithms |
| `atomic-nlq` | Natural-language query layer over the SQL and compute layers |
| `atomic-py` | Python bindings (PyO3 / maturin) |
| `atomic-js` | Node.js bindings (NAPI) |
| `atomic-cli` | Cross-compilation and binary distribution to workers |
| `atomic-k8s` | Kubernetes per-job worker allocation |

## Next steps

- [Getting Started](/guides/getting-started/) — install and run your first job
- [Architecture Overview](/architecture/overview/) — how the engine is built
- [The RDD API](/concepts/rdd-api/) — transformations, actions, and tasks
