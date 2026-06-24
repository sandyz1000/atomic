---
title: Roadmap
description: What is shipped, what is deliberately out of scope, and what may come next.
---

Atomic's v1.0 is complete: a distributed RDD engine with SQL, streaming,
structured streaming, graph processing, language bindings, and a release
pipeline. For the per-change history of what shipped, see
[`CHANGELOG.md`](https://github.com/sandyz1000/atomic/blob/main/CHANGELOG.md).

This page covers the forward-looking parts: what is intentionally not built, and
what may come next.

## Deliberate non-goals

These look like gaps but are design boundaries. Each has a supported
alternative, and closing it would conflict with the project's rule against
serializing closures for distributed work.

- **Push / receiver-model source distribution.** Only the direct-pull model
  (Kafka, files, any offset-addressable source) distributes across workers
  through `DistributedSource`. Custom receivers and socket fan-in stay
  driver-local.
- **Multiple watermarks per query.** One monotonic global watermark per query.
  Sliding and session windows and stream-stream joins share it.
- **Closure `sort_by` and closure custom partitioner distribution.** Closures
  cannot serialize across workers. The registered-task paths `sort_by_task` and
  `partition_by_named` / `TypedPartitioner` are the distributed equivalents.

## Candidate features

Capabilities worth building next:

- **Web dashboard** — job and stage timeline UI. Prometheus plus Grafana is the
  current recommendation.
- **Kinesis source** — follow-on from the Kafka source.
- **GCS and Azure Blob connectors** — S3 covers AWS; these add multi-cloud parity.
- **`map_with_state`** — arbitrary stateful streaming beyond `update_state_by_key`.
- **Broadcast join, sort-merge join, skew handling** — for very large shuffles
  and complex joins.
- **Kubernetes CRD operator** — a controller for declarative cluster management
  beyond the Helm chart.
- **NLQ CI integration test** — end-to-end test requiring `OPENAI_API_KEY`.
- **Streaming NLQ** — natural-language queries over micro-batch streams.

## Out of scope

- Kerberos / SASL authentication
- HDFS connector — S3 covers the primary cloud use case
