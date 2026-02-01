# 🔥 Atomic

## A distributed data processing framework written in Rust

---

## Overview

**atomic** is a distributed data processing framework written in **Rust**, inspired by **Apache Spark** — designed for type-safe, efficient, and resilient data processing.

atomic provides a **Spark-like RDD API** with familiar transformations and actions, backed by a **DAG-based execution engine** that optimizes task scheduling and execution across single machines or distributed clusters.

**Current Features:**

* Type-safe RDD API with lazy evaluation
* DAG-based task scheduling
* Local and distributed execution modes
* Shuffle operations for wide transformations
* Task result tracking and caching
* Deterministic, sandboxed execution
* Architecture portability
* Safety and isolation for untrusted code

This project is heavily inspired by [vega](https://github.com/rajasekarv/vega/tree/master). The original project is no longer maintained. atomic adopts a similar distributed execution model where compiled binaries are distributed to remote nodes, with communication handled via rkyv-based RPC for data and control messages.

---

## ⚙️ Core Architecture

```
┌─────────────────────────────────────────────────────┐
│                   atomic Framework                    │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌────────────┐      ┌─────────────────────────┐   │
│  │  RDD API   │─────▶│   DAG Scheduler         │   │
│  │ (map, etc) │      │  - Job Tracker          │   │
│  └────────────┘      │  - Stage Generation     │   │
│                      │  - Task Scheduling       │   │
│                      └──────────┬──────────────┘   │
│                                 │                   │
│                      ┌──────────▼──────────────┐   │
│                      │   Execution Modes       │   │
│                      ├─────────────────────────┤   │
│                      │  Local Scheduler        │   │
│                      │  - Thread pool exec     │   │
│                      │  - In-process tasks     │   │
│                      └─────────────────────────┘   │
│                                                      │
│  ┌─────────────────────────────────────────────┐   │
│  │         Supporting Components                │   │
│  ├─────────────────────────────────────────────┤   │
│  │  • Shuffle Manager (data exchange)          │   │
│  │  • Cache Tracker (RDD caching)              │   │
│  │  • Dependency Management (narrow/shuffle)   │   │
│  │  • Split/Partition abstraction              │   │
│  └─────────────────────────────────────────────┘   │
│                                                      │
└─────────────────────────────────────────────────────┘
```

### Distributed Execution Model

Atomic's distributed mode follows a **Vega-inspired architecture** where compiled binaries are distributed to worker nodes, and communication happens via **rkyv-based RPC** for efficient serialization of data and control messages.

#### Binary Distribution Strategies

##### Option 1: SSH + rsync/Ansible

* Build your Rust binary once
* Use rsync or Ansible to distribute the binary to remote nodes
* Suitable for static clusters or bare-metal deployments

##### Option 2: Container Registry (Kubernetes, Nomad, ECS)

* Package your atomic-based app into a Docker image (binary + config)
* Deploy the same image across all nodes
* Let the orchestrator handle image distribution

#### Communication Protocol

In distributed mode, atomic nodes communicate over **rkyv RPC** to move:

* **RDD partitions** — Serialized data chunks
* **Control messages** — Heartbeats, task status, errors
* **Task metadata** — Stage info, partition locations

The rkyv crate provides zero-copy deserialization for efficient data transfer between nodes.

#### Python/JavaScript Task Execution

For Python and JavaScript tasks, atomic follows a **Ray-inspired strategy**:

* Language-specific runtimes embedded on worker nodes
* Task definitions serialized and dispatched to appropriate workers
* Results collected and aggregated back to the driver

#### Kubernetes Deployment Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Kubernetes Cluster                      │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌──────────────────────┐                               │
│  │  Driver/Master Pod   │                               │
│  │  ┌────────────────┐  │                               │
│  │  │ DAG Scheduler  │  │◄──── rkyv RPC ────┐          │
│  │  │ Job Tracker    │  │                    │          │
│  │  │ Result Agg     │  │                    │          │
│  │  └────────────────┘  │                    │          │
│  │   (atomic:latest)    │                    │          │
│  └──────────────────────┘                    │          │
│           ▲                                   │          │
│           │                                   │          │
│           │  Service Discovery                │          │
│           │  (env vars, DNS)                  │          │
│           │                                   │          │
│  ┌────────┴───────────────────────────────────┐│          │
│  │                                           ││          │
│  │  ┌──────────────┐    ┌──────────────┐   ││          │
│  │  │ Worker Pod 1 │    │ Worker Pod 2 │   ││          │
│  │  │ ┌──────────┐ │    │ ┌──────────┐ │   ││          │
│  │  │ │ Executor │─┼────┼─│ Executor │ │◄──┘│          │
│  │  │ │ Shuffle  │ │    │ │ Shuffle  │ │    │          │
│  │  │ │ Cache    │ │    │ │ Cache    │ │    │          │
│  │  │ └──────────┘ │    │ └──────────┘ │    │          │
│  │  │ (atomic:latest)    │ (atomic:latest)   │          │
│  │  └──────────────┘    └──────────────┘   │          │
│  │                                           │          │
│  │              Worker Deployment            │          │
│  │         (Deployment/StatefulSet)          │          │
│  └───────────────────────────────────────────┘          │
│                                                          │
└─────────────────────────────────────────────────────────┘

        Container Registry
        ┌──────────────┐
        │ atomic:latest│  ← Build once, pull everywhere
        │ atomic:v1.2.3│
        └──────────────┘
```

**How It Works on Kubernetes:**

1. **Build & Package**
   * Compile your atomic-based application
   * Create a Docker image containing the binary, config, and dependencies
   * Push to a container registry (Docker Hub, GCR, ECR, etc.)

2. **Deploy**
   * **Driver Pod**: Single pod (Deployment or Job) that runs the DAG scheduler
   * **Worker Pods**: Multiple pods (Deployment, StatefulSet, or DaemonSet) that execute tasks
   * All pods use the **same image tag** (e.g., `atomic:latest`)

3. **Service Discovery**
   * Configure Kubernetes Service for the driver pod
   * Use environment variables (e.g., `ATOMIC_DRIVER_HOST`, `ATOMIC_LOCAL_IP`) for node registration
   * Workers discover and connect to the driver via DNS or injected config

4. **Runtime Behavior**
   * **Binary distribution**: Handled automatically by Kubernetes image-pull mechanism
   * **Data transfer**: Only serialized RDD partitions, task metadata, and control messages flow over rkyv RPC
   * **No per-job binary shipping**: Code updates happen via rolling image updates

5. **Scaling**
   * Scale workers via `kubectl scale` or HorizontalPodAutoscaler
   * Driver remains single-instance; workers are stateless (or use StatefulSet for caching)

**Benefits:**

* **Efficient**: No binary shipping at runtime—only data and metadata
* **Standard**: Leverage existing K8s tools for deployment, scaling, and monitoring
* **Versioned**: Image tags provide clear versioning and rollback capability
* **Isolated**: Each pod has resource limits and network isolation

---

## Key Concepts

* **RDD (Resilient Distributed Datasets)** — Immutable, partitioned collections that support transformations and actions
* **Lazy Evaluation** — Transformations are recorded as a DAG and executed only when an action is called
* **Type-Safe API** — Leverages Rust's type system to catch errors at compile time
* **Dependency Tracking** — Narrow (one-to-one, range) and shuffle (wide) dependencies for efficient execution
* **Task Scheduling** — DAG-based scheduler that generates stages and distributes tasks

---

## 🦀 Example: Word Count in atomic

```rust
use atomic::prelude::*;

fn main() -> Result<()> {
    let sc = Context::new("wordcount")?;

    let lines = sc.text_file("data/input.txt", 4)?;
    
    let words = lines.flat_map(|line| {
        line.split_whitespace()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    });
    
    let pairs = words.map(|word| (word, 1));
    let counts = pairs.reduce_by_key(|a, b| a + b, 4)?;
    
    counts.save_as_text_file("output/wordcount")?;
    
    Ok(())
}
```

---

## Core Components

### RDD Operations

**Transformations** (lazy):

* `map` — Apply a function to each element
* `flat_map` — Map and flatten results
* `filter` — Keep elements matching a predicate
* `map_partitions` — Transform entire partitions
* `union` — Combine two RDDs
* `cartesian` — Cartesian product of two RDDs
* `coalesce` — Reduce number of partitions

**Actions** (trigger execution):

* `collect` — Return all elements to driver
* `reduce` — Aggregate elements using a function
* `count` — Count number of elements
* `take` — Return first n elements
* `save_as_text_file` — Write to disk

### Shuffle Operations

For key-value RDDs:

* `reduce_by_key` — Combine values for each key
* `group_by_key` — Group values by key
* `join` — Inner join two RDDs by key
* `co_group` — Group multiple RDDs by key

---

## Roadmap

**Completed:**

* [x] RDD API (transformations & actions)
* [x] Local scheduler implementation
* [x] Shuffle operations
* [x] Cache tracking system
* [x] Dependency management (narrow/shuffle)
* [x] Task execution framework

**In Progress:**

* [ ] Distributed scheduler
* [ ] Network communication layer
* [ ] Result aggregation

**Future:**

* [ ] Checkpointing and recovery
* [ ] Query optimizer (predicate pushdown, pipelining)
* [ ] Python bindings for client API
* [ ] Web dashboard (metrics + job UI)
* [ ] Advanced shuffle optimizations
* Multi-language support (Python/JavaScript via embedded runtimes)

---

## Design Philosophy

> "Atomic brings Spark's proven RDD model to Rust, with a vision for distributed execution that prioritizes efficiency, portability, and safety through compiled binaries and efficient serialization."

---

## 📍 Roadmap

* [x] Flow definition API (DAG)
* [x] Local scheduler prototype
* [ ] Distributed cluster mode
* [ ] Checkpointing and recovery
* [ ] Optimizer (predicate pushdown, pipelining)
* [ ] Python bindings for client API
* [ ] Web dashboard (metrics + job UI)

---

## 📜 License

Licensed under the [Apache 2.0 License](LICENSE).
