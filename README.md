# 🔥 Ember

**A distributed data processing framework written in Rust**

---

## Overview

**Ember** is a distributed data processing framework written in **Rust**, inspired by **Apache Spark** — designed for type-safe, efficient, and resilient data processing.

Ember provides a **Spark-like RDD API** with familiar transformations and actions, backed by a **DAG-based execution engine** that optimizes task scheduling and execution across single machines or distributed clusters.

**Current Features:**
* Type-safe RDD API with lazy evaluation
* DAG-based task scheduling
* Local and distributed execution modes
* Shuffle operations for wide transformations
* Task result tracking and caching

**Future Vision:**
Instead of running user-defined functions directly, Ember will execute tasks inside **WASM runtimes** in distributed mode, ensuring:
* Deterministic, sandboxed execution
* Architecture portability
* Safety and isolation for untrusted code

This project is heavily inspired by [vega](https://github.com/rajasekarv/vega/tree/master). The original project is no longer maintained, and its distributed execution required nightly Rust. Ember aims to provide a safer alternative using WASM-compiled UDFs for distributed execution.

---

## ⚙️ Core Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Ember Framework                    │
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

Future: Distributed Mode with WASM Execution
┌─────────────┐        ┌──────────────────┐
│ Master Node │◄──────►│  Worker Nodes    │
│ - Scheduler │        │  - WASM Runtime  │
│ - Tracker   │        │  - Task Sandbox  │
└─────────────┘        └──────────────────┘
```

---

## Key Concepts

* **RDD (Resilient Distributed Datasets)** — Immutable, partitioned collections that support transformations and actions
* **Lazy Evaluation** — Transformations are recorded as a DAG and executed only when an action is called
* **Type-Safe API** — Leverages Rust's type system to catch errors at compile time
* **Dependency Tracking** — Narrow (one-to-one, range) and shuffle (wide) dependencies for efficient execution
* **Task Scheduling** — DAG-based scheduler that generates stages and distributes tasks
* **Future: WASM Isolation** — Tasks will run in sandboxed WASM runtimes in distributed mode

---

## 🦀 Example: Word Count in Ember

```rust
use ember::prelude::*;

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

## 🚀 Future: WASM Execution Model

In distributed mode, transformations will be compiled to **WASM modules** using [wasmtime](https://github.com/bytecodealliance/wasmtime).

```rust
// Future: WASM-executable task
#[wasm_task]
pub fn map_fn(line: &str) -> Vec<String> {
    line.split_whitespace()
        .map(|s| s.to_string())
        .collect()
}
```

The compiled `.wasm` module will be:
1. Serialized and sent to worker nodes
2. Executed in isolated WASM sandbox
3. Results streamed back to scheduler

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
* [ ] WASM sandbox execution for distributed mode
* [ ] Checkpointing and recovery
* [ ] Query optimizer (predicate pushdown, pipelining)
* [ ] Python bindings for client API
* [ ] Web dashboard (metrics + job UI)
* [ ] Advanced shuffle optimizations

---

## 💡 Why WASM for Distributed Mode?

| Feature     | Traditional | Ember (Future)   |
| ----------- | ----------- | ---------------- |
| Language    | JVM/Native  | Rust + WASM      |
| Task Safety | JVM sandbox | WASM sandbox     |
| Overhead    | Medium-High | Near-native      |
| Portability | Limited     | Any architecture |
| Edge Ready  | ❌           | ✅                |
| Memory Safe | Depends     | ✅ Always         |

**Key Advantages:**
* **Security**: No unsafe code execution on workers
* **Isolation**: Each task runs in its own sandbox
* **Efficiency**: Near-native performance with safety guarantees

---

## Design Philosophy

> "Ember brings Spark's proven RDD model to Rust, with a vision for WASM-powered distributed execution that prioritizes safety, portability, and efficiency."

---

## 📍 Roadmap

* [x] Flow definition API (DAG)
* [x] Local scheduler prototype
* [ ] Distributed cluster mode
* [ ] WASM sandbox execution
* [ ] Checkpointing and recovery
* [ ] Optimizer (predicate pushdown, pipelining)
* [ ] Python bindings for client API
* [ ] Web dashboard (metrics + job UI)

---


## 📜 License

Licensed under the [Apache 2.0 License](LICENSE).

