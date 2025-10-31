# 🔥 Ember

**A distributed data processing framework powered by WebAssembly and Rust**

---

## ✨ Overview

**Ember** is a next-generation distributed data processing framework written in **Rust**, inspired by **Apache Spark** — but rebuilt for the modern, secure, and portable compute era.

Instead of running user-defined functions (UDFs) directly on worker machines, **Ember executes tasks inside WASM runtimes**, ensuring:

* Deterministic, sandboxed execution
* Architecture portability
* Safety and isolation for untrusted code

At its core, Ember offers a **DAG-based dataflow engine**, a **distributed scheduler**, and a **WASM-based execution layer** that allows developers to scale compute workloads across clusters or edge nodes.

---

## ⚙️ Core Architecture

```
+--------------------+
|     Ember CLI      |  → Submit jobs, manage clusters, monitor status
+---------+----------+
          |
          v
+--------------------+        +--------------------+
|   Master Node      |  <-->  |   Worker Nodes     |
|  - DAG scheduler   |        |  - WASM executor   |
|  - Job tracker     |        |  - Task sandbox    |
|  - Resource manager|        |  - IO handlers     |
+--------------------+        +--------------------+
```

---

## 🧠 Key Concepts

* **Flow DAGs** — Define jobs as *directed acyclic graphs* of transformations and actions.
* **WASM Executors** — Each task is compiled or serialized into a WASM module executed remotely in a sandbox.
* **Task Isolation** — No shared state or unsafe code between workers.
* **Dynamic Scheduling** — Workload distributed based on node load and data locality.
* **Unified API** — One interface for batch and stream processing.

---

## 🦀 Example: Word Count in Ember

```rust
use ember::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut flow = Flow::new("wordcount");

    let input = flow.read_text("s3://datasets/text/");
    let words = input.flat_map(|line| line.split_whitespace());
    let pairs = words.map(|w| (w.to_string(), 1));
    let counts = pairs.reduce_by_key(|a, b| a + b);
    flow.write(counts, "s3://output/wordcount");

    Ember::run(flow).await
}
```

---

## 🧩 WASM Execution Model

Each transformation (e.g., `map`, `reduce`, `filter`) is serialized into a **WASM module** using the [wasmtime](https://github.com/bytecodealliance/wasmtime) runtime.

```rust
// A simple WASM-executable function
#[wasm_bindgen]
pub fn map_fn(line: &str) -> Vec<String> {
    line.split_whitespace().map(|s| s.to_string()).collect()
}
```

The compiled `.wasm` is shipped to remote workers via the scheduler, executed in isolation, and results are streamed back to the master node.

---

## 🧱 Project Structure

```
ember/
├── ember-core/        # Scheduler, DAG engine, and APIs
├── ember-runtime/     # WASM execution engine (Wasmtime backend)
├── ember-worker/      # Distributed worker nodes
├── ember-cli/         # CLI for job submission & monitoring
├── ember-network/     # RPC, message passing, and data transport
└── examples/          # Sample jobs & flows
```

---

## 🧪 Running Ember

```bash
# Start master
ember-cli master --bind 0.0.0.0:9090

# Start workers
ember-cli worker --connect master:9090 --slots 4

# Submit job
ember-cli submit jobs/wordcount.yaml
```

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

## 💡 Why WASM?

| Feature     | Spark       | Ember            |
| ----------- | ----------- | ---------------- |
| Language    | JVM         | Rust             |
| Task Safety | JVM sandbox | WASM sandbox     |
| Overhead    | High        | Near-native      |
| Portability | JVM-only    | Any architecture |
| Edge Ready  | ❌           | ✅                |

---

## 🧠 Vision

> “Spark redefined distributed compute for the JVM era.
> Ember is here to redefine it for the WebAssembly era.”

---

## 📜 License

Licensed under the [Apache 2.0 License](LICENSE).

