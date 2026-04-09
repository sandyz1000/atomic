# atomic-sql

SQL query layer for the Atomic distributed compute framework.

**DataFusion** handles SQL parsing and query optimization.  
**atomic-compute** handles parallel data materialization across partitions.

This mirrors the Spark SQL → Spark Core relationship: DataFusion's optimizer
produces a physical plan, and atomic-compute's scheduler executes the leaf
scan nodes in parallel (local threads in local mode, remote workers in
distributed mode).

---

## Usage

### Standalone mode (no compute context)

Register pre-loaded Arrow `RecordBatch`es directly and query them:

```rust
use atomic_sql::context::AtomicSqlContext;

#[tokio::main]
async fn main() -> atomic_sql::errors::Result<()> {
    let ctx = AtomicSqlContext::new();

    // Register in-memory data
    ctx.register_batches("orders", batches)?;

    // Run SQL — DataFusion executes entirely in-process
    let df = ctx.sql("SELECT customer_id, SUM(amount) FROM orders GROUP BY 1").await?;
    df.show().await?;
    Ok(())
}
```

You can also register files directly:

```rust
ctx.register_parquet("events", "data/events.parquet", Default::default()).await?;
ctx.register_csv("users", "data/users.csv", Default::default()).await?;
```

### With atomic-compute (parallel RDD-backed execution)

Register a `TypedRdd<RecordBatch>` as a SQL table. Each RDD partition is
materialized in parallel via atomic's scheduler when the query runs.

```rust
use std::sync::Arc;
use atomic_compute::context::Context;
use atomic_compute::env::Config;
use atomic_sql::context::AtomicSqlContext;

#[tokio::main]
async fn main() -> atomic_sql::errors::Result<()> {
    // Build an atomic-compute context (local mode — can also be distributed)
    let config = Config::local();
    let sc = Arc::new(Context::new_with_config(config).unwrap());

    // Parallelize data into an RDD (4 partitions)
    let batches: Vec<RecordBatch> = /* ... */;
    let rdd = sc.parallelize_typed(batches, 4);

    // Create SQL context backed by the compute context
    let ctx = AtomicSqlContext::with_compute(Arc::clone(&sc));

    // Register the RDD as a table — schema is inferred automatically
    ctx.register_rdd("events", rdd)?;

    // Run SQL — atomic-compute materializes partitions in parallel,
    // then DataFusion applies filter/aggregate/join operators
    let df = ctx.sql("SELECT user_id, COUNT(*) FROM events GROUP BY 1").await?;
    let results = df.collect().await?;
    Ok(())
}
```

---

## Data flow

```text
TypedRdd<RecordBatch>  ──register_rdd()──►  RddTableProvider (in catalog)
                                                    │
AtomicSqlContext.sql("SELECT ...")                  │
        │                                           │
        ▼                                           │
DataFusion: SQL → LogicalPlan → Optimizer           │
        │                                           │
        ▼                                           │
DataFusion: PhysicalPlan (leaf = RddScanExec) ◄────┘
        │
        ├─ RddScanExec.execute(0) → atomic-compute scheduler → thread/worker
        ├─ RddScanExec.execute(1) → atomic-compute scheduler → thread/worker
        └─ RddScanExec.execute(N) → atomic-compute scheduler → thread/worker
                │
                ▼
        DataFusion: Filter / Project / Aggregate / Join  (Arrow batches)
                │
                ▼
        DataFrame.collect() → Vec<RecordBatch>
```

---

## Key types

| Type | Description |
| --- | --- |
| `AtomicSqlContext` | Primary entry point. Wraps DataFusion `SessionContext`. |
| `AtomicSqlContext::new()` | Standalone mode — no atomic-compute context. |
| `AtomicSqlContext::with_compute(sc)` | RDD-backed mode — uses atomic-compute for parallel scan. |
| `AtomicSqlContext::register_rdd(name, rdd)` | Register a `TypedRdd<RecordBatch>` as a SQL table. |
| `AtomicSqlContext::register_batches(name, batches)` | Register in-memory batches (standalone). |
| `DataFrame` | Lazy result of a SQL query. Call `.collect()` to execute. |
| `RddTableProvider` | DataFusion `TableProvider` backed by an atomic RDD. |
| `RddScanExec` | DataFusion `ExecutionPlan` leaf that runs one RDD partition via atomic. |
