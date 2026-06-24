---
title: SQL and DataFrames
description: Run SQL queries over Arrow data and RDD-backed tables with atomic-sql.
---

`atomic-sql` runs SQL and DataFrame queries on [Apache
DataFusion](https://github.com/apache/datafusion). DataFusion provides SQL
parsing, logical planning, an optimizer, and physical execution over Apache
Arrow `RecordBatch` columns. `atomic-sql` adds the context, table providers, and
the bridge to RDD-backed data.

## Standalone queries

Register a file as a table and query it:

```rust
use atomic_sql::AtomicSqlContext;

let ctx = AtomicSqlContext::new();
ctx.register_parquet("orders", "data/orders.parquet", Default::default()).await?;
let df = ctx.sql("SELECT customer_id, SUM(amount) FROM orders GROUP BY 1").await?;
df.show().await?;
```

Parquet, CSV, and JSON readers are built in.

## RDD-backed tables

Register a `TypedRdd<RecordBatch>` as a table with `with_compute`. Each RDD
partition is materialized in parallel by the scheduler; DataFusion then applies
filter, projection, aggregation, and join on the returned batches.

```rust
let sc = Arc::new(Context::new_with_config(Config::local())?);
let rdd = sc.parallelize_typed(batches, num_partitions);

let ctx = AtomicSqlContext::with_compute(Arc::clone(&sc));
ctx.register_rdd("events", rdd)?;          // schema inferred from the first batch
let df = ctx.sql("SELECT user_id, COUNT(*) FROM events GROUP BY 1").await?;
df.show().await?;
```

The data flows from the RDD through a table provider into the DataFusion
physical plan, where each partition runs as a scan leaf on the scheduler:

```text
TypedRdd<RecordBatch>  ──register_rdd()──►  table provider
DataFusion physical plan (leaf = RDD scan)
  ├─ execute(partition = 0) ── scheduler ──► thread / worker
  └─ execute(partition = 1) ── scheduler ──► thread / worker
DataFusion: Filter / Project / Aggregate / Join  →  DataFrame.collect()
```

## Python

```python
import atomic_compute

ctx = atomic_compute.SqlContext()
ctx.register_csv("orders", "data/orders.csv")

df = ctx.sql("SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id")
df.show()

df.write_parquet("/tmp/output/")
table = df.to_arrow()      # PyArrow table
```

## Types

| Type | Role |
|---|---|
| `AtomicSqlContext` | Entry point; wraps a DataFusion `SessionContext` |
| `DataFrame` | Lazy result; wraps a DataFusion `DataFrame` |
| `AtomicTableProvider` | Table backed by pre-loaded `RecordBatch`es |
| `RddTableProvider` | Table backed by a live `TypedRdd<RecordBatch>` |
| `UdfRegistry` | Registers scalar and aggregate user-defined functions |

The row format is Arrow `RecordBatch` throughout. Custom optimizer rules and
physical operators are not added on top of DataFusion; its built-in rules and
operators handle planning and execution.
