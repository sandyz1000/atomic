//! SQL over RDD-backed tables — `atomic-sql` (DataFusion) example.
//!
//! Mirrors Spark's `SparkSQLExample`: build in-memory tables, register them as
//! SQL relations, and run projection / filter / aggregate / sort / join queries.
//! Each table is a `TypedRdd<RecordBatch>` materialized in parallel by Atomic's
//! scheduler; DataFusion then plans and executes the SQL over the Arrow batches.
//!
//! # Running locally
//!
//! ```bash
//! cargo run -p sql
//! ```
//!
//! There is no distributed mode here — DataFusion executes on the driver after
//! Atomic materializes each RDD partition. To scale the *scan*, split the input
//! into more `RecordBatch`es and raise the partition count passed to
//! `parallelize_typed` (each partition must hold at least one batch).
use std::sync::Arc;

use atomic_compute::context::Context;
use atomic_sql::AtomicSqlContext;
use datafusion::arrow::array::{Float64Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

/// `employees(id, name, dept_id, salary)`.
fn employees_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("dept_id", DataType::Int32, false),
        Field::new("salary", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(StringArray::from(vec![
                "Alice", "Bob", "Carol", "Dave", "Eve", "Frank",
            ])),
            Arc::new(Int32Array::from(vec![10, 10, 20, 20, 20, 30])),
            Arc::new(Float64Array::from(vec![
                120_000.0, 95_000.0, 105_000.0, 87_000.0, 99_000.0, 60_000.0,
            ])),
        ],
    )
    .expect("build employees batch")
}

/// `departments(dept_id, dept_name)`.
fn departments_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("dept_id", DataType::Int32, false),
        Field::new("dept_name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec!["Engineering", "Sales", "Support"])),
        ],
    )
    .expect("build departments batch")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The compute context materializes each registered RDD's partitions.
    let sc = Context::local()?;

    // `with_compute` wires DataFusion to atomic-compute so an RDD can back a table.
    let ctx = AtomicSqlContext::with_compute(Arc::clone(&sc));

    // Register two RDD-backed tables. Schema is inferred from the first batch.
    ctx.register_rdd(
        "employees",
        sc.parallelize_typed(vec![employees_batch()], 1),
    )?;
    ctx.register_rdd(
        "departments",
        sc.parallelize_typed(vec![departments_batch()], 1),
    )?;

    println!("=== 1. Projection + filter (WHERE) ===");
    ctx.sql("SELECT name, salary FROM employees WHERE salary > 100000 ORDER BY salary DESC")
        .await?
        .show()
        .await?;

    println!("\n=== 2. Aggregate (GROUP BY) ===");
    ctx.sql(
        "SELECT dept_id, COUNT(*) AS headcount, ROUND(AVG(salary)) AS avg_salary \
         FROM employees GROUP BY dept_id ORDER BY dept_id",
    )
    .await?
    .show()
    .await?;

    println!("\n=== 3. Join + aggregate across two tables ===");
    ctx.sql(
        "SELECT d.dept_name, COUNT(*) AS headcount, ROUND(SUM(e.salary)) AS payroll \
         FROM employees e JOIN departments d ON e.dept_id = d.dept_id \
         GROUP BY d.dept_name ORDER BY payroll DESC",
    )
    .await?
    .show()
    .await?;

    println!("\n=== 4. Window-free top-N (ORDER BY ... LIMIT) ===");
    ctx.sql("SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3")
        .await?
        .show()
        .await?;

    Ok(())
}
