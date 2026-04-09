/// Integration tests: atomic-compute RDD → atomic-sql SQL queries.
///
/// These tests verify that a `TypedRdd<RecordBatch>` registered via
/// `AtomicSqlContext::with_compute()` / `register_rdd()` is correctly
/// materialized and queryable by DataFusion's SQL engine.
use std::sync::Arc;

use atomic_compute::context::Context;
use atomic_sql::AtomicSqlContext;
use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

// ── helpers ───────────────────────────────────────────────────────────────────

fn make_people_batch(ids: &[i32], names: &[&str], ages: &[i32]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Int32Array::from(ages.to_vec())),
        ],
    )
    .unwrap()
}

fn local_context() -> Arc<Context> {
    Context::local().expect("failed to create local atomic context")
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_register_rdd_and_select_all() {
    let sc = local_context();
    let batch = make_people_batch(&[1, 2, 3], &["Alice", "Bob", "Carol"], &[30, 25, 35]);

    let rdd = sc.parallelize_typed(vec![batch], 1);
    let ctx = AtomicSqlContext::with_compute(Arc::clone(&sc));
    ctx.register_rdd("people", rdd).expect("register_rdd failed");

    let df = ctx.sql("SELECT id, name FROM people ORDER BY id").await.unwrap();
    let batches = df.collect().await.unwrap();

    let ids: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();

    assert_eq!(ids, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_register_rdd_with_filter() {
    let sc = local_context();
    let batch = make_people_batch(&[1, 2, 3, 4], &["Alice", "Bob", "Carol", "Dave"], &[30, 25, 35, 20]);

    let rdd = sc.parallelize_typed(vec![batch], 1);
    let ctx = AtomicSqlContext::with_compute(Arc::clone(&sc));
    ctx.register_rdd("people", rdd).expect("register_rdd failed");

    let df = ctx.sql("SELECT name FROM people WHERE age > 28 ORDER BY name").await.unwrap();
    let batches = df.collect().await.unwrap();

    let names: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|s| s.unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .collect();

    assert_eq!(names, vec!["Alice", "Carol"]);
}

#[tokio::test]
async fn test_register_rdd_multipartition() {
    let sc = local_context();
    let batch1 = make_people_batch(&[1, 2], &["Alice", "Bob"], &[30, 25]);
    let batch2 = make_people_batch(&[3, 4], &["Carol", "Dave"], &[35, 20]);

    // Two separate batches → two partitions
    let rdd = sc.parallelize_typed(vec![batch1, batch2], 2);
    let ctx = AtomicSqlContext::with_compute(Arc::clone(&sc));
    ctx.register_rdd("people", rdd).expect("register_rdd failed");

    let df = ctx.sql("SELECT COUNT(*) AS cnt FROM people").await.unwrap();
    let batches = df.collect().await.unwrap();

    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);

    assert_eq!(count, 4);
}

#[tokio::test]
async fn test_register_rdd_requires_compute_context() {
    // AtomicSqlContext::new() has no compute context — register_rdd must fail gracefully.
    let sc = local_context();
    let batch = make_people_batch(&[1], &["Alice"], &[30]);
    let rdd = sc.parallelize_typed(vec![batch], 1);

    let ctx = AtomicSqlContext::new(); // no compute context
    let result = ctx.register_rdd("people", rdd);
    assert!(result.is_err(), "expected error when no compute context is set");
}
