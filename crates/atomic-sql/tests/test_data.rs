use std::sync::Arc;

use atomic_sql::AtomicSqlContext;
use datafusion::arrow::array::{Int32Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Build a RecordBatch with columns `(key: i32, value: i32)`.
fn make_kv_batch(keys: &[i32], values: &[i32]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys.to_vec())),
            Arc::new(Int32Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

/// Build a RecordBatch with columns `(id: i32, name: String)`.
fn make_person_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

/// Collect all rows from a `RecordBatch` column as `i64`.
fn col_as_i64(batch: &RecordBatch, col: usize) -> Vec<i64> {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

/// Collect all rows from a `RecordBatch` column as `i32`.
fn col_as_i32(batch: &RecordBatch, col: usize) -> Vec<i32> {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .to_vec()
}

/// Collect all rows from a `RecordBatch` column as `String`.
fn col_as_string(batch: &RecordBatch, col: usize) -> Vec<String> {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.unwrap_or("").to_string())
        .collect()
}

/// Total row count across all batches.
fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ── basic SELECT tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_select_all() {
    let ctx = AtomicSqlContext::new();
    let batch = make_kv_batch(&[1, 2, 3], &[10, 20, 30]);
    ctx.register_batches("t", vec![batch]).unwrap();

    let result = ctx.sql("SELECT * FROM t").await.unwrap().collect().await.unwrap();
    assert_eq!(total_rows(&result), 3);
}

#[tokio::test]
async fn test_select_columns() {
    let ctx = AtomicSqlContext::new();
    let batch = make_kv_batch(&[1, 2], &[10, 20]);
    ctx.register_batches("t", vec![batch]).unwrap();

    let result = ctx.sql("SELECT key FROM t").await.unwrap().collect().await.unwrap();
    assert_eq!(result[0].num_columns(), 1);
    assert_eq!(result[0].schema().field(0).name(), "key");
    assert_eq!(total_rows(&result), 2);
}

// ── WHERE filter tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_where_filter() {
    let ctx = AtomicSqlContext::new();
    let batch = make_kv_batch(&[1, 2, 3, 4, 5], &[10, 20, 30, 40, 50]);
    ctx.register_batches("t", vec![batch]).unwrap();

    let result = ctx
        .sql("SELECT key FROM t WHERE value > 25")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(total_rows(&result), 3); // rows with value 30, 40, 50
}

#[tokio::test]
async fn test_where_filter_no_results() {
    let ctx = AtomicSqlContext::new();
    let batch = make_kv_batch(&[1, 2], &[10, 20]);
    ctx.register_batches("t", vec![batch]).unwrap();

    let result = ctx
        .sql("SELECT * FROM t WHERE value > 100")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(total_rows(&result), 0);
}

// ── aggregation tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_count() {
    let ctx = AtomicSqlContext::new();
    let batch = make_kv_batch(&[1, 2, 3], &[10, 20, 30]);
    ctx.register_batches("t", vec![batch]).unwrap();

    let result = ctx
        .sql("SELECT COUNT(*) AS cnt FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(total_rows(&result), 1);
    assert_eq!(col_as_i64(&result[0], 0), vec![3]);
}

#[tokio::test]
async fn test_sum() {
    let ctx = AtomicSqlContext::new();
    let batch = make_kv_batch(&[1, 2, 3], &[10, 20, 30]);
    ctx.register_batches("t", vec![batch]).unwrap();

    let result = ctx
        .sql("SELECT SUM(value) AS total FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(col_as_i64(&result[0], 0), vec![60]);
}

#[tokio::test]
async fn test_group_by_count() {
    let ctx = AtomicSqlContext::new();
    // key 1 appears twice, key 2 appears once
    let batch = make_kv_batch(&[1, 1, 2], &[10, 20, 30]);
    ctx.register_batches("t", vec![batch]).unwrap();

    let result = ctx
        .sql("SELECT key, COUNT(*) AS cnt FROM t GROUP BY key ORDER BY key")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(total_rows(&result), 2);
    let keys = col_as_i32(&result[0], 0);
    let counts = col_as_i64(&result[0], 1);
    assert_eq!(keys, vec![1, 2]);
    assert_eq!(counts, vec![2, 1]);
}

#[tokio::test]
async fn test_group_by_sum() {
    let ctx = AtomicSqlContext::new();
    let batch = make_kv_batch(&[1, 1, 2, 2], &[10, 20, 5, 15]);
    ctx.register_batches("t", vec![batch]).unwrap();

    let result = ctx
        .sql("SELECT key, SUM(value) AS total FROM t GROUP BY key ORDER BY key")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(total_rows(&result), 2);
    let keys = col_as_i32(&result[0], 0);
    let totals = col_as_i64(&result[0], 1);
    assert_eq!(keys, vec![1, 2]);
    assert_eq!(totals, vec![30, 20]);
}

// ── ORDER BY tests ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_order_by_asc() {
    let ctx = AtomicSqlContext::new();
    let batch = make_kv_batch(&[3, 1, 2], &[30, 10, 20]);
    ctx.register_batches("t", vec![batch]).unwrap();

    let result = ctx
        .sql("SELECT key FROM t ORDER BY key ASC")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let keys = col_as_i32(&result[0], 0);
    assert_eq!(keys, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_order_by_desc() {
    let ctx = AtomicSqlContext::new();
    let batch = make_kv_batch(&[1, 2, 3], &[10, 20, 30]);
    ctx.register_batches("t", vec![batch]).unwrap();

    let result = ctx
        .sql("SELECT key FROM t ORDER BY key DESC")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let keys = col_as_i32(&result[0], 0);
    assert_eq!(keys, vec![3, 2, 1]);
}

// ── LIMIT tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_limit() {
    let ctx = AtomicSqlContext::new();
    let batch = make_kv_batch(&[1, 2, 3, 4, 5], &[10, 20, 30, 40, 50]);
    ctx.register_batches("t", vec![batch]).unwrap();

    let result = ctx
        .sql("SELECT * FROM t LIMIT 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(total_rows(&result), 2);
}

// ── JOIN tests ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_inner_join() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("people", vec![make_person_batch(&[1, 2, 3], &["Alice", "Bob", "Carol"])])
        .unwrap();

    let salary_schema = Arc::new(Schema::new(vec![
        Field::new("person_id", DataType::Int32, false),
        Field::new("amount", DataType::Int32, false),
    ]));
    let salary_batch = RecordBatch::try_new(
        salary_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![5000, 3000])),
        ],
    )
    .unwrap();
    ctx.register_batches("salaries", vec![salary_batch]).unwrap();

    let result = ctx
        .sql("SELECT p.name, s.amount FROM people p JOIN salaries s ON p.id = s.person_id ORDER BY p.id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    // Only Alice (1) and Bob (2) have salaries; Carol (3) is excluded
    assert_eq!(total_rows(&result), 2);
    let names = col_as_string(&result[0], 0);
    assert_eq!(names, vec!["Alice", "Bob"]);
}

// ── multi-partition tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_partitioned_batches() {
    let ctx = AtomicSqlContext::new();
    let p1 = make_kv_batch(&[1, 2], &[10, 20]);
    let p2 = make_kv_batch(&[3, 4], &[30, 40]);
    // Register two separate partitions
    ctx.register_partitioned_batches("t", vec![vec![p1], vec![p2]])
        .unwrap();

    let result = ctx
        .sql("SELECT SUM(value) AS total FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(col_as_i64(&result[0], 0), vec![100]);
}

// ── DISTINCT test ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_distinct() {
    let ctx = AtomicSqlContext::new();
    let batch = make_kv_batch(&[1, 1, 2, 2, 3], &[10, 10, 20, 20, 30]);
    ctx.register_batches("t", vec![batch]).unwrap();

    let result = ctx
        .sql("SELECT DISTINCT key FROM t ORDER BY key")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(total_rows(&result), 3);
    assert_eq!(col_as_i32(&result[0], 0), vec![1, 2, 3]);
}

// ── EXPLAIN test (smoke test — just ensure it doesn't panic) ──────────────────

#[tokio::test]
async fn test_explain() {
    let ctx = AtomicSqlContext::new();
    let batch = make_kv_batch(&[1], &[10]);
    ctx.register_batches("t", vec![batch]).unwrap();

    let result = ctx
        .sql("EXPLAIN SELECT * FROM t WHERE key = 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(!result.is_empty());
}
