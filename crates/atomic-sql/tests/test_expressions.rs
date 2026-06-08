mod common;
use common::{col_as_i32, col_as_i64, col_as_string, col_as_u64, make_kv_batch, total_rows};

use atomic_sql::AtomicSqlContext;
use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn make_nullable_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Int32, true),
    ]));
    let val_array = {
        let mut builder = datafusion::arrow::array::Int32Builder::new();
        builder.append_value(10);
        builder.append_null();
        builder.append_value(30);
        builder.finish()
    };
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(val_array),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_computed_column_addition() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_kv_batch(&[1, 2, 3], &[10, 20, 30])]).unwrap();

    let batches = ctx
        .sql("SELECT key + value AS total FROM t ORDER BY total")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 3);
    let totals = col_as_i32(&batches[0], 0);
    assert_eq!(totals, vec![11, 22, 33]);
}

#[tokio::test]
async fn test_column_alias() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_kv_batch(&[1], &[10])]).unwrap();

    let batches = ctx
        .sql("SELECT key AS k, value AS v FROM t")
        .await.unwrap().collect().await.unwrap();

    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "k");
    assert_eq!(schema.field(1).name(), "v");
}

#[tokio::test]
async fn test_case_when() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_kv_batch(&[1, 2, 3], &[10, 20, 30])]).unwrap();

    let batches = ctx
        .sql("SELECT CASE WHEN value > 15 THEN 'big' ELSE 'small' END AS size FROM t ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    let sizes = col_as_string(&batches[0], 0);
    assert_eq!(sizes, vec!["small", "big", "big"]);
}

#[tokio::test]
async fn test_coalesce_replaces_null() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_nullable_batch()]).unwrap();

    let batches = ctx
        .sql("SELECT COALESCE(val, CAST(0 AS INT)) AS val_or_zero FROM t ORDER BY id")
        .await.unwrap().collect().await.unwrap();

    let vals = col_as_i32(&batches[0], 0);
    assert_eq!(vals, vec![10, 0, 30]);
}

#[tokio::test]
async fn test_is_null_filter() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_nullable_batch()]).unwrap();

    let batches = ctx
        .sql("SELECT id FROM t WHERE val IS NULL")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 1);
    let ids = col_as_i32(&batches[0], 0);
    assert_eq!(ids, vec![2]);
}

#[tokio::test]
async fn test_not_null_filter() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_nullable_batch()]).unwrap();

    let batches = ctx
        .sql("SELECT id FROM t WHERE val IS NOT NULL ORDER BY id")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test]
async fn test_string_upper_lower() {
    let ctx = AtomicSqlContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["Alice", "Bob"]))],
    )
    .unwrap();
    ctx.register_batches("t", vec![batch]).unwrap();

    let batches = ctx
        .sql("SELECT UPPER(name) AS u, LOWER(name) AS l FROM t ORDER BY name")
        .await.unwrap().collect().await.unwrap();

    let uppers = col_as_string(&batches[0], 0);
    assert_eq!(uppers, vec!["ALICE", "BOB"]);
    let lowers = col_as_string(&batches[0], 1);
    assert_eq!(lowers, vec!["alice", "bob"]);
}

#[tokio::test]
async fn test_string_length() {
    let ctx = AtomicSqlContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec!["Hi", "Hello"]))],
    )
    .unwrap();
    ctx.register_batches("t", vec![batch]).unwrap();

    let batches = ctx
        .sql("SELECT LENGTH(name) AS len FROM t ORDER BY name")
        .await.unwrap().collect().await.unwrap();

    let lens = col_as_i32(&batches[0], 0);
    assert_eq!(lens, vec![5, 2]); // "Hello"=5, "Hi"=2 (sorted alphabetically)
}

#[tokio::test]
async fn test_having_clause() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_kv_batch(&[1, 1, 2, 2, 3], &[10, 20, 30, 40, 50])]).unwrap();

    let batches = ctx
        .sql("SELECT key, SUM(value) AS total FROM t GROUP BY key HAVING SUM(value) > 50 ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    // key=1: sum=30 (fails), key=2: sum=70 (passes), key=3: sum=50 (fails ≤ 50).
    assert_eq!(total_rows(&batches), 1);
    let keys = col_as_i32(&batches[0], 0);
    assert_eq!(keys, vec![2]);
}

#[tokio::test]
async fn test_cte() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_kv_batch(&[1, 2, 3], &[10, 20, 30])]).unwrap();

    let batches = ctx
        .sql(
            "WITH evens AS (SELECT key, value FROM t WHERE key % 2 = 0)
             SELECT key FROM evens",
        )
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 1);
    let keys = col_as_i32(&batches[0], 0);
    assert_eq!(keys, vec![2]);
}

#[tokio::test]
async fn test_window_row_number() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_kv_batch(&[1, 2, 3], &[10, 20, 30])]).unwrap();

    let batches = ctx
        .sql("SELECT key, ROW_NUMBER() OVER (ORDER BY key) AS rn FROM t ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 3);
    let rns = col_as_u64(&batches[0], 1);
    assert_eq!(rns, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_window_running_sum() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_kv_batch(&[1, 2, 3], &[10, 20, 30])]).unwrap();

    let batches = ctx
        .sql("SELECT key, SUM(value) OVER (ORDER BY key ROWS UNBOUNDED PRECEDING) AS running FROM t ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 3);
    let running = col_as_i64(&batches[0], 1);
    assert_eq!(running, vec![10, 30, 60]);
}

#[tokio::test]
async fn test_window_function_rank() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_kv_batch(&[1, 2, 2, 3], &[10, 20, 20, 30])]).unwrap();

    let batches = ctx
        .sql("SELECT key, RANK() OVER (ORDER BY key) AS rnk FROM t ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 4);
    let ranks = col_as_u64(&batches[0], 1);
    // key=1→rank 1, key=2→rank 2 (tie), key=2→rank 2, key=3→rank 4.
    assert_eq!(ranks, vec![1, 2, 2, 4]);
}

#[tokio::test]
async fn test_cast_int_str() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_kv_batch(&[42], &[0])]).unwrap();

    let batches = ctx
        .sql("SELECT CAST(key AS VARCHAR) AS s FROM t")
        .await.unwrap().collect().await.unwrap();

    let strs = col_as_string(&batches[0], 0);
    assert_eq!(strs, vec!["42"]);
}
