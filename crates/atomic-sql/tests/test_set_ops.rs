mod common;
use common::{col_as_i32, make_kv_batch, total_rows};

use atomic_sql::AtomicSqlContext;

#[tokio::test]
async fn test_union_all_includes_duplicates() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("a", vec![make_kv_batch(&[1, 2], &[10, 20])]).unwrap();
    ctx.register_batches("b", vec![make_kv_batch(&[1, 3], &[10, 30])]).unwrap();

    let batches = ctx
        .sql("SELECT key FROM a UNION ALL SELECT key FROM b ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    // a has 2 rows, b has 2 rows → 4 rows (duplicates preserved).
    assert_eq!(total_rows(&batches), 4);
    let keys = col_as_i32(&batches[0], 0);
    assert_eq!(keys, vec![1, 1, 2, 3]);
}

#[tokio::test]
async fn test_union_distinct_deduplicates() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("a", vec![make_kv_batch(&[1, 2], &[10, 20])]).unwrap();
    ctx.register_batches("b", vec![make_kv_batch(&[1, 3], &[10, 30])]).unwrap();

    let batches = ctx
        .sql("SELECT key FROM a UNION SELECT key FROM b ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    // Distinct keys: 1, 2, 3.
    assert_eq!(total_rows(&batches), 3);
    let keys = col_as_i32(&batches[0], 0);
    assert_eq!(keys, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_intersect() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("a", vec![make_kv_batch(&[1, 2, 3], &[0, 0, 0])]).unwrap();
    ctx.register_batches("b", vec![make_kv_batch(&[2, 3, 4], &[0, 0, 0])]).unwrap();

    let batches = ctx
        .sql("SELECT key FROM a INTERSECT SELECT key FROM b ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    // Intersection: {2, 3}.
    assert_eq!(total_rows(&batches), 2);
    let keys = col_as_i32(&batches[0], 0);
    assert_eq!(keys, vec![2, 3]);
}

#[tokio::test]
async fn test_except() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("a", vec![make_kv_batch(&[1, 2, 3], &[0, 0, 0])]).unwrap();
    ctx.register_batches("b", vec![make_kv_batch(&[2, 3, 4], &[0, 0, 0])]).unwrap();

    let batches = ctx
        .sql("SELECT key FROM a EXCEPT SELECT key FROM b ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    // a - b = {1}.
    assert_eq!(total_rows(&batches), 1);
    let keys = col_as_i32(&batches[0], 0);
    assert_eq!(keys, vec![1]);
}

#[tokio::test]
async fn test_union_with_aggregation() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("a", vec![make_kv_batch(&[1, 1, 2], &[10, 10, 20])]).unwrap();
    ctx.register_batches("b", vec![make_kv_batch(&[2, 3, 3], &[20, 30, 30])]).unwrap();

    let batches = ctx
        .sql("SELECT key, COUNT(*) AS cnt FROM (SELECT key FROM a UNION ALL SELECT key FROM b) sub GROUP BY key ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    // key=1: 2 (from a), key=2: 2 (1 from a + 1 from b), key=3: 2 (from b).
    assert_eq!(total_rows(&batches), 3);
}

#[tokio::test]
async fn test_intersect_empty_result() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("a", vec![make_kv_batch(&[1, 2], &[0, 0])]).unwrap();
    ctx.register_batches("b", vec![make_kv_batch(&[3, 4], &[0, 0])]).unwrap();

    let batches = ctx
        .sql("SELECT key FROM a INTERSECT SELECT key FROM b")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 0);
}
