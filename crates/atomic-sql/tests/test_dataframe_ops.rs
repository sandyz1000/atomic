mod common;
use common::{col_as_i32, make_kv_batch, total_rows};

use atomic_sql::AtomicSqlContext;

/// keys [1,1,1,2,2,3], values arbitrary — used across the stat/split tests.
fn ctx_with_skewed_keys() -> AtomicSqlContext {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches(
        "t",
        vec![make_kv_batch(&[1, 1, 1, 2, 2, 3], &[0, 0, 0, 0, 0, 0])],
    )
    .unwrap();
    ctx
}

#[tokio::test]
async fn test_cache_same_result() {
    let ctx = ctx_with_skewed_keys();
    let df = ctx.sql("SELECT key FROM t ORDER BY key").await.unwrap();
    let cached = df.cache().await.unwrap();
    let batches = cached.collect().await.unwrap();
    assert_eq!(total_rows(&batches), 6);
    assert_eq!(col_as_i32(&batches[0], 0), vec![1, 1, 1, 2, 2, 3]);
}

#[tokio::test]
async fn test_random_split_partitions() {
    let ctx = ctx_with_skewed_keys();
    let df = ctx.sql("SELECT key FROM t").await.unwrap();
    let parts = df.random_split(&[0.5, 0.5], 42).await.unwrap();
    assert_eq!(parts.len(), 2);
    // Every row lands in exactly one part → counts sum to the total.
    let mut total = 0;
    for p in parts {
        total += total_rows(&p.collect().await.unwrap());
    }
    assert_eq!(total, 6);
}

#[tokio::test]
async fn test_freq_items() {
    let ctx = ctx_with_skewed_keys();
    let df = ctx.sql("SELECT key FROM t").await.unwrap();
    // support 0.4 → threshold ceil(0.4*6)=3; only key 1 (count 3) qualifies.
    let batches = df
        .freq_items("key", 0.4)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 1);
    assert_eq!(col_as_i32(&batches[0], 0), vec![1]);
}

#[tokio::test]
async fn test_sample_by_strata() {
    let ctx = ctx_with_skewed_keys();
    let df = ctx.sql("SELECT key FROM t").await.unwrap();
    // Fraction 1.0 for key 1 keeps all 3; key 3 fraction 0.0 keeps none; key 2 not listed → dropped.
    let batches = df
        .sample_by("key", &[("1", 1.0), ("3", 0.0)])
        .unwrap()
        .collect()
        .await
        .unwrap();
    let keys = col_as_i32(&batches[0], 0);
    assert!(keys.iter().all(|&k| k == 1));
    assert_eq!(keys.len(), 3);
}

#[tokio::test]
async fn test_to_df_rename() {
    let ctx = ctx_with_skewed_keys();
    let df = ctx.sql("SELECT key FROM t LIMIT 1").await.unwrap();
    let renamed = df.to_df(&["k"]).unwrap();
    assert_eq!(renamed.columns(), vec!["k".to_string()]);
}

#[tokio::test]
async fn test_to_df_arity() {
    let ctx = ctx_with_skewed_keys();
    let df = ctx.sql("SELECT key, value FROM t").await.unwrap();
    assert!(df.to_df(&["only_one"]).is_err());
}

#[tokio::test]
async fn test_tail() {
    let ctx = ctx_with_skewed_keys();
    let df = ctx.sql("SELECT key FROM t ORDER BY key").await.unwrap();
    // tail returns the collected last-n rows directly.
    let batches = df.tail(2).await.unwrap();
    assert_eq!(col_as_i32(&batches[0], 0), vec![2, 3]);
}

#[tokio::test]
async fn test_repartition_preserves_rows() {
    let ctx = ctx_with_skewed_keys();
    let df = ctx.sql("SELECT key FROM t").await.unwrap();
    let batches = df.repartition(4, &[]).unwrap().collect().await.unwrap();
    assert_eq!(total_rows(&batches), 6);
}

#[tokio::test]
async fn test_global_temp_view() {
    let ctx = ctx_with_skewed_keys();
    let df = ctx.sql("SELECT key FROM t WHERE key = 2").await.unwrap();
    ctx.create_or_replace_global_temp_view("v", df).unwrap();
    let batches = ctx
        .sql("SELECT COUNT(*) AS c FROM global_temp.v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    // key = 2 appears twice.
    assert_eq!(total_rows(&batches), 1);
}
