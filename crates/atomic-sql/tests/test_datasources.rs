mod common;
use common::{col_as_i32, col_as_string, make_kv_batch, total_rows};

use atomic_sql::AtomicSqlContext;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::Arc;


fn write_parquet(path: &std::path::Path, batch: RecordBatch) {
    let file = File::create(path).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}


#[tokio::test]
async fn test_csv_query() {
    let td = tempfile::tempdir().unwrap();
    let csv_path = td.path().join("data.csv");
    std::fs::write(&csv_path, "key,value\n1,10\n2,20\n3,30\n").unwrap();

    let ctx = AtomicSqlContext::new();
    ctx.register_csv("t", csv_path.to_str().unwrap(), Default::default())
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT key FROM t ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 3);
}

#[tokio::test]
async fn test_csv_filter() {
    let td = tempfile::tempdir().unwrap();
    let csv_path = td.path().join("data.csv");
    std::fs::write(&csv_path, "key,value\n1,10\n2,20\n3,30\n4,40\n5,50\n").unwrap();

    let ctx = AtomicSqlContext::new();
    ctx.register_csv("t", csv_path.to_str().unwrap(), Default::default())
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT key FROM t WHERE value > 25 ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 3); // rows 3,4,5
}

#[tokio::test]
async fn test_parquet_query() {
    let td = tempfile::tempdir().unwrap();
    let pq_path = td.path().join("data.parquet");
    write_parquet(&pq_path, make_kv_batch(&[1, 2, 3], &[10, 20, 30]));

    let ctx = AtomicSqlContext::new();
    ctx.register_parquet("t", pq_path.to_str().unwrap(), Default::default())
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT SUM(value) AS total FROM t")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 1);
    use datafusion::arrow::array::Int64Array;
    let total = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(total, 60);
}

#[tokio::test]
async fn test_parquet_projection() {
    let td = tempfile::tempdir().unwrap();
    let pq_path = td.path().join("data.parquet");
    write_parquet(&pq_path, make_kv_batch(&[1, 2], &[10, 20]));

    let ctx = AtomicSqlContext::new();
    ctx.register_parquet("t", pq_path.to_str().unwrap(), Default::default())
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT key FROM t ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(batches[0].num_columns(), 1);
    let keys = col_as_i32(&batches[0], 0);
    assert_eq!(keys, vec![1, 2]);
}

#[tokio::test]
async fn test_deregister_query_fails() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_kv_batch(&[1], &[10])]).unwrap();
    ctx.deregister_table("t").unwrap();

    let result = ctx.sql("SELECT * FROM t").await;
    assert!(result.is_err(), "query on deregistered table should fail");
}

#[tokio::test]
async fn test_partitioned_aggregate() {
    let ctx = AtomicSqlContext::new();
    let p1 = make_kv_batch(&[1, 2], &[10, 20]);
    let p2 = make_kv_batch(&[3, 4], &[30, 40]);
    ctx.register_partitioned_batches("t", vec![vec![p1], vec![p2]])
        .unwrap();

    let batches = ctx
        .sql("SELECT SUM(value) AS total FROM t")
        .await.unwrap().collect().await.unwrap();

    use datafusion::arrow::array::Int64Array;
    let total = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(total, 100);
}

#[tokio::test]
async fn test_parquet_roundtrip() {
    let td = tempfile::tempdir().unwrap();
    let out_dir = td.path().join("output");

    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_kv_batch(&[10, 20], &[100, 200])]).unwrap();

    let df = ctx.sql("SELECT key, value FROM t ORDER BY key").await.unwrap();
    let write_opts = DataFrameWriteOptions::new().with_single_file_output(false);
    df.write_parquet(out_dir.to_str().unwrap(), write_opts, None)
        .await
        .unwrap();

    // Read the written files back.
    let ctx2 = AtomicSqlContext::new();
    ctx2.register_parquet("t2", out_dir.to_str().unwrap(), Default::default())
        .await
        .unwrap();
    let batches = ctx2
        .sql("SELECT key FROM t2 ORDER BY key")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test]
async fn test_json_query() {
    let td = tempfile::tempdir().unwrap();
    let json_path = td.path().join("data.json");
    std::fs::write(
        &json_path,
        "{\"key\": 1, \"value\": 10}\n{\"key\": 2, \"value\": 20}\n",
    )
    .unwrap();

    let ctx = AtomicSqlContext::new();
    ctx.register_json("t", json_path.to_str().unwrap(), Default::default())
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT COUNT(*) AS cnt FROM t")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 1);
    use datafusion::arrow::array::Int64Array;
    let cnt = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(cnt, 2);
}
