mod common;
use common::{col_as_i32, col_as_string, make_employees_batch, make_kv_batch, make_salaries_batch, total_rows};

use atomic_sql::AtomicSqlContext;
use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[tokio::test]
async fn test_inner_join() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("emp", vec![make_employees_batch()]).unwrap();
    ctx.register_batches("sal", vec![make_salaries_batch()]).unwrap();

    let batches = ctx
        .sql("SELECT e.name, s.salary FROM emp e JOIN sal s ON e.id = s.emp_id ORDER BY e.id")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 2, "inner join: only Alice and Bob have salaries");
    let names = col_as_string(&batches[0], 0);
    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[tokio::test]
async fn test_left_join_preserve() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("emp", vec![make_employees_batch()]).unwrap();
    ctx.register_batches("sal", vec![make_salaries_batch()]).unwrap();

    let batches = ctx
        .sql("SELECT e.name FROM emp e LEFT JOIN sal s ON e.id = s.emp_id ORDER BY e.id")
        .await.unwrap().collect().await.unwrap();

    // All 4 employees must appear (Carol and Dave get NULL salary).
    assert_eq!(total_rows(&batches), 4, "left join must return all left rows");
}

#[tokio::test]
async fn test_left_join_nulls() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("emp", vec![make_employees_batch()]).unwrap();
    ctx.register_batches("sal", vec![make_salaries_batch()]).unwrap();

    let batches = ctx
        .sql("SELECT e.name, s.salary FROM emp e LEFT JOIN sal s ON e.id = s.emp_id ORDER BY e.id")
        .await.unwrap().collect().await.unwrap();

    // 4 rows; salary column has NULLs for Carol (idx 2) and Dave (idx 3).
    assert_eq!(total_rows(&batches), 4);
    let salary_col = batches[0].column(1);
    // NULLs for rows 2 and 3 (Carol, Dave).
    assert!(salary_col.is_null(2));
    assert!(salary_col.is_null(3));
}

#[tokio::test]
async fn test_right_join_preserve() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("emp", vec![make_employees_batch()]).unwrap();
    ctx.register_batches("sal", vec![make_salaries_batch()]).unwrap();

    let batches = ctx
        .sql("SELECT s.emp_id FROM emp e RIGHT JOIN sal s ON e.id = s.emp_id ORDER BY s.emp_id")
        .await.unwrap().collect().await.unwrap();

    // All salary rows (2) must appear.
    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test]
async fn test_cross_join() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("a", vec![make_kv_batch(&[1, 2], &[10, 20])]).unwrap();
    ctx.register_batches("b", vec![make_kv_batch(&[3, 4], &[30, 40])]).unwrap();

    let batches = ctx
        .sql("SELECT a.key, b.key FROM a CROSS JOIN b ORDER BY a.key, b.key")
        .await.unwrap().collect().await.unwrap();

    // 2 × 2 = 4 rows.
    assert_eq!(total_rows(&batches), 4);
}

#[tokio::test]
async fn test_join_filter() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("emp", vec![make_employees_batch()]).unwrap();
    ctx.register_batches("sal", vec![make_salaries_batch()]).unwrap();

    let batches = ctx
        .sql("SELECT e.name FROM emp e JOIN sal s ON e.id = s.emp_id WHERE s.salary > 85000")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 1);
    assert_eq!(col_as_string(&batches[0], 0), vec!["Alice"]);
}

#[tokio::test]
async fn test_join_with_aggregation() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("emp", vec![make_employees_batch()]).unwrap();
    ctx.register_batches("sal", vec![make_salaries_batch()]).unwrap();

    let batches = ctx
        .sql("SELECT e.dept, SUM(s.salary) AS total FROM emp e JOIN sal s ON e.id = s.emp_id GROUP BY e.dept ORDER BY e.dept")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 1, "only 'eng' dept has salaries");
    let dept = col_as_string(&batches[0], 0);
    assert_eq!(dept, vec!["eng"]);
}

#[tokio::test]
async fn test_self_join() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("t", vec![make_kv_batch(&[1, 2, 3], &[10, 20, 30])]).unwrap();

    // Self-join: find all pairs where t1.value < t2.value.
    let batches = ctx
        .sql("SELECT a.key, b.key FROM t a JOIN t b ON a.value < b.value ORDER BY a.key, b.key")
        .await.unwrap().collect().await.unwrap();

    // Pairs: (1,2),(1,3),(2,3) → 3 rows.
    assert_eq!(total_rows(&batches), 3);
}

#[tokio::test]
async fn test_join_no_matches() {
    let ctx = AtomicSqlContext::new();
    ctx.register_batches("a", vec![make_kv_batch(&[1, 2], &[10, 20])]).unwrap();
    ctx.register_batches("b", vec![make_kv_batch(&[99, 100], &[90, 100])]).unwrap();

    let batches = ctx
        .sql("SELECT a.key FROM a JOIN b ON a.key = b.key")
        .await.unwrap().collect().await.unwrap();

    assert_eq!(total_rows(&batches), 0);
}
