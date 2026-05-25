use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array, StringArray, StringViewArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

// ── Helpers ───────────────────────────────────────────────────────────────────

pub fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

pub fn col_as_i32(batch: &RecordBatch, col: usize) -> Vec<i32> {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .to_vec()
}

pub fn col_as_i64(batch: &RecordBatch, col: usize) -> Vec<i64> {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

pub fn col_as_u64(batch: &RecordBatch, col: usize) -> Vec<u64> {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .values()
        .to_vec()
}

/// Returns string values from a column that may be `StringArray` or `StringViewArray`.
pub fn col_as_string(batch: &RecordBatch, col: usize) -> Vec<String> {
    let arr = batch.column(col);
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return a.iter().map(|v| v.unwrap_or("").to_string()).collect();
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringViewArray>() {
        return a.iter().map(|v| v.unwrap_or("").to_string()).collect();
    }
    panic!(
        "column {} is neither StringArray nor StringViewArray, got {:?}",
        col,
        batch.schema().field(col).data_type()
    );
}

// ── Fixtures ──────────────────────────────────────────────────────────────────

/// `(id: i32, name: Utf8, dept: Utf8)` — 4 rows across 3 departments.
pub fn make_employees_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("dept", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol", "Dave"])),
            Arc::new(StringArray::from(vec!["eng", "eng", "hr", "hr"])),
        ],
    )
    .unwrap()
}

/// `(emp_id: i32, salary: i32)` — Alice and Bob have salaries; Carol/Dave do not.
pub fn make_salaries_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("emp_id", DataType::Int32, false),
        Field::new("salary", DataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![90_000, 80_000])),
        ],
    )
    .unwrap()
}

/// `(key: i32, value: i32)`
pub fn make_kv_batch(keys: &[i32], values: &[i32]) -> RecordBatch {
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
