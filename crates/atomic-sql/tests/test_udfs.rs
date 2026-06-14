mod common;
use common::{col_as_i32, col_as_i64, make_kv_batch, total_rows};

use atomic_sql::AtomicSqlContext;
use datafusion::arrow::array::{ArrayRef, Int32Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::sync::Arc;

/// A UDF that multiplies every Int32 value by `factor`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MultiplyUdf {
    signature: Signature,
    factor: i32,
}

impl MultiplyUdf {
    fn new(factor: i32) -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Int32]),
                Volatility::Immutable,
            ),
            factor,
        }
    }
}

impl ScalarUDFImpl for MultiplyUdf {
    fn name(&self) -> &str {
        "multiply"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let col = args.args[0].clone().into_array(args.number_rows)?;
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        let factor = self.factor;
        let result: Int32Array = arr.values().iter().map(|&v| v * factor).collect();
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn register_double_udf(ctx: &AtomicSqlContext) {
    let udf = ScalarUDF::new_from_impl(MultiplyUdf::new(2));
    ctx.inner().register_udf(udf);
}

#[tokio::test]
async fn test_udf_select() {
    let ctx = AtomicSqlContext::new();
    register_double_udf(&ctx);
    ctx.register_batches("t", vec![make_kv_batch(&[1, 2, 3], &[10, 20, 30])])
        .unwrap();

    let batches = ctx
        .sql("SELECT multiply(value) AS doubled FROM t ORDER BY value")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(total_rows(&batches), 3);
    let vals = col_as_i32(&batches[0], 0);
    assert_eq!(vals, vec![20, 40, 60]);
}

#[tokio::test]
async fn test_udf_where() {
    let ctx = AtomicSqlContext::new();
    register_double_udf(&ctx);
    ctx.register_batches("t", vec![make_kv_batch(&[1, 2, 3], &[10, 20, 30])])
        .unwrap();

    let batches = ctx
        .sql("SELECT key FROM t WHERE multiply(value) > 30 ORDER BY key")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // multiply(value) > 30 → value > 15 → rows with value=20 and 30.
    assert_eq!(total_rows(&batches), 2);
    let keys = col_as_i32(&batches[0], 0);
    assert_eq!(keys, vec![2, 3]);
}

#[tokio::test]
async fn test_udf_constant() {
    let ctx = AtomicSqlContext::new();
    register_double_udf(&ctx);
    ctx.register_batches("t", vec![make_kv_batch(&[1], &[5])])
        .unwrap();

    let batches = ctx
        .sql("SELECT multiply(CAST(7 AS INT)) AS result FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let vals = col_as_i32(&batches[0], 0);
    assert_eq!(vals, vec![14]);
}

#[tokio::test]
async fn test_two_udfs_composed() {
    let ctx = AtomicSqlContext::new();
    // Register "double" (×2) and "triple" (×3).
    let double_udf = ScalarUDF::new_from_impl(MultiplyUdf::new(2));
    ctx.inner().register_udf(double_udf);

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TripleUdf(Signature);
    impl ScalarUDFImpl for TripleUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            "triple"
        }
        fn signature(&self) -> &Signature {
            &self.0
        }
        fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int32)
        }
        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let col = args.args[0].clone().into_array(args.number_rows)?;
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            let result: Int32Array = arr.values().iter().map(|&v| v * 3).collect();
            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }
    }
    let triple_udf = ScalarUDF::new_from_impl(TripleUdf(Signature::new(
        TypeSignature::Exact(vec![DataType::Int32]),
        Volatility::Immutable,
    )));
    ctx.inner().register_udf(triple_udf);
    ctx.register_batches("t", vec![make_kv_batch(&[1, 2], &[10, 20])])
        .unwrap();

    let batches = ctx
        .sql("SELECT triple(multiply(value)) AS result FROM t ORDER BY value")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // value=10 → ×2=20 → ×3=60;  value=20 → ×2=40 → ×3=120.
    let vals = col_as_i32(&batches[0], 0);
    assert_eq!(vals, vec![60, 120]);
}

#[tokio::test]
async fn test_udf_group_by() {
    let ctx = AtomicSqlContext::new();
    register_double_udf(&ctx);
    ctx.register_batches("t", vec![make_kv_batch(&[1, 1, 2, 2], &[10, 20, 5, 5])])
        .unwrap();

    let batches = ctx
        .sql("SELECT key, SUM(multiply(value)) AS total FROM t GROUP BY key ORDER BY key")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(total_rows(&batches), 2);
    let totals = col_as_i64(&batches[0], 1);
    // key=1: sum of doubled [10,20] = 20+40 = 60; key=2: sum of doubled [5,5] = 10+10 = 20.
    assert_eq!(totals, vec![60, 20]);
}
