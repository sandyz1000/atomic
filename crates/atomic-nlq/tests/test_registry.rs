use std::sync::Arc;

use atomic_nlq::registry::ToolRegistry;
use datafusion::arrow::datatypes::DataType;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

// ── Minimal ScalarUDF impl for tests ─────────────────────────────────────────

#[derive(Debug, PartialEq, Eq, Hash)]
struct IsLuxury;

impl ScalarUDFImpl for IsLuxury {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "is_luxury"
    }
    fn signature(&self) -> &Signature {
        use std::sync::OnceLock;
        static SIG: OnceLock<Signature> = OnceLock::new();
        SIG.get_or_init(|| {
            Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8]),
                Volatility::Immutable,
            )
        })
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let _ = args;
        Ok(ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Boolean(Some(false))))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct EstimateLtv;

impl ScalarUDFImpl for EstimateLtv {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        "estimate_ltv"
    }
    fn signature(&self) -> &Signature {
        use std::sync::OnceLock;
        static SIG: OnceLock<Signature> = OnceLock::new();
        SIG.get_or_init(|| {
            Signature::new(
                TypeSignature::Exact(vec![DataType::Int64]),
                Volatility::Volatile,
            )
        })
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let _ = args;
        Ok(ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Float64(Some(0.0))))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

fn make_registry() -> ToolRegistry {
    let ctx = Arc::new(SessionContext::new());
    ToolRegistry::new(ctx)
}

#[test]
fn test_register_scalar_appears_in_descriptions() {
    let reg = make_registry();
    let udf = ScalarUDF::from(IsLuxury);
    reg.register_scalar(udf, "Returns true if the category is a luxury item").unwrap();

    let descs = reg.udf_descriptions();
    assert_eq!(descs.len(), 1);
    assert_eq!(descs[0].name, "is_luxury");
    assert!(
        descs[0].description.contains("luxury"),
        "description should contain the word 'luxury'"
    );
}

#[test]
fn test_register_multiple_udfs() {
    let reg = make_registry();
    reg.register_scalar(
        ScalarUDF::from(IsLuxury),
        "Returns true if the category is a luxury item",
    )
    .unwrap();
    reg.register_scalar(
        ScalarUDF::from(EstimateLtv),
        "Estimates customer lifetime value from customer_id",
    )
    .unwrap();

    let descs = reg.udf_descriptions();
    assert_eq!(descs.len(), 2);
    let names: std::collections::HashSet<_> = descs.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains("is_luxury"));
    assert!(names.contains("estimate_ltv"));
}

#[test]
fn test_udf_description_has_signature() {
    let reg = make_registry();
    reg.register_scalar(ScalarUDF::from(IsLuxury), "Returns true if luxury").unwrap();

    let descs = reg.udf_descriptions();
    assert!(
        !descs[0].signature.is_empty(),
        "signature should not be empty"
    );
}

#[test]
fn test_empty_registry_returns_empty_descriptions() {
    let reg = make_registry();
    assert!(reg.udf_descriptions().is_empty());
}

#[test]
fn test_duplicate_registration_overwrites() {
    let reg = make_registry();
    reg.register_scalar(ScalarUDF::from(IsLuxury), "first description").unwrap();
    reg.register_scalar(ScalarUDF::from(IsLuxury), "second description").unwrap();

    let descs = reg.udf_descriptions();
    // DashMap replace on same key — only one entry
    assert_eq!(descs.len(), 1);
    assert_eq!(descs[0].description, "second description");
}
