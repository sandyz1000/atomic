use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum JsUdfStageError {
    #[error(
        "cannot ship a native/bound function to workers; \
         wrap it in an arrow function, e.g. map(x => Math.sqrt(x))"
    )]
    NativeFunction,
    #[error("failed to encode context as JSON: {0}")]
    ContextEncode(#[from] serde_json::Error),
}

impl From<JsUdfStageError> for napi::Error {
    fn from(e: JsUdfStageError) -> Self {
        napi::Error::from_reason(e.to_string())
    }
}

/// Reject a function's source text if it is native/bound code (e.g. `Math.sqrt`),
/// which loses its implementation entirely when shipped to a worker as a string.
pub(crate) fn reject_native_source(src: &str) -> Result<(), JsUdfStageError> {
    if src.contains("[native code]") {
        return Err(JsUdfStageError::NativeFunction);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_native_fn_rejected() {
        let src = "function sqrt() { [native code] }";
        assert!(matches!(
            reject_native_source(src),
            Err(JsUdfStageError::NativeFunction)
        ));
    }

    #[test]
    fn test_arrow_fn_accepted() {
        assert!(reject_native_source("(x) => x * 2").is_ok());
    }
}
