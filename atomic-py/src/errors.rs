//

use std::fmt;

#[derive(Debug)]
struct ApplicationNameNotSetError {
    message: String,
}

#[derive(Debug)]
struct ArgumentRequiredError {
    message: String,
}

#[derive(Debug)]
struct AttributeNotCallableError {
    message: String,
}

#[derive(Debug)]
struct AttributeNotSupportedError {
    message: String,
}

#[derive(Debug)]
struct AxisLengthMismatchError {
    message: String,
}

#[derive(Debug)]
struct BroadcastVariableNotLoadedError {
    message: String,
}

#[derive(Debug)]
struct CallBeforeInitializeError {
    message: String,
}

#[derive(Debug)]
struct CannotAcceptObjectInTypeError {
    message: String,
}

#[derive(Debug)]
struct CannotAccessToDunderError {
    message: String,
}

#[derive(Debug)]
struct CannotApplyInForColumnError {
    message: String,
}


#[derive(Debug)]
struct CannotBeEmptyError {
    message: String,
}

#[derive(Debug)]
struct CannotBeNoneError {
    message: String,
}

#[derive(Debug)]
struct CannotConvertColumnIntoBoolError {
    message: String,
}

#[derive(Debug)]
struct CannotConvertTypeError {
    message: String,
}

#[derive(Debug)]
struct CannotGetBatchIdError {
    message: String,
}

#[derive(Debug)]
struct CannotInferArrayType {
    message: String,
}

#[derive(Debug)]
struct CannotInferEmptySchemaError {
    message: String,
}

#[derive(Debug)]
struct CannotInferSchemaForTypeError {
    message: String,
}

#[derive(Debug)]
struct CannotInferTypeForFieldError {
    message: String,
}

#[derive(Debug)]
struct CannotMergeTypeError {
    message: String,
}

#[derive(Debug)]
struct CannotOpenSocketError {
    message: String,
}

#[derive(Debug)]
struct CannotParseDataTypeError {
    message: String,
}

#[derive(Debug)]
struct CannotProvideMetadataError {
    message: String,
}

#[derive(Debug)]
struct CannotSetTogetherError {
    message: String,
}

#[derive(Debug)]
struct CannotSpecifyReturnTypeForUDFError {
    message: String,
}

#[derive(Debug)]
struct ColumnInListError {
    message: String,
}

#[derive(Debug)]
struct ContextOnlyValidOnDriverError {
    message: String,
}

#[derive(Debug)]
struct ContextUnavailableForRemoteClientError {
    message: String,
}

#[derive(Debug)]
struct DisallowedTypeForContainerError {
    message: String,
}

#[derive(Debug)]
struct DuplicatedFieldNameInArrowStructError {
    message: String,
}

#[derive(Debug)]
struct ExceedRetryError {
    message: String,
}

#[derive(Debug)]
struct HigherOrderFunctionShouldReturnColumnError {
    message: String,
}

#[derive(Debug)]
struct IncorrectConfForProfileError {
    message: String,
}

#[derive(Debug)]
struct InvalidBroadcastOperationError {
    message: String,
}

#[derive(Debug)]
struct InvalidCallOnUnresolvedObjectError {
    message: String,
}

#[derive(Debug)]
struct InvalidConnectURLError {
    message: String,
}

#[derive(Debug)]
struct InvalidItemForContainerError {
    message: String,
}

#[derive(Debug)]
struct InvalidNDArrayDimensionError {
    message: String,
}

#[derive(Debug)]
struct InvalidPandasUDFError {
    message: String,
}

#[derive(Debug)]
struct InvalidPandasUDFTypeError {
    message: String,
}

#[derive(Debug)]
struct InvalidReturnTypeForPandasUDFError {
    message: String,
}

#[derive(Debug)]
struct InvalidTimeoutTimestampError {
    message: String,
}

#[derive(Debug)]
struct InvalidTypeError {
    message: String,
}

#[derive(Debug)]
struct InvalidTypenameCallError {
    message: String,
}

#[derive(Debug)]
struct InvalidUDFEvalTypeError {
    message: String,
}

#[derive(Debug)]
struct InvalidWhenUsageError {
    message: String,
}

trait Error: fmt::Display {
    fn description(&self) -> &str {
        "An error occurred"
    }

    fn cause(&self) -> Option<&dyn Error> {
        None
    }

    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }

    fn full_message(&self) -> String {
        format!("{}: {}", self.description(), self)
    }
}

impl Error for ApplicationNameNotSetError {
    fn description(&self) -> &str {
        "An application name must be set in your configuration."
    }
}

impl Error for ArgumentRequiredError {
    fn description(&self) -> &str {
        "Argument `<arg_name>` is required when <condition>."
    }
}

impl Error for AttributeNotCallableError {
    fn description(&self) -> &str {
        "Attribute `<attr_name>` in provided object `<obj_name>` is not callable."
    }
}

impl Error for AttributeNotSupportedError {
    fn description(&self) -> &str {
        "Attribute `<attr_name>` in provided object `<obj_name>` is not supported."
    }
}

impl Error for AxisLengthMismatchError {
    fn description(&self) -> &str {
        "Length mismatch: Expected axis has <expected_length> element, new values have <actual_length> elements."
    }
}

impl Error for BroadcastVariableNotLoadedError {
    fn description(&self) -> &str {
        "Broadcast variable `<var_name>` is not loaded."
    }
}

impl Error for CallBeforeInitializeError {
    fn description(&self) -> &str {
        "Not supported to call `<func_name>` before initialize <object>."
    }
}

impl Error for CannotAcceptObjectInTypeError {
    fn description(&self) -> &str {
        "`<data_type>` can not accept object `<obj_name>` in type `<obj_type>`."
    }
}

impl Error for CannotAccessToDunderError {
    fn description(&self) -> &str {
        "Dunder(double underscore) attribute is for internal use only."
    }
}

impl Error for CannotApplyInForColumnError {
    fn description(&self) -> &str {
        "Cannot apply 'in' operator against a column: please use 'contains' in a string column or 'array_contains' function for an array column."
    }
}

impl Error for CannotBeEmptyError {
    fn description(&self) -> &str {
        "At least one <item> must be specified."
    }
}

impl Error for CannotBeNoneError {
    fn description(&self) -> &str {
        "Cannot be None."
    }
}

impl Error for CannotConvertColumnIntoBoolError {
    fn description(&self) -> &str {
        "Cannot convert column to bool. please use '&' for 'and', '|' for 'or', '~' for 'not' when building DataFrame boolean expressions."
    }
}

impl Error for CannotConvertTypeError {
    fn description(&self) -> &str {
        "Cannot convert <from_type> into <to_type>."
    }
}

impl Error for CannotGetBatchIdError {
    fn description(&self) -> &str {
        "Cannot get batch id from <obj_name>."
    }
}

impl Error for CannotInferArrayType {
    fn description(&self) -> &str {
        "Can not infer Array Type from an list with None as the first element."
    }
}

impl Error for CannotInferEmptySchemaError {
    fn description(&self) -> &str {
        "Can not infer schema from empty dataset."
    }
}

impl Error for CannotInferSchemaForTypeError {
    fn description(&self) -> &str {
        "Cannot infer schema for type: `<data_type>`."
    }
}

impl Error for CannotInferTypeForFieldError {
    fn description(&self) -> &str {
        "Cannot infer type for field `<field_name>`."
    }
}

impl Error for CannotMergeTypeError {
    fn description(&self) -> &str {
        "Can not merge type `<data_type1>` and `<data_type2>`."
    }
}

impl Error for CannotOpenSocketError {
    fn description(&self) -> &str {
        "Cannot open socket."
    }
}

impl Error for CannotParseDataTypeError {
    fn description(&self) -> &str {
        "Cannot parse data type."
    }
}

impl Error for CannotProvideMetadataError {
    fn description(&self) -> &str {
        "Cannot provide metadata."
    }
}

impl  Error for InvalidWhenUsageError {
    fn description(&self) -> &str {
        "when() can only be applied on a Column previously generated by when() function, and cannot be applied once otherwise() is applied."
    }
}

impl Error for IncorrectConfForProfileError {
    fn description(&self) -> &str {
        "`python.profile.memory` configuration must be set to `true` to enable Python profile."
    }
}

impl Error for InvalidConnectURLError {
    fn description(&self) -> &str {
        "Invalid connect url."
    }
}