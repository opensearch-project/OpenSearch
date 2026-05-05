/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `json_array_length(value)` — length of a JSON array.
//!
//! # Observable semantics (matches legacy SQL plugin's
//! `JsonArrayLengthFunctionImpl`; verified by the SQL plugin IT
//! `CalcitePPLJsonBuiltinFunctionIT.testJsonArrayLength`):
//!
//! | input                       | output    |
//! |-----------------------------|-----------|
//! | JSON array `[e0, ..., eN]`  | `N+1`     |
//! | NULL                        | NULL      |
//! | non-array JSON (object, scalar) | NULL  |
//! | malformed / unparseable JSON    | NULL  |
//!
//! Only plan-time arity / type failures (e.g. non-string argument) surface as
//! `plan_err!`. Runtime input — of any content — never errors.
//!
//! # Division of labor with the Java adapter
//!
//! `JsonFunctionAdapters.JsonArrayLengthAdapter` is a plain
//! `AbstractNameMappingAdapter` rename — it doesn't touch operands. All
//! validation is in this UDF: `coerce_types` enforces Utf8 via
//! `CoerceMode::Utf8`, `invoke_with_args` handles malformed / non-array → NULL.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Builder, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value;

use super::{coerce_args, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(JsonArrayLengthUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonArrayLengthUdf {
    signature: Signature,
}

impl JsonArrayLengthUdf {
    pub fn new() -> Self {
        // user_defined + coerce_types lets DF cast LargeUtf8 / Utf8View → Utf8.
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for JsonArrayLengthUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for JsonArrayLengthUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_array_length"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return plan_err!(
                "json_array_length expects 1 argument, got {}",
                arg_types.len()
            );
        }
        // Int32 to match PPL's INTEGER_FORCE_NULLABLE declaration. Returning
        // Int64 here works for literal args (Calcite const-folds and inserts a
        // narrowing CAST on the project) but leaks Int64 through the column
        // path — caller sees Integer for literals, Long for column refs.
        Ok(DataType::Int32)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args("json_array_length", arg_types, &[CoerceMode::Utf8])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!(
                "json_array_length expects 1 argument, got {}",
                args.args.len()
            );
        }
        let n = args.number_rows;

        // Scalar fast-path: parse once, broadcast as scalar output.
        if let ColumnarValue::Scalar(sv) = &args.args[0] {
            let len = match sv {
                ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) | ScalarValue::Utf8View(opt) => {
                    opt.as_deref().and_then(json_array_len)
                }
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Int32(len)));
        }

        let arr = args.args[0].clone().into_array(n)?;
        let strings = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "json_array_length: expected Utf8, got {:?}",
                arr.data_type()
            ))
        })?;

        let mut builder = Int32Builder::with_capacity(n);
        for i in 0..n {
            if strings.is_null(i) {
                builder.append_null();
                continue;
            }
            match json_array_len(strings.value(i)) {
                Some(len) => builder.append_value(len),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Returns the array length as i32, or None for malformed / non-array input.
/// i32 matches PPL's declared INTEGER return type; arrays exceeding i32::MAX
/// elements (>2B) saturate to NULL rather than silently truncating.
fn json_array_len(s: &str) -> Option<i32> {
    serde_json::from_str::<Value>(s)
        .ok()
        .and_then(|v| v.as_array().map(|a| a.len()))
        .and_then(|len| i32::try_from(len).ok())
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::Field;

    #[test]
    fn parses_array_returns_length() {
        assert_eq!(json_array_len("[1,2,3]"), Some(3));
        assert_eq!(json_array_len("[]"), Some(0));
        assert_eq!(json_array_len("[\"a\",\"b\"]"), Some(2));
        // Heterogeneous array — parity with legacy Gson List parse.
        assert_eq!(json_array_len("[1,\"x\",{\"k\":1}]"), Some(3));
    }

    #[test]
    fn non_array_json_returns_none() {
        assert_eq!(json_array_len("{\"k\":1}"), None);
        assert_eq!(json_array_len("\"scalar\""), None);
        assert_eq!(json_array_len("42"), None);
        assert_eq!(json_array_len("null"), None);
    }

    #[test]
    fn malformed_json_returns_none() {
        assert_eq!(json_array_len("not-json"), None);
        assert_eq!(json_array_len("[1,2"), None);
        assert_eq!(json_array_len(""), None);
    }

    #[test]
    fn coerce_types_accepts_string_variants() {
        let udf = JsonArrayLengthUdf::new();
        for t in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let out = udf.coerce_types(std::slice::from_ref(&t)).unwrap();
            assert_eq!(out, vec![DataType::Utf8], "input {t:?} should coerce to Utf8");
        }
    }

    #[test]
    fn coerce_types_rejects_non_string() {
        let udf = JsonArrayLengthUdf::new();
        let err = udf.coerce_types(&[DataType::Int64]).unwrap_err();
        assert!(err.to_string().contains("expected string"));
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = JsonArrayLengthUdf::new();
        assert!(udf.coerce_types(&[]).is_err());
        assert!(udf.coerce_types(&[DataType::Utf8, DataType::Utf8]).is_err());
    }

    #[test]
    fn return_type_is_int32() {
        let udf = JsonArrayLengthUdf::new();
        let out = udf.return_type(&[DataType::Utf8]).unwrap();
        assert_eq!(out, DataType::Int32);
    }

    #[test]
    fn invoke_handles_nulls_malformed_and_non_array() {
        let udf = JsonArrayLengthUdf::new();
        let input = StringArray::from(vec![
            Some("[1,2,3]"),
            None,
            Some("{\"k\":1}"),
            Some("not-json"),
            Some("[]"),
        ]);
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            number_rows: 5,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("out", DataType::Int32, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.value(0), 3);
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
        assert!(arr.is_null(3));
        assert_eq!(arr.value(4), 0);
    }

    #[test]
    fn invoke_scalar_input_produces_scalar_output() {
        let udf = JsonArrayLengthUdf::new();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some("[1,2,3,4]".into())))],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("out", DataType::Int32, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        match out {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(4))) => {}
            other => panic!("expected Int32(Some(4)), got {other:?}"),
        }
    }

    #[test]
    fn invoke_scalar_null_input_yields_scalar_null() {
        let udf = JsonArrayLengthUdf::new();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("out", DataType::Int32, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        match out {
            ColumnarValue::Scalar(ScalarValue::Int32(None)) => {}
            other => panic!("expected Int32(None), got {other:?}"),
        }
    }
}
