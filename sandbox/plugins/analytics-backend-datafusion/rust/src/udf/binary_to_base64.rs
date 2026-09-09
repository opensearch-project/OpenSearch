/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! binary_to_base64(bytes) — base64-encode a binary buffer per the OpenSearch
//! `binary` field wire contract.
//!
//! Bound from `IpBinaryCastFunctionAdapter` (Java) which rewrites
//! `CAST(<BinaryType> AS VARCHAR)` to a call into this UDF, so the output
//! matches what `| fields binary_value` already returns instead of getting
//! buffer-reinterpreted as Latin-1 by DataFusion's built-in `cast(binary,
//! utf8)` kernel.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(BinaryToBase64Udf::new()));
}

/// `binary_to_base64(binary)` → `varchar`.
#[derive(Debug)]
pub struct BinaryToBase64Udf {
    signature: Signature,
}

impl BinaryToBase64Udf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Binary])],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for BinaryToBase64Udf {
    fn default() -> Self {
        Self::new()
    }
}

// `ScalarUDFImpl` requires DynEq + DynHash. Stateless — all instances are equivalent.
impl PartialEq for BinaryToBase64Udf {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for BinaryToBase64Udf {}
impl Hash for BinaryToBase64Udf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "binary_to_base64".hash(state);
    }
}

impl ScalarUDFImpl for BinaryToBase64Udf {
    fn name(&self) -> &str {
        "binary_to_base64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!(
                "binary_to_base64 expects exactly 1 argument, got {}",
                args.args.len()
            );
        }
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Binary(opt)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(opt.as_ref().map(|b| STANDARD.encode(b))),
            )),
            ColumnarValue::Scalar(other) => {
                exec_err!("binary_to_base64: expected Binary input, got {other:?}")
            }
            ColumnarValue::Array(arr) => {
                let bin = arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(format!(
                        "binary_to_base64: expected BinaryArray, got {:?}",
                        arr.data_type()
                    ))
                })?;
                let out: StringArray = (0..bin.len())
                    .map(|i| {
                        if bin.is_null(i) {
                            None
                        } else {
                            Some(STANDARD.encode(bin.value(i)))
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, AsArray, BinaryArray};
    use datafusion::arrow::datatypes::Field;

    fn udf() -> BinaryToBase64Udf {
        BinaryToBase64Udf::new()
    }

    fn invoke_scalar(value: ScalarValue) -> Result<ColumnarValue> {
        let u = udf();
        let return_field = Arc::new(Field::new(u.name(), DataType::Utf8, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(value)],
            arg_fields: vec![Arc::new(Field::new("v", DataType::Binary, true))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        u.invoke_with_args(args)
    }

    fn as_utf8(v: ColumnarValue) -> Option<String> {
        match v {
            ColumnarValue::Scalar(ScalarValue::Utf8(opt)) => opt,
            other => panic!("expected Utf8 scalar, got {other:?}"),
        }
    }

    #[test]
    fn round_trips_known_payload() {
        // "Some binary blob" is what the test fixture stores in binary_value;
        // | fields binary_value already returns "U29tZSBiaW5hcnkgYmxvYg==" today.
        let out = invoke_scalar(ScalarValue::Binary(Some(b"Some binary blob".to_vec()))).unwrap();
        assert_eq!(as_utf8(out).unwrap(), "U29tZSBiaW5hcnkgYmxvYg==");
    }

    #[test]
    fn empty_input_yields_empty_string() {
        let out = invoke_scalar(ScalarValue::Binary(Some(Vec::new()))).unwrap();
        assert_eq!(as_utf8(out).unwrap(), "");
    }

    #[test]
    fn null_input_yields_null() {
        let out = invoke_scalar(ScalarValue::Binary(None)).unwrap();
        assert!(as_utf8(out).is_none());
    }

    #[test]
    fn array_input_preserves_null_mask() {
        let u = udf();
        let return_field = Arc::new(Field::new(u.name(), DataType::Utf8, true));
        let values: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![
            Some(b"Some binary blob".as_ref()),
            None,
            Some(b"".as_ref()),
        ]));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(values)],
            arg_fields: vec![Arc::new(Field::new("v", DataType::Binary, true))],
            number_rows: 3,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        match u.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(arr) => {
                let s = arr.as_string::<i32>();
                assert_eq!(s.value(0), "U29tZSBiaW5hcnkgYmxvYg==");
                assert!(s.is_null(1));
                assert_eq!(s.value(2), "");
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }
}
