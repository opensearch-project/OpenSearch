/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! crc32(input): CRC-32 (IEEE 802.3 polynomial) of the UTF-8 byte stream of `input`.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(Crc32Udf::new()));
}

/// `crc32(varchar)` → `bigint`. Non-negative integer in the range [0, 2^32 - 1].
#[derive(Debug)]
pub struct Crc32Udf {
    signature: Signature,
}

impl Crc32Udf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for Crc32Udf {
    fn default() -> Self {
        Self::new()
    }
}

// `ScalarUDFImpl` requires DynEq + DynHash. All instances are functionally identical
// (no parameterization), so equality is trivial.
impl PartialEq for Crc32Udf {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for Crc32Udf {}
impl Hash for Crc32Udf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "crc32".hash(state);
    }
}

impl ScalarUDFImpl for Crc32Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "crc32"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("crc32 expects exactly 1 argument, got {}", args.args.len());
        }
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(opt))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(opt)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int64(opt.as_ref().map(|s| hash_u32_to_i64(s.as_bytes()))),
            )),
            ColumnarValue::Scalar(other) => exec_err!("crc32: expected Utf8 input, got {other:?}"),
            ColumnarValue::Array(arr) => {
                let out: Int64Array = match arr.data_type() {
                    DataType::Utf8 => arr
                        .as_string::<i32>()
                        .iter()
                        .map(|opt| opt.map(|s| hash_u32_to_i64(s.as_bytes())))
                        .collect(),
                    DataType::LargeUtf8 => arr
                        .as_string::<i64>()
                        .iter()
                        .map(|opt| opt.map(|s| hash_u32_to_i64(s.as_bytes())))
                        .collect(),
                    other => {
                        return exec_err!("crc32: expected Utf8 array, got {other:?}");
                    }
                };
                Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
            }
        }
    }
}

/// IEEE 802.3 CRC-32, zero-extended to i64 so the value stays non-negative.
fn hash_u32_to_i64(value: &[u8]) -> i64 {
    crc32fast::hash(value) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::Field;

    fn udf() -> Crc32Udf {
        Crc32Udf::new()
    }

    fn invoke_scalar(value: ScalarValue) -> Result<ColumnarValue> {
        let u = udf();
        let return_field = Arc::new(Field::new(u.name(), DataType::Int64, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(value)],
            arg_fields: vec![Arc::new(Field::new("v", DataType::Utf8, true))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        u.invoke_with_args(args)
    }

    fn as_i64(v: ColumnarValue) -> Option<i64> {
        match v {
            ColumnarValue::Scalar(ScalarValue::Int64(opt)) => opt,
            other => panic!("expected Int64 scalar, got {other:?}"),
        }
    }

    // Standard reference: crc32("") == 0, crc32("a") == 0xE8B7BE43.
    // See https://rosettacode.org/wiki/CRC-32#Rust for the canonical vector.
    #[test]
    fn empty_input_is_zero() {
        let out = invoke_scalar(ScalarValue::Utf8(Some(String::new()))).unwrap();
        assert_eq!(as_i64(out), Some(0));
    }

    #[test]
    fn single_char_matches_reference_vector() {
        let out = invoke_scalar(ScalarValue::Utf8(Some("a".to_string()))).unwrap();
        assert_eq!(as_i64(out), Some(0xE8B7_BE43));
    }

    #[test]
    fn null_input_yields_null() {
        let out = invoke_scalar(ScalarValue::Utf8(None)).unwrap();
        assert!(as_i64(out).is_none());
    }

    #[test]
    fn result_is_always_non_negative() {
        // 0xFFFFFFFF ≈ 4.29e9; must fit as a positive i64 (zero-extended), never a
        // negative sign-extended i32.
        let out = invoke_scalar(ScalarValue::Utf8(Some("123456789".to_string()))).unwrap();
        let got = as_i64(out).unwrap();
        assert!(got >= 0, "crc32 must be non-negative, got {got}");
        // Reference vector for "123456789" = 0xCBF43926.
        assert_eq!(got, 0xCBF4_3926);
    }

    #[test]
    fn array_input_preserves_null_mask() {
        let u = udf();
        let return_field = Arc::new(Field::new(u.name(), DataType::Int64, true));
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some(""),
        ]));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(values)],
            arg_fields: vec![Arc::new(Field::new("v", DataType::Utf8, true))],
            number_rows: 3,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        match u.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(arr) => {
                let ints = arr.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
                assert_eq!(ints.value(0), 0xE8B7_BE43);
                assert!(ints.is_null(1));
                assert_eq!(ints.value(2), 0);
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }
}
