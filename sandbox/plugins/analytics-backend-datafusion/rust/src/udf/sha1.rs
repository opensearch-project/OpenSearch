/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! sha1(input): — 160-bit SHA-1 digest rendered as a lowercase hex string.
//!
//! DataFusion's built-in `digest(..., 'sha1')` is not exposed (its `DigestAlgorithm`
//! enum covers md5/sha224/256/384/512/blake2/blake3 but not SHA-1), so this UDF
//! wraps the `sha1` crate directly. Mirrors DataFusion's `Md5Func` return-type
//! — lowercase hex `Utf8`

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use sha1::{Digest, Sha1};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(Sha1Udf::new()));
}

/// `sha1(varchar)` → `varchar` (40-character lowercase hex string).
#[derive(Debug)]
pub struct Sha1Udf {
    signature: Signature,
}

impl Sha1Udf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for Sha1Udf {
    fn default() -> Self {
        Self::new()
    }
}

// `ScalarUDFImpl` requires DynEq + DynHash. All instances are functionally identical
// (no parameterization), so equality is trivial.
impl PartialEq for Sha1Udf {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for Sha1Udf {}
impl Hash for Sha1Udf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "sha1".hash(state);
    }
}

impl ScalarUDFImpl for Sha1Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sha1"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("sha1 expects exactly 1 argument, got {}", args.args.len());
        }
        match &args.args[0] {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) | ScalarValue::Utf8View(opt),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                opt.as_ref().map(|s| hash_hex(s.as_bytes())),
            ))),
            ColumnarValue::Scalar(other) => exec_err!("sha1: expected Utf8 input, got {other:?}"),
            ColumnarValue::Array(arr) => {
                let view = super::json_common::StringArrayView::from_array(arr)?;
                let out: StringArray = (0..arr.len())
                    .map(|i| view.cell(i).map(|s| hash_hex(s.as_bytes())))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
            }
        }
    }
}

/// Hex encoding lookup table for fast byte-to-hex conversion
const HEX_CHARS_LOWER: &[u8; 16] = b"0123456789abcdef";

fn hash_hex(value: &[u8]) -> String {
    let result = Sha1::digest(value);
    let mut s = String::with_capacity(result.len() * 2);
    for &b in result.as_slice() {
        s.push(HEX_CHARS_LOWER[(b >> 4) as usize] as char);
        s.push(HEX_CHARS_LOWER[(b & 0x0f) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, AsArray, StringArray};
    use datafusion::arrow::datatypes::Field;

    fn udf() -> Sha1Udf {
        Sha1Udf::new()
    }

    fn invoke_scalar(value: ScalarValue) -> Result<ColumnarValue> {
        let u = udf();
        let return_field = Arc::new(Field::new(u.name(), DataType::Utf8, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(value)],
            arg_fields: vec![Arc::new(Field::new("v", DataType::Utf8, true))],
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

    // RFC 3174 appendix A — canonical SHA-1 test vectors (abc, empty string, alphabet).
    #[test]
    fn rfc_abc_vector() {
        let out = invoke_scalar(ScalarValue::Utf8(Some("abc".to_string()))).unwrap();
        assert_eq!(
            as_utf8(out).unwrap(),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
    }

    #[test]
    fn empty_input_hashes_to_canonical_value() {
        let out = invoke_scalar(ScalarValue::Utf8(Some(String::new()))).unwrap();
        assert_eq!(
            as_utf8(out).unwrap(),
            "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        );
    }

    #[test]
    fn null_input_yields_null() {
        let out = invoke_scalar(ScalarValue::Utf8(None)).unwrap();
        assert!(as_utf8(out).is_none());
    }

    #[test]
    fn array_input_preserves_null_mask() {
        let u = udf();
        let return_field = Arc::new(Field::new(u.name(), DataType::Utf8, true));
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("abc"),
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
                let s = arr.as_string::<i32>();
                assert_eq!(s.value(0), "a9993e364706816aba3e25717850c26c9cd0d89d");
                assert!(s.is_null(1));
                assert_eq!(s.value(2), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }
}
