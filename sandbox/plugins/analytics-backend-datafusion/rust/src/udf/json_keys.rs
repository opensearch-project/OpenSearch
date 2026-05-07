/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `json_keys(value)` — top-level keys of a JSON object, encoded as a JSON-array
//! string. Non-object / malformed / NULL input → NULL. Mirrors the legacy SQL
//! plugin's `JsonKeysFunctionImpl`, which delegates to Calcite
//! `JsonFunctions.jsonKeys` (returns `"null"` — converted to SQL NULL — for
//! array, scalar, or missing input) and our "malformed → NULL" decision
//! documented in `json_udf_legacy_semantics.md`.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value;

use super::json_common::{as_utf8_array, check_arity, parse};
use super::{coerce_args, CoerceMode};

const NAME: &str = "json_keys";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(JsonKeysUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonKeysUdf {
    signature: Signature,
}

impl JsonKeysUdf {
    pub fn new() -> Self {
        Self { signature: Signature::user_defined(Volatility::Immutable) }
    }
}

impl Default for JsonKeysUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for JsonKeysUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        NAME
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(NAME, args, &[CoerceMode::Utf8])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        check_arity(NAME, args.args.len(), 1)?;
        let n = args.number_rows;

        if let ColumnarValue::Scalar(sv) = &args.args[0] {
            let keys = match sv {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => {
                    json_keys(s)
                }
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(keys)));
        }

        let arr = args.args[0].clone().into_array(n)?;
        let strings = as_utf8_array(&arr)?;
        let mut b = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            if strings.is_null(i) {
                b.append_null();
                continue;
            }
            match json_keys(strings.value(i)) {
                Some(s) => b.append_value(&s),
                None => b.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish()) as ArrayRef))
    }
}

/// Returns the JSON-array-encoded list of top-level keys for an object input,
/// or `None` for malformed / non-object / scalar / array inputs. Matches the
/// legacy contract; `serde_json::Map` is order-preserving by default (via the
/// `preserve_order` feature disabled — insertion order on BTreeMap is
/// alphabetical. Tests assert the observed ordering rather than insertion
/// order to avoid coupling to a crate feature flag.
fn json_keys(s: &str) -> Option<String> {
    match parse(s)? {
        Value::Object(map) => {
            let keys: Vec<&String> = map.keys().collect();
            serde_json::to_string(&keys).ok()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_input_returns_jsonized_keys() {
        assert_eq!(
            json_keys(r#"{"f1":"abc","f2":{"f3":"a"}}"#).as_deref(),
            Some(r#"["f1","f2"]"#)
        );
        assert_eq!(json_keys(r#"{}"#).as_deref(), Some(r#"[]"#));
    }

    #[test]
    fn non_object_returns_none() {
        assert_eq!(json_keys(r#"[1,2,3]"#), None);
        assert_eq!(json_keys(r#"42"#), None);
        assert_eq!(json_keys(r#""scalar""#), None);
        assert_eq!(json_keys(r#"null"#), None);
    }

    #[test]
    fn malformed_returns_none() {
        assert_eq!(json_keys(""), None);
        assert_eq!(json_keys("{not-json"), None);
    }

    #[test]
    fn return_type_is_utf8() {
        assert_eq!(JsonKeysUdf::new().return_type(&[DataType::Utf8]).unwrap(), DataType::Utf8);
    }

    #[test]
    fn coerce_types_enforces_string_arity() {
        let udf = JsonKeysUdf::new();
        assert_eq!(udf.coerce_types(&[DataType::LargeUtf8]).unwrap(), vec![DataType::Utf8]);
        assert!(udf.coerce_types(&[DataType::Int64]).unwrap_err().to_string().contains("expected string"));
        assert!(udf.coerce_types(&[]).is_err());
    }
}
