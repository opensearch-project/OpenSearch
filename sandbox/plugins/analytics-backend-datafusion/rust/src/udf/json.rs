/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `json(value)` — validate a string as JSON. Parses the input; on success the
//! parsed value is re-serialised (round-trip) and returned, on failure or
//! NULL input the result is NULL. Mirrors PPL's legacy `JsonFunctionImpl.eval`.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::json_common::{check_arity, parse, StringArrayView};
use super::{coerce_args, CoerceMode};

const NAME: &str = "json";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(JsonUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonUdf {
    signature: Signature,
}

impl JsonUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for JsonUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for JsonUdf {
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
            let out = match sv {
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)) => normalise(s),
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(out)));
        }

        let arr = args.args[0].clone().into_array(n)?;
        let strings = StringArrayView::from_array(&arr)?;
        let mut b = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            match strings.cell(i).and_then(normalise) {
                Some(s) => b.append_value(&s),
                None => b.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish()) as ArrayRef))
    }
}

/// Round-trip parse → serialise. Invalid JSON returns `None`. Matches the
/// legacy contract: the return string is the canonical re-serialisation,
/// not the raw input.
fn normalise(s: &str) -> Option<String> {
    let value = parse(s)?;
    serde_json::to_string(&value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_json_round_trips() {
        // Test asserts the round-tripped serialisation; whitespace is stripped.
        assert_eq!(
            normalise(r#"[1,2,3,{"f1":1,"f2":[5,6]},4]"#).as_deref(),
            Some(r#"[1,2,3,{"f1":1,"f2":[5,6]},4]"#)
        );
    }

    #[test]
    fn invalid_json_returns_none() {
        assert_eq!(normalise(r#"{"invalid": "json""#), None);
        assert_eq!(normalise("not-json"), None);
    }

    #[test]
    fn return_type_is_utf8() {
        assert_eq!(
            JsonUdf::new().return_type(&[DataType::Utf8]).unwrap(),
            DataType::Utf8
        );
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = JsonUdf::new();
        assert!(udf.coerce_types(&[]).is_err());
        assert!(udf.coerce_types(&[DataType::Utf8, DataType::Utf8]).is_err());
    }
}
