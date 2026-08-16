/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `json_valid(value)` — TRUE iff the input parses as JSON.
//!
//! Parity target: the legacy SQL-plugin `JsonUtils.isValidJson` (Jackson
//! `ObjectMapper.readTree`), which is the runtime behind the PPL `json_valid`
//! the `JsonFunctionsIT` suite asserts against. Contract:
//!   * valid JSON → TRUE
//!   * malformed input → FALSE
//!   * NULL / missing input → FALSE (legacy returns `LITERAL_FALSE`, NOT null),
//!     so `where not json_valid(col)` includes NULL rows
//!   * empty / whitespace-only input → TRUE (Jackson `readTree("")` returns a
//!     `MissingNode` without throwing; serde rejects it, so `is_valid_json`
//!     special-cases it — see that fn)

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value;

use super::json_common::StringArrayView;
use super::{coerce_args, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(JsonValidUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonValidUdf {
    signature: Signature,
}

impl JsonValidUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for JsonValidUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for JsonValidUdf {
    fn name(&self) -> &str {
        "json_valid"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return plan_err!("json_valid expects 1 argument, got {}", arg_types.len());
        }
        Ok(DataType::Boolean)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args("json_valid", arg_types, &[CoerceMode::Utf8])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return plan_err!("json_valid expects 1 argument, got {}", args.args.len());
        }
        let n = args.number_rows;

        if let ColumnarValue::Scalar(sv) = &args.args[0] {
            // NULL input → FALSE (not NULL), matching the legacy SQL-plugin
            // JsonUtils.isValidJson: `if (isNull || isMissing) return LITERAL_FALSE`.
            let valid = match sv {
                ScalarValue::Utf8(opt)
                | ScalarValue::LargeUtf8(opt)
                | ScalarValue::Utf8View(opt) => opt.as_deref().map(is_valid_json).unwrap_or(false),
                _ => false,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(valid))));
        }

        let arr = args.args[0].clone().into_array(n)?;
        // CoerceMode::Utf8 preserves the observed string type, so the input may be
        // Utf8 / LargeUtf8 / Utf8View — StringArrayView reads all three uniformly.
        let strings = StringArrayView::from_array(&arr)?;

        let mut builder = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            // NULL row → FALSE (legacy JsonUtils.isValidJson returns FALSE for null/missing),
            // so `where not json_valid(col)` includes NULL rows as the IT expects.
            builder.append_value(strings.cell(i).map(is_valid_json).unwrap_or(false));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// TRUE iff `s` is a well-formed JSON document (object, array, or scalar).
/// Matches the legacy PPL contract in SQL-plugin `JsonUtils.isValidJson`, which uses Jackson
/// `ObjectMapper.readTree`. Jackson returns a `MissingNode` (no exception) for empty or
/// whitespace-only input, so the legacy function — and the `JsonFunctionsIT.test_json_valid`
/// fixture — treat `""` as VALID. serde_json's `from_str` instead errors on empty input, so we
/// special-case empty/whitespace to preserve parity; all other inputs use serde's RFC 8259 parse.
/// Callers handle the NULL-input case before dispatching here; this helper is total over `&str`.
fn is_valid_json(s: &str) -> bool {
    // Jackson readTree("") / readTree("   ") → MissingNode (valid); serde would reject. Match legacy.
    if s.trim().is_empty() {
        return true;
    }
    serde_json::from_str::<Value>(s).is_ok()
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, BooleanArray, StringArray};
    use datafusion::arrow::datatypes::Field;

    #[test]
    fn parses_every_json_shape_as_valid() {
        // Legacy CalcitePPLJsonBuiltinFunctionIT.testJsonValid pins array input;
        // objects / nested / scalars / whitespace-padded are covered here so future
        // callers do not discover Jackson parity edges empirically.
        assert!(is_valid_json("[1,2,3,4]"));
        assert!(is_valid_json("{\"k\":1}"));
        assert!(is_valid_json("{\"a\":[1,{\"b\":null}]}"));
        assert!(is_valid_json("42"));
        assert!(is_valid_json("\"s\""));
        assert!(is_valid_json("null"));
        assert!(is_valid_json("true"));
        assert!(is_valid_json("  [1, 2]  "));
    }

    #[test]
    fn rejects_malformed() {
        // Pinned against legacy IT's {"invalid": "json"} case + garbage.
        assert!(!is_valid_json("{\"invalid\": \"json\""));
        assert!(!is_valid_json("not-json"));
        assert!(!is_valid_json("[1,2"));
    }

    #[test]
    fn empty_and_whitespace_are_valid() {
        // Legacy JsonUtils.isValidJson uses Jackson readTree, which returns MissingNode (no throw)
        // for empty / whitespace-only input — so the PPL contract (and JsonFunctionsIT's
        // "json empty string" fixture row) treats these as VALID. serde would reject them, hence the
        // special-case in is_valid_json.
        assert!(is_valid_json(""));
        assert!(is_valid_json("   "));
    }

    #[test]
    fn return_type_is_boolean() {
        let udf = JsonValidUdf::new();
        assert_eq!(
            udf.return_type(&[DataType::Utf8]).unwrap(),
            DataType::Boolean
        );
    }

    #[test]
    fn coerce_types_accepts_string_variants() {
        let udf = JsonValidUdf::new();
        // CoerceMode::Utf8 preserves the observed string type (Utf8/LargeUtf8/Utf8View);
        // the StringArrayView reader handles all three at execution time.
        for t in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            assert_eq!(
                udf.coerce_types(std::slice::from_ref(&t)).unwrap(),
                vec![t.clone()]
            );
        }
    }

    #[test]
    fn coerce_types_rejects_non_string() {
        let udf = JsonValidUdf::new();
        let err = udf.coerce_types(&[DataType::Int64]).unwrap_err();
        assert!(err.to_string().contains("expected string"));
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = JsonValidUdf::new();
        assert!(udf.coerce_types(&[]).is_err());
        assert!(udf.coerce_types(&[DataType::Utf8, DataType::Utf8]).is_err());
    }

    #[test]
    fn invoke_column_null_input_is_false() {
        // NULL/missing input → FALSE (not NULL), matching legacy JsonUtils.isValidJson
        // (`if (isNull || isMissing) return LITERAL_FALSE`). This is what makes
        // `where not json_valid(col)` include NULL rows, per JsonFunctionsIT.test_not_json_valid.
        let udf = JsonValidUdf::new();
        let input = StringArray::from(vec![
            Some("[1,2,3,4]"),
            None,
            Some("{\"invalid\": \"json\""),
            Some("42"),
            Some(""),
        ]);
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            number_rows: 5,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!arr.is_null(1), "NULL input must produce FALSE, not NULL");
        assert!(arr.value(0));
        assert!(
            !arr.value(1),
            "NULL input → FALSE (legacy JsonUtils.isValidJson)"
        );
        assert!(!arr.value(2));
        assert!(arr.value(3));
        // Empty string is VALID per the legacy Jackson contract (see empty_and_whitespace_are_valid).
        assert!(arr.value(4));
    }

    #[test]
    fn invoke_scalar_input_produces_scalar_output() {
        let udf = JsonValidUdf::new();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "[1,2,3,4]".into(),
            )))],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        match udf.invoke_with_args(args).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {}
            other => panic!("expected Boolean(Some(true)), got {other:?}"),
        }
    }

    #[test]
    fn invoke_scalar_null_is_false() {
        // NULL scalar → Boolean(Some(false)), matching legacy JsonUtils.isValidJson.
        let udf = JsonValidUdf::new();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        match udf.invoke_with_args(args).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))) => {}
            other => panic!("expected Boolean(Some(false)), got {other:?}"),
        }
    }
}
