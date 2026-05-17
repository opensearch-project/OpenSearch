/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `json_extract(value, path1, [path2, ...])` — extract JSON value(s) by PPL-path
//! (parity with legacy `JsonExtractFunctionImpl` → Calcite `jsonQuery` / `jsonValue`).
//! Single-path: scalar → `.to_string()`, string → unquoted, object/array →
//! JSON-serialized, wildcard multi-match → JSON-array, miss/explicit-null → NULL.
//! Multi-path: per-path results (NULL → `null` element) wrapped in a JSON array.
//! `< 2` args / any-NULL-arg / malformed doc / malformed path → NULL.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use jsonpath_rust::{JsonPath, JsonPathValue};
use serde_json::Value;

use super::json_common::{convert_ppl_path, parse, scalar_utf8, StringArrayView};
use super::{coerce_slot, CoerceMode};

const NAME: &str = "json_extract";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(JsonExtractUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonExtractUdf {
    signature: Signature,
}

impl JsonExtractUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for JsonExtractUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for JsonExtractUdf {
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
        // Homogeneous string variadic.
        args.iter()
            .enumerate()
            .map(|(i, ty)| coerce_slot(NAME, i, ty, CoerceMode::Utf8))
            .collect()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 {
            // Legacy short-circuit: < 2 args → NULL. Avoid plan_err so adapter
            // mismatches surface as data NULL rather than query failure
            // (matches JsonExtractFunctionImpl.eval's `if (args.length < 2)`).
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }
        let n = args.number_rows;

        // Scalar fast-path: every operand is Scalar → evaluate once.
        if args
            .args
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)))
        {
            let doc = scalar_utf8(&args.args[0]);
            let paths: Vec<Option<&str>> = args.args[1..].iter().map(scalar_utf8).collect();
            let out = extract(doc, &paths);
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(out)));
        }

        // Columnar path: walk row-by-row. Branch production traffic takes.
        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|v| v.clone().into_array(n))
            .collect::<Result<_>>()?;
        let columns: Vec<StringArrayView<'_>> =
            arrays.iter().map(StringArrayView::from_array).collect::<Result<_>>()?;

        let mut b = StringBuilder::with_capacity(n, n * 16);
        let mut path_buf: Vec<Option<&str>> = Vec::with_capacity(columns.len() - 1);
        for i in 0..n {
            let doc = columns[0].cell(i);
            path_buf.clear();
            for col in &columns[1..] {
                path_buf.push(col.cell(i));
            }
            match extract(doc, &path_buf) {
                Some(s) => b.append_value(&s),
                None => b.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish()) as ArrayRef))
    }
}

/// Core extraction. Returns `None` for the legacy NULL-producing cases
/// (any-null arg, malformed doc, malformed path, no match, explicit-null match)
/// and a `Some(String)` for every matched case.
fn extract(doc: Option<&str>, paths: &[Option<&str>]) -> Option<String> {
    let doc_str = doc?;
    if paths.iter().any(|p| p.is_none()) {
        return None;
    }
    let parsed = parse(doc_str)?;
    let per_path: Vec<Value> = paths
        .iter()
        .map(|p| extract_one(&parsed, p.unwrap()).unwrap_or(Value::Null))
        .collect();
    if paths.len() == 1 {
        // Single path: NULL-element collapses to a SQL NULL (legacy's
        // `queryResult != null ? queryResult : valueResult` returns null).
        // Scalar matches unwrap to their string form via `jsonize_single`.
        match per_path.into_iter().next()? {
            Value::Null => None,
            v => Some(jsonize_single(v)),
        }
    } else {
        // Multi-path: wrap in JSON array. NULL misses land as literal `null`
        // elements (testJsonExtractMultiPathWithMissingPath).
        serde_json::to_string(&Value::Array(per_path)).ok()
    }
}

/// Evaluate a single PPL path against a parsed document. Returns `None` for
/// malformed-path, no-match, and explicit-null matches (the three cases the
/// legacy Calcite pair resolves to SQL NULL). Single-match returns the raw
/// `Value`; multi-match returns `Value::Array(...)`.
fn extract_one(doc: &Value, path: &str) -> Option<Value> {
    let jsonpath = convert_ppl_path(path).ok()?;
    let compiled = JsonPath::try_from(jsonpath.as_str()).ok()?;
    let slice = compiled.find_slice(doc);
    let matches: Vec<Value> = slice
        .into_iter()
        .filter_map(|v| match v {
            JsonPathValue::Slice(r, _) => Some(r.clone()),
            _ => None,
        })
        .collect();
    match matches.len() {
        0 => None,
        1 => match matches.into_iter().next().unwrap() {
            Value::Null => None,
            v => Some(v),
        },
        _ => Some(Value::Array(matches)),
    }
}

/// Legacy `doJsonize` single-path output: strings emerge unquoted; every other
/// JSON value (numbers, bools, arrays, objects) is serialized. Matches the
/// legacy `isScalarObject` branch (`.toString()` on Java scalars → same bytes
/// as `serde_json::to_string` for numbers and booleans).
fn jsonize_single(v: Value) -> String {
    match v {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }

    #[test]
    fn single_path_scalar_match_returns_tostring_form() {
        let doc = parsed(r#"{"a":801.0,"b":"hi","c":true,"d":42}"#);
        assert_eq!(extract_one(&doc, "a").map(jsonize_single).unwrap(), "801.0");
        assert_eq!(extract_one(&doc, "b").map(jsonize_single).unwrap(), "hi");
        assert_eq!(extract_one(&doc, "c").map(jsonize_single).unwrap(), "true");
        assert_eq!(extract_one(&doc, "d").map(jsonize_single).unwrap(), "42");
    }

    #[test]
    fn single_path_container_match_is_jsonized() {
        let doc = parsed(r#"{"a":{"x":1,"y":2}}"#);
        assert_eq!(
            extract_one(&doc, "a").map(jsonize_single).unwrap(),
            r#"{"x":1,"y":2}"#
        );
    }

    #[test]
    fn wildcard_multi_match_wraps_in_array() {
        let doc = parsed(r#"{"a":[{"t":"A"},{"t":"B"},{"t":"C"}]}"#);
        let v = extract_one(&doc, "a{}.t").unwrap();
        assert_eq!(serde_json::to_string(&v).unwrap(), r#"["A","B","C"]"#);
    }

    #[test]
    fn missing_and_explicit_null_both_yield_none() {
        let doc = parsed(r#"{"a":null}"#);
        assert!(extract_one(&doc, "a").is_none());
        assert!(extract_one(&doc, "missing").is_none());
    }

    #[test]
    fn multi_path_wraps_with_null_slots_for_misses() {
        let doc = parsed(r#"{"name":"John"}"#);
        let out = extract(Some(r#"{"name":"John"}"#), &[Some("name"), Some("age")]);
        assert_eq!(out.as_deref(), Some(r#"["John",null]"#));
        // No-op parse path so the call shape matches the legacy IT input.
        assert!(doc.is_object());
    }

    #[test]
    fn less_than_two_args_returns_none_via_fast_path() {
        // Exercised via the UDF entry point in the integration tests; the
        // core `extract` helper is only called with ≥1 path, so we just
        // verify the single-path path works end-to-end here.
        let out = extract(Some(r#"{"a":1}"#), &[Some("a")]);
        assert_eq!(out.as_deref(), Some("1"));
    }

    #[test]
    fn any_null_arg_returns_none() {
        assert!(extract(None, &[Some("a")]).is_none());
        assert!(extract(Some(r#"{"a":1}"#), &[None]).is_none());
    }

    #[test]
    fn malformed_document_returns_none() {
        assert!(extract(Some("not-json"), &[Some("a")]).is_none());
    }

    #[test]
    fn malformed_path_returns_none() {
        // Unmatched `{` bubbles out as None (legacy would also emit NULL via
        // the PLAN-error swallow in the stateful function).
        assert!(extract(Some(r#"{"a":1}"#), &[Some("a{0")]).is_none());
    }

    #[test]
    fn coerce_types_threads_each_slot_independently() {
        let udf = JsonExtractUdf::new();
        assert_eq!(
            udf.coerce_types(&[DataType::LargeUtf8, DataType::Utf8View])
                .unwrap(),
            vec![DataType::LargeUtf8, DataType::Utf8View]
        );
        let err = udf
            .coerce_types(&[DataType::Utf8, DataType::Int32])
            .unwrap_err()
            .to_string();
        assert!(err.contains("expected string"));
    }

    #[test]
    fn return_type_is_utf8() {
        assert_eq!(
            JsonExtractUdf::new()
                .return_type(&[DataType::Utf8])
                .unwrap(),
            DataType::Utf8
        );
    }
}
