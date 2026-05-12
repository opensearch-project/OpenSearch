/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `json_extend(value, path1, val1, [path2, val2, ...])` — spread-or-append:
//! if `valN` parses as a JSON array its elements are pushed individually onto
//! the path-matched target array; otherwise the whole value is pushed as a
//! single string element (parity with legacy `gson.fromJson(..., List.class)`
//! try/fall-back in `JsonExtendFunctionImpl`). Non-array / missing targets are
//! no-ops; any-NULL-arg / malformed-doc / malformed-path → NULL.
//!
//! Intentional divergence from legacy: spread preserves source numeric type
//! (`[1,2,3]` → `1,2,3`). Gson widens every number to `Double` (`1.0, 2.0,
//! 3.0`); no legacy IT covers this edge case, so every existing fixture still
//! passes. Tracked for follow-up cross-engine alignment.

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

use super::json_common::{as_utf8_array, parse, parse_ppl_segments, walk_mut, Segment};
use super::{coerce_slot, CoerceMode};

const NAME: &str = "json_extend";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(JsonExtendUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonExtendUdf {
    signature: Signature,
}

impl JsonExtendUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for JsonExtendUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for JsonExtendUdf {
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
        args.iter()
            .enumerate()
            .map(|(i, ty)| coerce_slot(NAME, i, ty, CoerceMode::Utf8))
            .collect()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 3 || args.args.len().is_multiple_of(2) {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }
        let n = args.number_rows;

        if args
            .args
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)))
        {
            let doc = scalar_utf8(&args.args[0]);
            let rest: Vec<Option<&str>> = args.args[1..].iter().map(scalar_utf8).collect();
            let out = extend(doc, &rest);
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(out)));
        }

        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|v| v.clone().into_array(n))
            .collect::<Result<_>>()?;
        let columns: Vec<&datafusion::arrow::array::StringArray> =
            arrays.iter().map(as_utf8_array).collect::<Result<_>>()?;

        let mut b = StringBuilder::with_capacity(n, n * 16);
        let mut rest: Vec<Option<&str>> = Vec::with_capacity(columns.len() - 1);
        for i in 0..n {
            let doc = cell(columns[0], i);
            rest.clear();
            for col in &columns[1..] {
                rest.push(cell(col, i));
            }
            match extend(doc, &rest) {
                Some(s) => b.append_value(&s),
                None => b.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish()) as ArrayRef))
    }
}

fn scalar_utf8(v: &ColumnarValue) -> Option<&str> {
    match v {
        ColumnarValue::Scalar(
            ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s),
        ) => s.as_deref(),
        _ => None,
    }
}

fn cell(arr: &datafusion::arrow::array::StringArray, i: usize) -> Option<&str> {
    if arr.is_null(i) {
        None
    } else {
        Some(arr.value(i))
    }
}

/// Classify the raw value string into the push-list the terminal closure
/// should apply. A successful JSON-array parse expands to the array
/// elements; anything else (scalar, object, malformed JSON, plain string)
/// expands to `[Value::String(value)]` — the legacy `gson.fromJson`
/// try/fall-back pattern.
fn spread(raw: &str) -> Vec<Value> {
    if let Ok(Value::Array(items)) = serde_json::from_str::<Value>(raw) {
        return items;
    }
    vec![Value::String(raw.to_string())]
}

fn extend(doc: Option<&str>, rest: &[Option<&str>]) -> Option<String> {
    let doc_str = doc?;
    if rest.iter().any(|p| p.is_none()) {
        return None;
    }
    let mut value = parse(doc_str)?;
    for chunk in rest.chunks(2) {
        let path = chunk[0].unwrap();
        let new_val = chunk[1].unwrap();
        let segments = parse_ppl_segments(path).ok()?;
        if segments.is_empty() {
            continue;
        }
        let items = spread(new_val);
        extend_one(&mut value, &segments, &items);
    }
    serde_json::to_string(&value).ok()
}

fn extend_one(root: &mut Value, segments: &[Segment<'_>], items: &[Value]) {
    walk_mut(root, segments, |parent, final_seg| {
        match (parent, final_seg) {
            (Value::Object(map), Segment::Field(name)) => {
                if let Some(Value::Array(arr)) = map.get_mut(*name) {
                    arr.extend(items.iter().cloned());
                }
            }
            (Value::Array(arr), Segment::Index(i)) if *i < arr.len() => {
                if let Value::Array(inner) = &mut arr[*i] {
                    inner.extend(items.iter().cloned());
                }
            }
            (Value::Array(arr), Segment::Wildcard) => {
                for slot in arr.iter_mut() {
                    if let Value::Array(inner) = slot {
                        inner.extend(items.iter().cloned());
                    }
                }
            }
            _ => {}
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_array_value_is_spread_into_target_array() {
        // testJsonExtend case c — the stringified json_array(...) value is a
        // JSON array, so its elements are spread (contrast json_append's case
        // c, which pushes the whole string as one element).
        assert_eq!(
            extend(
                Some(r#"{"school":{"teacher":["Alice"]}}"#),
                &[Some("school.teacher"), Some(r#"["Tom","Walt"]"#)],
            )
            .as_deref(),
            Some(r#"{"school":{"teacher":["Alice","Tom","Walt"]}}"#)
        );
    }

    #[test]
    fn non_array_value_falls_back_to_single_push() {
        // testJsonExtend case a — json_object(...) stringifies to a JSON
        // object, not an array. Parse-as-List fails → single element pushed.
        assert_eq!(
            extend(
                Some(r#"{"student":[{"name":"Bob","rank":1}]}"#),
                &[Some("student"), Some(r#"{"name":"Tommy","rank":5}"#)],
            )
            .as_deref(),
            Some(r#"{"student":[{"name":"Bob","rank":1},"{\"name\":\"Tommy\",\"rank\":5}"]}"#)
        );
    }

    #[test]
    fn plain_string_value_falls_back_to_single_push() {
        // testJsonExtend case b — plain "Tom" / "Walt" strings are not JSON
        // arrays, so each is pushed as a single element.
        assert_eq!(
            extend(
                Some(r#"{"teacher":["Alice"]}"#),
                &[Some("teacher"), Some("Tom"), Some("teacher"), Some("Walt")],
            )
            .as_deref(),
            Some(r#"{"teacher":["Alice","Tom","Walt"]}"#)
        );
    }

    #[test]
    fn non_array_target_is_silent_noop() {
        assert_eq!(
            extend(
                Some(r#"{"teacher":"Alice"}"#),
                &[Some("teacher"), Some(r#"["Tom"]"#)],
            )
            .as_deref(),
            Some(r#"{"teacher":"Alice"}"#)
        );
    }

    #[test]
    fn missing_path_is_silent_noop() {
        assert_eq!(
            extend(
                Some(r#"{"teacher":["Alice"]}"#),
                &[Some("students"), Some("Tom")],
            )
            .as_deref(),
            Some(r#"{"teacher":["Alice"]}"#)
        );
    }

    #[test]
    fn empty_json_array_value_is_a_noop_on_target() {
        // Parses as an array of zero items → nothing to push.
        assert_eq!(
            extend(
                Some(r#"{"teacher":["Alice"]}"#),
                &[Some("teacher"), Some("[]")],
            )
            .as_deref(),
            Some(r#"{"teacher":["Alice"]}"#)
        );
    }

    #[test]
    fn mixed_type_json_array_elements_preserve_their_types() {
        // Integers/booleans come through as JSON numbers/booleans, not as
        // stringified elements — diverges from legacy Gson (which widens to
        // Double) but no legacy IT asserts Gson's widening. See module-level
        // docs for the rationale + tracking-issue pointer.
        assert_eq!(
            extend(
                Some(r#"{"xs":[0]}"#),
                &[Some("xs"), Some("[1,2,true,\"s\"]")],
            )
            .as_deref(),
            Some(r#"{"xs":[0,1,2,true,"s"]}"#)
        );
    }

    #[test]
    fn wildcard_path_extends_every_array_child() {
        assert_eq!(
            extend(
                Some(r#"{"groups":[["a"],["b","c"]]}"#),
                &[Some("groups{}"), Some(r#"["x","y"]"#)],
            )
            .as_deref(),
            Some(r#"{"groups":[["a","x","y"],["b","c","x","y"]]}"#)
        );
    }

    #[test]
    fn any_null_arg_returns_none() {
        assert!(extend(None, &[Some("a"), Some("v")]).is_none());
        assert!(extend(Some(r#"{"a":[1]}"#), &[None, Some("v")]).is_none());
        assert!(extend(Some(r#"{"a":[1]}"#), &[Some("a"), None]).is_none());
    }

    #[test]
    fn malformed_doc_returns_none() {
        assert!(extend(Some("not-json"), &[Some("a"), Some("v")]).is_none());
    }

    #[test]
    fn malformed_path_returns_none() {
        assert!(extend(Some(r#"{"a":[1]}"#), &[Some("a{"), Some("v")]).is_none());
    }

    #[test]
    fn coerce_types_enforces_string_on_every_slot() {
        let udf = JsonExtendUdf::new();
        assert_eq!(
            udf.coerce_types(&[DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View])
                .unwrap(),
            vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]
        );
        let err = udf
            .coerce_types(&[DataType::Utf8, DataType::Int32, DataType::Utf8])
            .unwrap_err()
            .to_string();
        assert!(err.contains("expected string"));
    }

    #[test]
    fn return_type_is_utf8() {
        assert_eq!(
            JsonExtendUdf::new().return_type(&[DataType::Utf8]).unwrap(),
            DataType::Utf8
        );
    }
}
