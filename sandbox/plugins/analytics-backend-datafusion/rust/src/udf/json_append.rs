/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `json_append(value, path1, val1, [path2, val2, ...])` — push `valN` onto
//! the array at each PPL-path match. Mirrors the legacy SQL plugin's
//! `JsonAppendFunctionImpl`, which delegates to Calcite's
//! `JsonFunctions.jsonInsert` with a `.meaningless_key` suffix trick so Jayway
//! routes the insert to the array-parent branch (`Collection.add`). Net
//! observable effect: one new trailing element per matching array.
//!
//! # Observable semantics (verified against
//! `CalcitePPLJsonBuiltinFunctionIT.testJsonAppend`):
//!
//! | input                                               | (path, value)                                | output                                                              |
//! |-----------------------------------------------------|----------------------------------------------|---------------------------------------------------------------------|
//! | `{"teacher":["Alice"],"student":[...]}`             | `student`, `{"name":"Tomy","rank":5}` (str)  | adds the string `"{\"name\":\"Tomy\",\"rank\":5}"` to `student`      |
//! | `{"teacher":["Alice"],"student":[...]}`             | `teacher,Tom,teacher,Walt` (multi-pair)      | adds `"Tom"` then `"Walt"` to `teacher`                              |
//! | `{"school":{"teacher":["Alice"], ...}}`             | `school.teacher`, `["Tom","Walt"]` (str)     | adds the string `"[\"Tom\",\"Walt\"]"` to `school.teacher`           |
//! | NULL doc / any NULL arg / odd trailing arg          | —                                            | NULL                                                                 |
//! | malformed doc / malformed path                      | —                                            | NULL                                                                 |
//! | path resolves to non-array                          | —                                            | silent no-op (input unchanged) — parity with Jayway `.add` skip      |
//!
//! Values are always pushed as `Value::String` because every UDF arg is
//! coerced to Utf8 — nested `json_object`/`json_array` results arrive here
//! already stringified and are appended as strings, matching legacy.
//!
//! # Division of labor with the Java adapter
//!
//! `JsonFunctionAdapters.JsonAppendAdapter` renames the Calcite operator to
//! `json_append` so isthmus resolves to this UDF via the YAML signature.

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

const NAME: &str = "json_append";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(JsonAppendUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonAppendUdf {
    signature: Signature,
}

impl JsonAppendUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for JsonAppendUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for JsonAppendUdf {
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
        // Need doc + at least one (path, value) pair. Odd trailing arg mirrors
        // the legacy `RuntimeException("needs corresponding path and values")`
        // thrown by `JsonAppendFunctionImpl.eval`; we surface it as NULL to
        // keep parity with the "malformed input → NULL" convention.
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
            let out = append(doc, &rest);
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
            match append(doc, &rest) {
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

/// Apply each (path, value) pair to a fresh parse of `doc`. Push-only:
/// non-array targets (scalar, object) are silent no-ops, matching legacy
/// `jsonInsert`'s Collection-parent branch skip.
fn append(doc: Option<&str>, rest: &[Option<&str>]) -> Option<String> {
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
            // Root-path is a no-op (legacy `ctx.set("$", v)` is silently
            // discarded by Jayway for the same reason).
            continue;
        }
        append_one(&mut value, &segments, new_val);
    }
    serde_json::to_string(&value).ok()
}

fn append_one(root: &mut Value, segments: &[Segment<'_>], new_val: &str) {
    let item = Value::String(new_val.to_string());
    walk_mut(root, segments, |parent, final_seg| {
        match (parent, final_seg) {
            // Push onto the matched array when the final segment names an
            // existing array-valued field. Non-array / missing → no-op.
            (Value::Object(map), Segment::Field(name)) => {
                if let Some(Value::Array(arr)) = map.get_mut(*name) {
                    arr.push(item.clone());
                }
            }
            // Direct array-index / wildcard targets: push onto the *addressed*
            // array element when that element is itself an array.
            (Value::Array(arr), Segment::Index(i)) if *i < arr.len() => {
                if let Value::Array(inner) = &mut arr[*i] {
                    inner.push(item.clone());
                }
            }
            (Value::Array(arr), Segment::Wildcard) => {
                for slot in arr.iter_mut() {
                    if let Value::Array(inner) = slot {
                        inner.push(item.clone());
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
    fn single_value_appended_to_named_array() {
        // testJsonAppend case b, single pair.
        assert_eq!(
            append(
                Some(r#"{"teacher":["Alice"]}"#),
                &[Some("teacher"), Some("Tom")],
            )
            .as_deref(),
            Some(r#"{"teacher":["Alice","Tom"]}"#)
        );
    }

    #[test]
    fn multiple_pairs_append_sequentially() {
        // testJsonAppend case b (multi-pair).
        assert_eq!(
            append(
                Some(r#"{"teacher":["Alice"]}"#),
                &[Some("teacher"), Some("Tom"), Some("teacher"), Some("Walt")],
            )
            .as_deref(),
            Some(r#"{"teacher":["Alice","Tom","Walt"]}"#)
        );
    }

    #[test]
    fn nested_path_appends_to_inner_array() {
        // testJsonAppend case c — a pre-stringified JSON array is appended as
        // a single string element (legacy calls gson/jackson on the outer doc
        // but NOT on the value; our Utf8-coerced arg arrives already
        // stringified and is pushed as-is).
        assert_eq!(
            append(
                Some(r#"{"school":{"teacher":["Alice"]}}"#),
                &[Some("school.teacher"), Some(r#"["Tom","Walt"]"#)],
            )
            .as_deref(),
            Some(r#"{"school":{"teacher":["Alice","[\"Tom\",\"Walt\"]"]}}"#)
        );
    }

    #[test]
    fn stringified_json_object_value_is_appended_as_single_string() {
        // testJsonAppend case a — `json_object(...)` lowers to a string, so
        // the element lands as a stringified object (legacy and Rust agree).
        assert_eq!(
            append(
                Some(r#"{"student":[{"name":"Bob","rank":1}]}"#),
                &[Some("student"), Some(r#"{"name":"Tomy","rank":5}"#)],
            )
            .as_deref(),
            Some(r#"{"student":[{"name":"Bob","rank":1},"{\"name\":\"Tomy\",\"rank\":5}"]}"#)
        );
    }

    #[test]
    fn non_array_target_is_silent_noop() {
        // teacher is a scalar here, not an array — legacy `Collection.add`
        // branch skips; no-op is the observable parity.
        assert_eq!(
            append(
                Some(r#"{"teacher":"Alice"}"#),
                &[Some("teacher"), Some("Tom")],
            )
            .as_deref(),
            Some(r#"{"teacher":"Alice"}"#)
        );
    }

    #[test]
    fn missing_path_is_silent_noop() {
        assert_eq!(
            append(
                Some(r#"{"teacher":["Alice"]}"#),
                &[Some("students"), Some("Tom")],
            )
            .as_deref(),
            Some(r#"{"teacher":["Alice"]}"#)
        );
    }

    #[test]
    fn wildcard_path_appends_to_every_array_child() {
        // Nested wildcard: every element of groups is an array; each receives
        // the same appended scalar.
        assert_eq!(
            append(
                Some(r#"{"groups":[["a"],["b","c"]]}"#),
                &[Some("groups{}"), Some("x")],
            )
            .as_deref(),
            Some(r#"{"groups":[["a","x"],["b","c","x"]]}"#)
        );
    }

    #[test]
    fn any_null_arg_returns_none() {
        assert!(append(None, &[Some("a"), Some("v")]).is_none());
        assert!(append(Some(r#"{"a":[1]}"#), &[None, Some("v")]).is_none());
        assert!(append(Some(r#"{"a":[1]}"#), &[Some("a"), None]).is_none());
    }

    #[test]
    fn malformed_doc_returns_none() {
        assert!(append(Some("not-json"), &[Some("a"), Some("v")]).is_none());
    }

    #[test]
    fn malformed_path_returns_none() {
        assert!(append(Some(r#"{"a":[1]}"#), &[Some("a{"), Some("v")]).is_none());
    }

    #[test]
    fn coerce_types_enforces_string_on_every_slot() {
        let udf = JsonAppendUdf::new();
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
            JsonAppendUdf::new().return_type(&[DataType::Utf8]).unwrap(),
            DataType::Utf8
        );
    }
}
