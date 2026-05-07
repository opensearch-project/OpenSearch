/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `json_set(value, path1, val1, [path2, val2, ...])` — replace the value at
//! each PPL-path match. Mirrors the legacy SQL plugin's `JsonSetFunctionImpl`,
//! which delegates to Calcite's `JsonFunctions.jsonSet` (Jayway `ctx.set`
//! wrapped in an `if (ctx.read(k) != null)` guard — *replace-only*, never
//! inserts a new key).
//!
//! # Observable semantics (verified against
//! `CalcitePPLJsonBuiltinFunctionIT.testJsonSet*`):
//!
//! | input                                      | (path, value)       | output                                  |
//! |--------------------------------------------|---------------------|-----------------------------------------|
//! | `{"a":[{"b":1},{"b":2}]}`                  | `a{}.b`, `"3"`      | `{"a":[{"b":"3"},{"b":"3"}]}`           |
//! | `{"a":[{"b":1},{"b":2}]}`                  | `a{}.b.d`, `"3"`    | input unchanged (path doesn't exist)    |
//! | `{"a":[{"b":1},{"b":{"c":2}}]}`            | `a{}.b.c`, `"3"`    | `{"a":[{"b":1},{"b":{"c":"3"}}]}`       |
//! | NULL doc / any NULL arg / odd trailing arg | —                   | NULL                                    |
//! | malformed doc / malformed path             | —                   | NULL                                    |
//!
//! Values are always stored as JSON strings because every UDF arg is coerced
//! to Utf8 — matching the legacy fixture `"b":"3"` (not `"b":3`).
//!
//! # Division of labor with the Java adapter
//!
//! `JsonFunctionAdapters.JsonSetAdapter` renames the Calcite operator to
//! `json_set` so isthmus resolves to this UDF via the YAML signature.

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

const NAME: &str = "json_set";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(JsonSetUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonSetUdf {
    signature: Signature,
}

impl JsonSetUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for JsonSetUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for JsonSetUdf {
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
        // Need doc + at least one (path, value) pair. Odd trailing arg (unpaired
        // path) short-circuits to NULL — matches the legacy `for (i=1; i<args.length; i+=2)`
        // loop which would IOOBE on an unpaired path; we surface it as NULL to
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
            let out = set(doc, &rest);
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
            match set(doc, &rest) {
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

/// Apply each (path, value) pair to a fresh parse of `doc`. Replace-only:
/// missing paths are no-ops, matching legacy `jsonSet`'s `ctx.read != null`
/// guard.
fn set(doc: Option<&str>, rest: &[Option<&str>]) -> Option<String> {
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
            // Setting the root is a Jayway no-op (root is indelible and
            // unreplaceable via `ctx.set("$", v)`). Mirror that.
            continue;
        }
        set_one(&mut value, &segments, new_val);
    }
    serde_json::to_string(&value).ok()
}

fn set_one(root: &mut Value, segments: &[Segment<'_>], new_val: &str) {
    let replacement = Value::String(new_val.to_string());
    walk_mut(root, segments, |parent, final_seg| {
        match (parent, final_seg) {
            // Replace-only: only overwrite if the key already exists.
            (Value::Object(map), Segment::Field(name)) if map.contains_key(*name) => {
                map.insert((*name).to_string(), replacement.clone());
            }
            (Value::Array(arr), Segment::Index(i)) if *i < arr.len() => {
                arr[*i] = replacement.clone();
            }
            (Value::Array(arr), Segment::Wildcard) => {
                for slot in arr.iter_mut() {
                    *slot = replacement.clone();
                }
            }
            // Type mismatch is a silent no-op (legacy SUPPRESS_EXCEPTIONS).
            _ => {}
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_replace_matches_legacy_fixture() {
        // testJsonSet
        assert_eq!(
            set(
                Some(r#"{"a":[{"b":1},{"b":2}]}"#),
                &[Some("a{}.b"), Some("3")],
            )
            .as_deref(),
            Some(r#"{"a":[{"b":"3"},{"b":"3"}]}"#)
        );
    }

    #[test]
    fn wrong_path_leaves_input_unchanged() {
        // testJsonSetWithWrongPath — 'a{}.b.d' doesn't exist (b is a scalar).
        assert_eq!(
            set(
                Some(r#"{"a":[{"b":1},{"b":2}]}"#),
                &[Some("a{}.b.d"), Some("3")],
            )
            .as_deref(),
            Some(r#"{"a":[{"b":1},{"b":2}]}"#)
        );
    }

    #[test]
    fn partial_wildcard_match_only_sets_where_path_exists() {
        // testJsonSetPartialSet
        assert_eq!(
            set(
                Some(r#"{"a":[{"b":1},{"b":{"c":2}}]}"#),
                &[Some("a{}.b.c"), Some("3")],
            )
            .as_deref(),
            Some(r#"{"a":[{"b":1},{"b":{"c":"3"}}]}"#)
        );
    }

    #[test]
    fn multiple_path_value_pairs_apply_sequentially() {
        assert_eq!(
            set(
                Some(r#"{"a":1,"b":2}"#),
                &[Some("a"), Some("10"), Some("b"), Some("20")],
            )
            .as_deref(),
            Some(r#"{"a":"10","b":"20"}"#)
        );
    }

    #[test]
    fn any_null_arg_returns_none() {
        assert!(set(None, &[Some("a"), Some("v")]).is_none());
        assert!(set(Some(r#"{"a":1}"#), &[None, Some("v")]).is_none());
        assert!(set(Some(r#"{"a":1}"#), &[Some("a"), None]).is_none());
    }

    #[test]
    fn malformed_doc_returns_none() {
        assert!(set(Some("not-json"), &[Some("a"), Some("v")]).is_none());
    }

    #[test]
    fn malformed_path_returns_none() {
        assert!(set(Some(r#"{"a":1}"#), &[Some("a{"), Some("v")]).is_none());
    }

    #[test]
    fn coerce_types_enforces_string_on_every_slot() {
        let udf = JsonSetUdf::new();
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
            JsonSetUdf::new().return_type(&[DataType::Utf8]).unwrap(),
            DataType::Utf8
        );
    }
}
