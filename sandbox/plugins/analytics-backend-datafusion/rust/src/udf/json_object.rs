/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `json_object(k1, tag1, v1, k2, tag2, v2, ...)` — build a JSON object from
//! interleaved (key, type-tag, value) triples. Keys, tags, and values arrive
//! as Utf8 (the Java `JsonObjectAdapter` casts every operand to VARCHAR and
//! prepends a per-value type-tag string). Tags drive how each value is
//! emitted in the JSON output:
//!
//! * `n` — numeric: emit unquoted (`5`).
//! * `b` — boolean: emit unquoted (`true` / `false`).
//! * `j` — already-JSON (nested call result): emit as JSON-string with
//!   the original text escaped (parity with legacy
//!   `JsonObjectFunctionImpl` which calls `gson.toJson(value)` on a Java
//!   `String` instance, producing a quoted-and-escaped fragment).
//! * `s` (default) — plain string: emit as a JSON-quoted string.
//!
//! Any-NULL input → NULL output. Triple-arity mismatch (operand count not
//! a multiple of 3) → NULL.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::{Map, Value};

use super::json_common::{scalar_utf8, StringArrayView};
use super::{coerce_slot, CoerceMode};

const NAME: &str = "json_object";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(JsonObjectUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonObjectUdf {
    signature: Signature,
}

impl JsonObjectUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for JsonObjectUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for JsonObjectUdf {
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
        let n = args.number_rows;

        if args
            .args
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)))
        {
            let cells: Vec<Option<&str>> = args.args.iter().map(scalar_utf8).collect();
            let out = build_object(&cells);
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(out)));
        }

        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|v| v.clone().into_array(n))
            .collect::<Result<_>>()?;
        let columns: Vec<StringArrayView<'_>> = arrays
            .iter()
            .map(StringArrayView::from_array)
            .collect::<Result<_>>()?;

        let mut b = StringBuilder::with_capacity(n, n * 32);
        let mut row_cells: Vec<Option<&str>> = Vec::with_capacity(columns.len());
        for i in 0..n {
            row_cells.clear();
            for col in &columns {
                row_cells.push(col.cell(i));
            }
            match build_object(&row_cells) {
                Some(s) => b.append_value(&s),
                None => b.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish()) as ArrayRef))
    }
}

/// Build a JSON object from interleaved (key, tag, value) triples. Returns
/// `None` if any cell is NULL or the operand count isn't a multiple of 3.
fn build_object(cells: &[Option<&str>]) -> Option<String> {
    if cells.is_empty() || !cells.len().is_multiple_of(3) {
        return None;
    }
    if cells.iter().any(|c| c.is_none()) {
        return None;
    }
    let mut map = Map::with_capacity(cells.len() / 3);
    for triple in cells.chunks(3) {
        let key = triple[0].unwrap().to_string();
        let tag = triple[1].unwrap();
        let value = triple[2].unwrap();
        map.insert(key, encode_value(tag, value));
    }
    serde_json::to_string(&Value::Object(map)).ok()
}

/// Encode a single value per its type-tag. Unrecognised tags fall through to
/// the string-quoting branch — matches legacy `gson.toJson(Object)` which
/// only special-cases numeric / boolean / null Java types.
pub(crate) fn encode_value(tag: &str, value: &str) -> Value {
    match tag {
        "n" => serde_json::from_str::<Value>(value).unwrap_or(Value::String(value.to_string())),
        "b" => match value {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            _ => Value::String(value.to_string()),
        },
        "j" => Value::String(value.to_string()),
        _ => Value::String(value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_keys_and_numeric_values() {
        // testJsonObject case a — single (string, fp) pair.
        assert_eq!(
            build_object(&[Some("key"), Some("n"), Some("123.45")]).as_deref(),
            Some(r#"{"key":123.45}"#)
        );
    }

    #[test]
    fn nested_json_value_is_quoted_as_string() {
        // testJsonObject case b — outer json_object's value is a nested
        // json_object result (already a JSON string, tagged 'j').
        assert_eq!(
            build_object(&[Some("outer"), Some("j"), Some(r#"{"inner":123.45}"#)]).as_deref(),
            Some(r#"{"outer":"{\"inner\":123.45}"}"#)
        );
    }

    #[test]
    fn string_value_round_trips_with_escaping() {
        assert_eq!(
            build_object(&[Some("name"), Some("s"), Some("Tomy")]).as_deref(),
            Some(r#"{"name":"Tomy"}"#)
        );
    }

    #[test]
    fn null_anywhere_returns_none() {
        assert!(build_object(&[None, Some("s"), Some("v")]).is_none());
        assert!(build_object(&[Some("k"), None, Some("v")]).is_none());
        assert!(build_object(&[Some("k"), Some("s"), None]).is_none());
    }

    #[test]
    fn arity_must_be_triple() {
        assert!(build_object(&[]).is_none());
        assert!(build_object(&[Some("k"), Some("s")]).is_none());
        assert!(build_object(&[Some("k1"), Some("s"), Some("v1"), Some("k2")]).is_none());
    }

    #[test]
    fn return_type_is_utf8() {
        assert_eq!(
            JsonObjectUdf::new().return_type(&[DataType::Utf8]).unwrap(),
            DataType::Utf8
        );
    }
}
