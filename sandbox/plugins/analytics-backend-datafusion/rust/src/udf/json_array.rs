/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `json_array(tag1, v1, tag2, v2, ...)` — build a JSON array from interleaved
//! (type-tag, value) pairs. Same tag vocabulary as `json_object` (`n`, `b`,
//! `j`, `s`); the Java `JsonArrayAdapter` casts every value to VARCHAR and
//! prepends one tag per slot. Any-NULL → NULL; odd-arity → NULL.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value;

use super::json_common::{scalar_utf8, StringArrayView};
use super::json_object::encode_value;
use super::{coerce_slot, CoerceMode};

const NAME: &str = "json_array";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(JsonArrayUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonArrayUdf {
    signature: Signature,
}

impl JsonArrayUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for JsonArrayUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for JsonArrayUdf {
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
            let out = build_array(&cells);
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
            match build_array(&row_cells) {
                Some(s) => b.append_value(&s),
                None => b.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish()) as ArrayRef))
    }
}

/// Build a JSON array from interleaved (tag, value) pairs. Returns `None` if
/// any cell is NULL or the operand count is odd.
fn build_array(cells: &[Option<&str>]) -> Option<String> {
    if !cells.len().is_multiple_of(2) {
        return None;
    }
    if cells.iter().any(|c| c.is_none()) {
        return None;
    }
    let mut out = Vec::with_capacity(cells.len() / 2);
    for pair in cells.chunks(2) {
        let tag = pair[0].unwrap();
        let value = pair[1].unwrap();
        out.push(encode_value(tag, value));
    }
    serde_json::to_string(&Value::Array(out)).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_values_emit_unquoted() {
        // testJsonArray — six numerics in source order.
        let cells = [
            Some("n"),
            Some("1"),
            Some("n"),
            Some("2"),
            Some("n"),
            Some("0"),
            Some("n"),
            Some("-1"),
            Some("n"),
            Some("1.1"),
            Some("n"),
            Some("-0.11"),
        ];
        assert_eq!(build_array(&cells).as_deref(), Some("[1,2,0,-1,1.1,-0.11]"));
    }

    #[test]
    fn mixed_types_round_trip_per_tag() {
        // testJsonArrayWithDifferentType — int + string + nested-json.
        let cells = [
            Some("n"),
            Some("1"),
            Some("s"),
            Some("123"),
            Some("j"),
            Some(r#"{"name":3}"#),
        ];
        assert_eq!(
            build_array(&cells).as_deref(),
            Some(r#"[1,"123","{\"name\":3}"]"#)
        );
    }

    #[test]
    fn empty_array_is_emitted() {
        assert_eq!(build_array(&[]).as_deref(), Some("[]"));
    }

    #[test]
    fn null_anywhere_returns_none() {
        assert!(build_array(&[None, Some("v")]).is_none());
        assert!(build_array(&[Some("s"), None]).is_none());
    }

    #[test]
    fn odd_arity_returns_none() {
        assert!(build_array(&[Some("s")]).is_none());
        assert!(build_array(&[Some("s"), Some("v"), Some("n")]).is_none());
    }

    #[test]
    fn return_type_is_utf8() {
        assert_eq!(
            JsonArrayUdf::new().return_type(&[DataType::Utf8]).unwrap(),
            DataType::Utf8
        );
    }
}
