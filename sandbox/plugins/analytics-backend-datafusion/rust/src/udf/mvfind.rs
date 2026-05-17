/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `mvfind(arr, regex)` — find the 0-based index of the first array element
//! matching a regex, or NULL if no match.
//!
//! Mirrors PPL's [`MVFindFunctionImpl`] semantics:
//!
//! * Per-element regex match (Java's `Matcher.find` semantics — substring
//!   match, not full-string anchored). The Rust `regex` crate's
//!   `Regex::is_match` matches the same way (unanchored).
//! * NULL element → skipped (continues to next element).
//! * NULL array or NULL pattern → NULL result.
//! * Empty array → NULL result (no element to match).
//! * Returns Int32 — PPL's surface is `Integer`.
//! * Result type is consistent with the YAML declaration in
//!   `opensearch_array_functions.yaml`.
//!
//! # Pattern compilation strategy
//!
//! When the pattern operand is a non-NULL Utf8 scalar literal we compile the
//! regex once up front (mirrors the SQL plugin's `tryCompileLiteralPattern`
//! plan-time optimization). Column-valued patterns are compiled per row;
//! invalid patterns yield NULL for that row (per PPL spec, dynamic-pattern
//! errors are non-fatal — bad rows just produce NULL).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, GenericListArray, Int16Array,
    Int32Array, Int32Builder, Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};

use super::json_common::StringArrayView;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use regex::Regex;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(MvfindUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MvfindUdf {
    signature: Signature,
}

impl MvfindUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for MvfindUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for MvfindUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "mvfind"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("mvfind expects 2 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Int32)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!("mvfind expects 2 arguments, got {}", arg_types.len());
        }
        if !matches!(
            &arg_types[0],
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
        ) {
            return plan_err!("mvfind: arg 0 expected list type, got {:?}", arg_types[0]);
        }
        let pattern_t = match &arg_types[1] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
            other => return plan_err!("mvfind: arg 1 expected string, got {other:?}"),
        };
        Ok(vec![arg_types[0].clone(), pattern_t])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return plan_err!("mvfind expects 2 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;

        // Fast path: pattern is a Utf8 scalar literal — compile once.
        let scalar_regex: Option<Regex> = if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(p)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(p)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(p))) = &args.args[1]
        {
            // Plan-time invalid pattern → planning error so users see it instantly.
            // Mirrors the SQL plugin's IllegalArgumentException for invalid literal regex.
            match Regex::new(p) {
                Ok(r) => Some(r),
                Err(e) => return plan_err!("mvfind: invalid regex pattern '{p}': {e}"),
            }
        } else {
            None
        };

        let arr_arr = args.args[0].clone().into_array(n)?;
        let list = arr_arr.as_any().downcast_ref::<GenericListArray<i32>>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "mvfind: expected ListArray, got {:?}",
                arr_arr.data_type()
            ))
        })?;

        // Materialize a column-valued pattern up front; for scalar patterns we keep
        // the pre-compiled regex.
        let pattern_arr_ref: Option<ArrayRef> = if scalar_regex.is_none() {
            Some(args.args[1].clone().into_array(n)?)
        } else {
            None
        };
        let pattern_arr: Option<StringArrayView<'_>> =
            pattern_arr_ref.as_ref().map(StringArrayView::from_array).transpose()?;

        let mut builder = Int32Builder::with_capacity(n);
        for i in 0..n {
            if list.is_null(i) {
                builder.append_null();
                continue;
            }
            // Per-row regex (compile if column-valued; reuse the scalar compile otherwise).
            let regex_for_row: Option<Regex> = match (&scalar_regex, pattern_arr.as_ref().and_then(|a| a.cell(i))) {
                (Some(r), _) => Some(r.clone()),
                (None, Some(s)) => Regex::new(s).ok(),
                _ => None,
            };
            let regex = match regex_for_row {
                Some(r) => r,
                None => {
                    builder.append_null();
                    continue;
                }
            };
            let row = list.value(i);
            match find_first_match(row.as_ref(), &regex) {
                Some(idx) => builder.append_value(idx),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Walk an Arrow array of any supported scalar type, return the 0-based index
/// of the first non-null element whose stringified form matches `regex`.
/// Returns None if no element matches.
fn find_first_match(arr: &dyn Array, regex: &Regex) -> Option<i32> {
    let n = arr.len();
    macro_rules! scan {
        ($A:ty, $fmt:expr) => {{
            let typed = arr.as_any().downcast_ref::<$A>()?;
            for i in 0..n {
                if typed.is_null(i) {
                    continue;
                }
                if regex.is_match(&$fmt(typed.value(i))) {
                    return Some(i as i32);
                }
            }
            None
        }};
    }
    match arr.data_type() {
        DataType::Utf8 => {
            let typed = arr.as_any().downcast_ref::<datafusion::arrow::array::StringArray>()?;
            for i in 0..n {
                if !typed.is_null(i) && regex.is_match(typed.value(i)) {
                    return Some(i as i32);
                }
            }
            None
        }
        DataType::LargeUtf8 => {
            let typed = arr.as_any().downcast_ref::<datafusion::arrow::array::LargeStringArray>()?;
            for i in 0..n {
                if !typed.is_null(i) && regex.is_match(typed.value(i)) {
                    return Some(i as i32);
                }
            }
            None
        }
        DataType::Utf8View => {
            let typed = arr.as_any().downcast_ref::<datafusion::arrow::array::StringViewArray>()?;
            for i in 0..n {
                if !typed.is_null(i) && regex.is_match(typed.value(i)) {
                    return Some(i as i32);
                }
            }
            None
        }
        DataType::Int8 => scan!(Int8Array, |v: i8| v.to_string()),
        DataType::Int16 => scan!(Int16Array, |v: i16| v.to_string()),
        DataType::Int32 => scan!(Int32Array, |v: i32| v.to_string()),
        DataType::Int64 => scan!(Int64Array, |v: i64| v.to_string()),
        DataType::UInt8 => scan!(UInt8Array, |v: u8| v.to_string()),
        DataType::UInt16 => scan!(UInt16Array, |v: u16| v.to_string()),
        DataType::UInt32 => scan!(UInt32Array, |v: u32| v.to_string()),
        DataType::UInt64 => scan!(UInt64Array, |v: u64| v.to_string()),
        DataType::Float32 => scan!(Float32Array, |v: f32| v.to_string()),
        DataType::Float64 => scan!(Float64Array, |v: f64| v.to_string()),
        DataType::Boolean => scan!(BooleanArray, |v: bool| v.to_string()),
        DataType::Null => None,
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Builder, ListBuilder, StringBuilder};
    use datafusion::arrow::datatypes::Field;

    fn run(arr: ArrayRef, pattern: &str) -> ArrayRef {
        let n = arr.len();
        let return_field = Arc::new(Field::new("out", DataType::Int32, true));
        let arg_fields: Vec<Arc<Field>> = vec![
            Arc::new(Field::new("a", arr.data_type().clone(), true)),
            Arc::new(Field::new("p", DataType::Utf8, true)),
        ];
        let result = MvfindUdf::new()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(arr),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern.to_string()))),
                ],
                arg_fields,
                number_rows: n,
                return_field,
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })
            .unwrap();
        match result {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(_) => panic!("expected array"),
        }
    }

    fn list_of_strings(rows: &[Option<&[Option<&str>]>]) -> ArrayRef {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in rows {
            match row {
                None => builder.append_null(),
                Some(elems) => {
                    for e in *elems {
                        match e {
                            Some(s) => builder.values().append_value(s),
                            None => builder.values().append_null(),
                        }
                    }
                    builder.append(true);
                }
            }
        }
        Arc::new(builder.finish())
    }

    fn list_of_ints(rows: &[Option<&[Option<i32>]>]) -> ArrayRef {
        let mut builder = ListBuilder::new(Int32Builder::new());
        for row in rows {
            match row {
                None => builder.append_null(),
                Some(elems) => {
                    for e in *elems {
                        match e {
                            Some(v) => builder.values().append_value(*v),
                            None => builder.values().append_null(),
                        }
                    }
                    builder.append(true);
                }
            }
        }
        Arc::new(builder.finish())
    }

    fn int_value(arr: &ArrayRef, row: usize) -> Option<i32> {
        let typed = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        if typed.is_null(row) {
            None
        } else {
            Some(typed.value(row))
        }
    }

    #[test]
    fn first_match_returns_zero_based_index() {
        let arr = list_of_strings(&[Some(&[Some("apple"), Some("banana"), Some("apricot")])]);
        let result = run(arr, "ban.*");
        assert_eq!(int_value(&result, 0), Some(1));
    }

    #[test]
    fn no_match_returns_null() {
        let arr = list_of_strings(&[Some(&[Some("apple"), Some("banana")])]);
        let result = run(arr, "kiwi");
        assert_eq!(int_value(&result, 0), None);
    }

    #[test]
    fn null_array_returns_null() {
        let arr = list_of_strings(&[None]);
        let result = run(arr, "any");
        assert_eq!(int_value(&result, 0), None);
    }

    #[test]
    fn empty_array_returns_null() {
        let arr = list_of_strings(&[Some(&[])]);
        let result = run(arr, "any");
        assert_eq!(int_value(&result, 0), None);
    }

    #[test]
    fn null_element_skipped_index_still_zero_based() {
        let arr = list_of_strings(&[Some(&[None, Some("banana")])]);
        let result = run(arr, "ban.*");
        assert_eq!(int_value(&result, 0), Some(1));
    }

    #[test]
    fn integer_array_stringified_for_regex() {
        let arr = list_of_ints(&[Some(&[Some(10), Some(20), Some(30)])]);
        let result = run(arr, "^2");
        assert_eq!(int_value(&result, 0), Some(1));
    }

    #[test]
    fn substring_match_semantics_are_unanchored() {
        // Java's Matcher.find: "banana" matches /an/ (unanchored).
        let arr = list_of_strings(&[Some(&[Some("apple"), Some("banana")])]);
        let result = run(arr, "an");
        assert_eq!(int_value(&result, 0), Some(1));
    }
}
