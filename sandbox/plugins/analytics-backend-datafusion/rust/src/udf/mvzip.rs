/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `mvzip(left, right [, separator])` — element-wise zip of two arrays into an
//! array of strings, joined per pair by a separator (default `,`).
//!
//! Mirrors PPL's [`MVZipFunctionImpl`] semantics:
//!
//! * Result length is `min(len(left), len(right))` (Python-`zip` truncation).
//! * Either array NULL → NULL result.
//! * Element NULLs are rendered as empty strings (matches the SQL plugin's
//!   `Objects.toString(elem, "")`), so `mvzip([1, NULL], ["a", "b"])` yields
//!   `["1,a", ",b"]`.
//! * The separator is a Utf8 scalar. Calling it as a column would require
//!   per-row materialization; PPL's surface only exposes a literal so we
//!   constrain to scalars here and produce a planning error otherwise.
//!
//! Result type is `List<Utf8>` regardless of the input element types — `mvzip`
//! is fundamentally a string-formatting operation. The Java side relies on the
//! `opensearch_array_functions.yaml` declaration to type the call before
//! substrait emission.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, GenericListArray,
    Int16Array, Int32Array, Int64Array, Int8Array, ListArray, ListBuilder, StringArray,
    StringBuilder, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(MvzipUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MvzipUdf {
    signature: Signature,
}

impl MvzipUdf {
    pub fn new() -> Self {
        // user_defined lets us accept ListArray with any element type and a
        // string separator without enumerating every concrete combination.
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for MvzipUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for MvzipUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "mvzip"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 && arg_types.len() != 3 {
            return plan_err!("mvzip expects 2 or 3 arguments, got {}", arg_types.len());
        }
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 && arg_types.len() != 3 {
            return plan_err!("mvzip expects 2 or 3 arguments, got {}", arg_types.len());
        }
        for (i, t) in arg_types.iter().take(2).enumerate() {
            if !matches!(t, DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)) {
                return plan_err!("mvzip: arg {i} expected list type, got {t:?}");
            }
        }
        let mut coerced: Vec<DataType> = arg_types[..2].to_vec();
        if arg_types.len() == 3 {
            match &arg_types[2] {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    coerced.push(DataType::Utf8)
                }
                other => return plan_err!("mvzip: arg 2 (separator) expected string, got {other:?}"),
            }
        }
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 && args.args.len() != 3 {
            return plan_err!("mvzip expects 2 or 3 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;

        // Materialize left/right as ListArrays. Scalar list operands are unusual but
        // ColumnarValue::into_array handles them by replicating to {row count}.
        let left_arr = args.args[0].clone().into_array(n)?;
        let right_arr = args.args[1].clone().into_array(n)?;

        let separator: String = if args.args.len() == 3 {
            scalar_string(&args.args[2])
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "mvzip: separator must be a non-NULL string scalar literal".to_string(),
                    )
                })?
                .to_string()
        } else {
            ",".to_string()
        };

        let left = downcast_list(&left_arr, "left")?;
        let right = downcast_list(&right_arr, "right")?;

        // Output: List<Utf8>, one row per input row.
        let mut builder = ListBuilder::new(StringBuilder::new());
        for i in 0..n {
            if left.is_null(i) || right.is_null(i) {
                builder.append_null();
                continue;
            }
            let left_row = left.value(i);
            let right_row = right.value(i);
            let take = left_row.len().min(right_row.len());
            let left_strs = elements_as_strings(left_row.as_ref())?;
            let right_strs = elements_as_strings(right_row.as_ref())?;
            for j in 0..take {
                let l = left_strs[j].as_deref().unwrap_or("");
                let r = right_strs[j].as_deref().unwrap_or("");
                let mut joined =
                    String::with_capacity(l.len() + separator.len() + r.len());
                joined.push_str(l);
                joined.push_str(&separator);
                joined.push_str(r);
                builder.values().append_value(joined);
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Extract a non-NULL Utf8 scalar literal, or return None for everything else
/// (NULL scalar, column-valued, or non-string types).
fn scalar_string(cv: &ColumnarValue) -> Option<&str> {
    if let ColumnarValue::Scalar(sv) = cv {
        match sv {
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)) => Some(s.as_str()),
            _ => None,
        }
    } else {
        None
    }
}

fn downcast_list<'a>(arr: &'a ArrayRef, slot: &str) -> Result<&'a ListArray> {
    arr.as_any().downcast_ref::<GenericListArray<i32>>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "mvzip: {slot} expected ListArray, got {:?}",
            arr.data_type()
        ))
    })
}

/// Convert each element of an Arrow array of any supported scalar type to its
/// canonical string form (matching `Objects.toString(elem, "")` semantics on
/// the SQL plugin side — NULL elements become None which the caller renders as
/// the empty string).
fn elements_as_strings(arr: &dyn Array) -> Result<Vec<Option<String>>> {
    let n = arr.len();
    let mut out: Vec<Option<String>> = Vec::with_capacity(n);
    macro_rules! collect {
        ($A:ty, $fmt:expr) => {{
            let typed = arr.as_any().downcast_ref::<$A>().ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "mvzip: failed to downcast element vector to {}",
                    stringify!($A)
                ))
            })?;
            for i in 0..n {
                if typed.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some($fmt(typed.value(i))));
                }
            }
        }};
    }
    match arr.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            // String children may arrive as any of the three Utf8 flavors; AsArray
            // handles the dispatch for us.
            let s = arr.as_string_opt::<i32>();
            if let Some(typed) = s {
                for i in 0..n {
                    if typed.is_null(i) {
                        out.push(None);
                    } else {
                        out.push(Some(typed.value(i).to_string()));
                    }
                }
            } else {
                let large = arr.as_string_opt::<i64>().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "mvzip: string element downcast failed for {:?}",
                        arr.data_type()
                    ))
                })?;
                for i in 0..n {
                    if large.is_null(i) {
                        out.push(None);
                    } else {
                        out.push(Some(large.value(i).to_string()));
                    }
                }
            }
        }
        DataType::Int8 => collect!(Int8Array, |v: i8| v.to_string()),
        DataType::Int16 => collect!(Int16Array, |v: i16| v.to_string()),
        DataType::Int32 => collect!(Int32Array, |v: i32| v.to_string()),
        DataType::Int64 => collect!(Int64Array, |v: i64| v.to_string()),
        DataType::UInt8 => collect!(UInt8Array, |v: u8| v.to_string()),
        DataType::UInt16 => collect!(UInt16Array, |v: u16| v.to_string()),
        DataType::UInt32 => collect!(UInt32Array, |v: u32| v.to_string()),
        DataType::UInt64 => collect!(UInt64Array, |v: u64| v.to_string()),
        DataType::Float32 => collect!(Float32Array, |v: f32| v.to_string()),
        DataType::Float64 => collect!(Float64Array, |v: f64| v.to_string()),
        DataType::Boolean => collect!(BooleanArray, |v: bool| v.to_string()),
        DataType::Null => {
            // Element vector with Null type — every cell is NULL by definition, so
            // emit None for each. Reachable when the input list is empty and its
            // declared element type is Null/UNKNOWN (e.g. PPL `array()` no-arg
            // before the SQL-plugin VARCHAR-default kicks in).
            for _ in 0..n {
                out.push(None);
            }
        }
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "mvzip: unsupported list element type {other:?}"
            )));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Builder;
    use datafusion::arrow::array::StringBuilder as ArrowStringBuilder;

    fn run(left: ArrayRef, right: ArrayRef, sep: Option<&str>) -> ArrayRef {
        let mut args = vec![ColumnarValue::Array(left.clone()), ColumnarValue::Array(right.clone())];
        if let Some(s) = sep {
            args.push(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.to_string()))));
        }
        let n = left.len();
        let return_field = Arc::new(Field::new(
            "out",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ));
        let arg_fields: Vec<Arc<Field>> = args
            .iter()
            .enumerate()
            .map(|(i, _)| Arc::new(Field::new(format!("a{i}"), DataType::Utf8, true)))
            .collect();
        let result = MvzipUdf::new()
            .invoke_with_args(ScalarFunctionArgs {
                args,
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
        let mut builder = ListBuilder::new(ArrowStringBuilder::new());
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

    fn extract_row_strings(arr: &ArrayRef, row: usize) -> Vec<String> {
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        let inner = list.value(row);
        let strs = inner.as_string::<i32>();
        (0..strs.len()).map(|i| strs.value(i).to_string()).collect()
    }

    #[test]
    fn basic_two_string_arrays_with_default_separator() {
        let left = list_of_strings(&[Some(&[Some("a"), Some("b")])]);
        let right = list_of_strings(&[Some(&[Some("1"), Some("2")])]);
        let result = run(left, right, None);
        assert_eq!(extract_row_strings(&result, 0), vec!["a,1", "b,2"]);
    }

    #[test]
    fn custom_separator() {
        let left = list_of_strings(&[Some(&[Some("x"), Some("y")])]);
        let right = list_of_strings(&[Some(&[Some("1"), Some("2")])]);
        let result = run(left, right, Some("-"));
        assert_eq!(extract_row_strings(&result, 0), vec!["x-1", "y-2"]);
    }

    #[test]
    fn truncate_to_shorter_array() {
        let left = list_of_strings(&[Some(&[Some("a"), Some("b"), Some("c")])]);
        let right = list_of_strings(&[Some(&[Some("1")])]);
        let result = run(left, right, None);
        assert_eq!(extract_row_strings(&result, 0), vec!["a,1"]);
    }

    #[test]
    fn null_element_renders_as_empty_string() {
        let left = list_of_strings(&[Some(&[Some("a"), None])]);
        let right = list_of_strings(&[Some(&[Some("1"), Some("2")])]);
        let result = run(left, right, None);
        assert_eq!(extract_row_strings(&result, 0), vec!["a,1", ",2"]);
    }

    #[test]
    fn null_array_yields_null_row() {
        let left = list_of_strings(&[None]);
        let right = list_of_strings(&[Some(&[Some("1")])]);
        let result = run(left, right, None);
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(list.is_null(0));
    }

    #[test]
    fn empty_array_yields_empty_result() {
        let left = list_of_strings(&[Some(&[])]);
        let right = list_of_strings(&[Some(&[Some("1")])]);
        let result = run(left, right, None);
        assert_eq!(extract_row_strings(&result, 0), Vec::<String>::new());
    }

    #[test]
    fn integer_arrays_are_stringified() {
        let left = list_of_ints(&[Some(&[Some(10), Some(20)])]);
        let right = list_of_ints(&[Some(&[Some(1), Some(2)])]);
        let result = run(left, right, None);
        assert_eq!(extract_row_strings(&result, 0), vec!["10,1", "20,2"]);
    }
}
